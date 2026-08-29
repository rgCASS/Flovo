//! 自定义节点适配层。
//!
//! 该模块提供 `CustomLogic` 与 `CustomNode`。开发者只需要实现纯业务逻辑，
//! 适配器负责输入解析、输出写入和默认分支选择。

use crate::error::Result;
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// 自定义节点业务逻辑 trait。
///
/// 开发者只需实现此 trait，`CustomNode` 适配器自动完成 `BaseNode` 转换。
/// `execute` 接收已解析的输入，开发者无需了解 `parameter_bus` 内部细节。
#[async_trait]
pub trait CustomLogic: Send + Sync {
    /// 声明需要的输入参数名称列表。
    ///
    /// 这些名称对应 workflow JSON 配置中 `input_map` 的 key。`CustomNode`
    /// 会按此列表从 `parameter_bus` 解析输入，并传给 `execute`。
    fn input_keys(&self) -> Vec<String>;

    /// 声明输出参数名称。
    ///
    /// 对应 `BaseNode::output_parameters()` 中的唯一输出参数名。
    fn output_key(&self) -> &str;

    /// 执行业务逻辑。
    ///
    /// # 参数
    /// - `inputs`: 从 `parameter_bus` 按 `input_keys()` 解析得到的参数值。
    /// - `ctx`: 节点上下文，可用于访问 workflow/context 等运行期信息。
    ///
    /// # 返回
    /// 返回输出值；`CustomNode` 会自动写入 `parameter_bus` 的 `output_key()`。
    async fn execute(&self, inputs: HashMap<String, Value>, ctx: &NodeContext) -> Result<Value>;
}

/// `CustomLogic` 到 `BaseNode` 的薄适配器。
///
/// `CustomNode` 固定为 step 节点、默认 `default` 分支、默认不可重入。
/// 它只负责工作流协议层面的适配，不承载业务逻辑。
pub struct CustomNode {
    name: String,
    config: NodeConfig,
    logic: Arc<dyn CustomLogic>,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
}

impl CustomNode {
    /// 创建新的自定义节点适配器。
    ///
    /// # 参数
    /// - `config`: workflow JSON 转换后的节点配置。
    /// - `_ctx`: 节点上下文；当前构造阶段无需使用，保留签名与节点工厂一致。
    /// - `logic`: 业务逻辑对象。
    ///
    /// # 行为
    /// - `logic.input_keys()` 中的每个 key 注册为 step 输入参数。
    /// - `logic.output_key()` 注册为唯一 step 输出参数。
    /// - 分支固定为 `default`。
    pub fn new(config: NodeConfig, _ctx: NodeContext, logic: Arc<dyn CustomLogic>) -> Self {
        let input_params = logic
            .input_keys()
            .into_iter()
            .map(|key| (key, ParameterType::Step))
            .collect();

        let mut output_params = HashMap::new();
        output_params.insert(logic.output_key().to_string(), ParameterType::Step);

        Self {
            name: config.node_name.clone(),
            config,
            logic,
            input_params,
            output_params,
            choices: vec!["default".to_string()],
        }
    }
}

#[async_trait]
impl BaseNode for CustomNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn node_type(&self) -> NodeType {
        NodeType::Step
    }

    fn input_parameters(&self) -> &HashMap<String, ParameterType> {
        &self.input_params
    }

    fn output_parameters(&self) -> &HashMap<String, ParameterType> {
        &self.output_params
    }

    fn choices(&self) -> &[String] {
        &self.choices
    }

    fn is_key_node(&self) -> bool {
        self.config.key_node
    }

    fn is_reentrant(&self) -> bool {
        false
    }

    /// 运行自定义节点。
    ///
    /// 执行流程：
    /// 1. 等待节点激活事件。
    /// 2. 按 `input_keys()` 声明解析输入。
    /// 3. 调用业务逻辑。
    /// 4. 写入唯一输出。
    /// 5. 选择固定的 `default` 分支。
    async fn run(&self, ctx: &NodeContext) -> Result<()> {
        NodeHelper::wait_for_event(&self.name, ctx).await?;

        let mut inputs = HashMap::new();
        for key in self.input_params.keys() {
            if let Some(value) = NodeHelper::get_input(key, &self.config.input_map, ctx).await? {
                inputs.insert(key.clone(), value);
            }
        }

        let output = self.logic.execute(inputs, ctx).await?;
        NodeHelper::set_output(&self.name, self.logic.output_key(), output, ctx).await?;
        NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;

        Ok(())
    }
}

/// 创建一个 `CustomNode` 的工厂函数。
pub fn create_custom_factory(
    logic: Arc<dyn CustomLogic>,
) -> impl Fn(NodeConfig, NodeContext) -> Result<Arc<dyn BaseNode>> {
    move |config, ctx| Ok(Arc::new(CustomNode::new(config, ctx, logic.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::WorkflowError;
    use crate::workflow::WorkFlow;

    struct EchoLogic;

    #[async_trait]
    impl CustomLogic for EchoLogic {
        fn input_keys(&self) -> Vec<String> {
            vec!["input".to_string()]
        }

        fn output_key(&self) -> &str {
            "output"
        }

        async fn execute(
            &self,
            inputs: HashMap<String, Value>,
            _ctx: &NodeContext,
        ) -> Result<Value> {
            Ok(inputs.get("input").cloned().unwrap_or(Value::Null))
        }
    }

    fn config(
        node_name: &str,
        input_map: HashMap<String, Option<String>>,
        key_node: bool,
    ) -> NodeConfig {
        let mut choice_map = HashMap::new();
        choice_map.insert("default".to_string(), "finish".to_string());

        NodeConfig {
            id: 1,
            node_name: node_name.to_string(),
            input_map,
            choice_map,
            attrs: HashMap::new(),
            key_node,
        }
    }

    fn workflow_with_node(node: Arc<dyn BaseNode>) -> (Arc<WorkFlow>, NodeContext) {
        let workflow = WorkFlow::new("custom_test".to_string());
        workflow.set_nodes(vec![node]).expect("set nodes");
        let ctx = NodeContext::new(Arc::downgrade(&workflow));
        (workflow, ctx)
    }

    #[test]
    fn test_custom_logic_echo() {
        let mut input_map = HashMap::new();
        input_map.insert("input".to_string(), Some("start.value".to_string()));
        let node = CustomNode::new(
            config("echo", input_map, true),
            NodeContext::new(std::sync::Weak::new()),
            Arc::new(EchoLogic),
        );

        assert_eq!(node.name(), "echo");
        assert_eq!(node.node_type(), NodeType::Step);
        assert_eq!(node.choices(), &["default".to_string()]);
        assert!(!node.is_reentrant());
        assert!(node.is_key_node());
        assert_eq!(
            node.input_parameters().get("input"),
            Some(&ParameterType::Step)
        );
        assert_eq!(
            node.output_parameters().get("output"),
            Some(&ParameterType::Step)
        );
    }

    #[tokio::test]
    async fn test_custom_node_run_echo() {
        let mut input_map = HashMap::new();
        input_map.insert("input".to_string(), Some("start.value".to_string()));

        let node = Arc::new(CustomNode::new(
            config("echo", input_map, false),
            NodeContext::new(std::sync::Weak::new()),
            Arc::new(EchoLogic),
        )) as Arc<dyn BaseNode>;
        let (workflow, ctx) = workflow_with_node(Arc::clone(&node));

        let mut start_outputs = HashMap::new();
        start_outputs.insert("value".to_string(), ParameterType::Step);
        workflow
            .parameter_bus
            .init_output_parameters("start", &start_outputs);
        workflow
            .parameter_bus
            .set_value("start", "value", serde_json::json!("hello"))
            .await
            .expect("set input");

        let run_task = tokio::spawn(async move { node.run(&ctx).await });
        workflow
            .choose_node("start", "echo")
            .await
            .expect("activate node");

        run_task.await.expect("join").expect("run custom node");

        let output = workflow
            .parameter_bus
            .get_value("echo.output")
            .await
            .expect("get output");
        assert_eq!(output, serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn test_custom_node_multi_input() {
        struct SumLogic;

        #[async_trait]
        impl CustomLogic for SumLogic {
            fn input_keys(&self) -> Vec<String> {
                vec!["a".to_string(), "b".to_string()]
            }

            fn output_key(&self) -> &str {
                "sum"
            }

            async fn execute(
                &self,
                inputs: HashMap<String, Value>,
                _ctx: &NodeContext,
            ) -> Result<Value> {
                let a = inputs.get("a").and_then(Value::as_i64).unwrap_or_default();
                let b = inputs.get("b").and_then(Value::as_i64).unwrap_or_default();
                Ok(serde_json::json!(a + b))
            }
        }

        let mut input_map = HashMap::new();
        input_map.insert("a".to_string(), Some("start.a".to_string()));
        input_map.insert("b".to_string(), Some("start.b".to_string()));

        let node = Arc::new(CustomNode::new(
            config("sum", input_map, false),
            NodeContext::new(std::sync::Weak::new()),
            Arc::new(SumLogic),
        )) as Arc<dyn BaseNode>;
        let (workflow, ctx) = workflow_with_node(Arc::clone(&node));

        let mut start_outputs = HashMap::new();
        start_outputs.insert("a".to_string(), ParameterType::Step);
        start_outputs.insert("b".to_string(), ParameterType::Step);
        workflow
            .parameter_bus
            .init_output_parameters("start", &start_outputs);
        workflow
            .parameter_bus
            .set_value("start", "a", serde_json::json!(2))
            .await
            .expect("set a");
        workflow
            .parameter_bus
            .set_value("start", "b", serde_json::json!(3))
            .await
            .expect("set b");

        let run_task = tokio::spawn(async move { node.run(&ctx).await });
        workflow
            .choose_node("start", "sum")
            .await
            .expect("activate node");

        run_task.await.expect("join").expect("run sum node");

        let output = workflow
            .parameter_bus
            .get_value("sum.sum")
            .await
            .expect("get output");
        assert_eq!(output, serde_json::json!(5));
    }

    #[tokio::test]
    async fn test_custom_node_error_propagation() {
        struct FailLogic;

        #[async_trait]
        impl CustomLogic for FailLogic {
            fn input_keys(&self) -> Vec<String> {
                Vec::new()
            }

            fn output_key(&self) -> &str {
                "output"
            }

            async fn execute(
                &self,
                _inputs: HashMap<String, Value>,
                _ctx: &NodeContext,
            ) -> Result<Value> {
                Err(WorkflowError::Other("custom failure".to_string()))
            }
        }

        let node = Arc::new(CustomNode::new(
            config("fail", HashMap::new(), false),
            NodeContext::new(std::sync::Weak::new()),
            Arc::new(FailLogic),
        )) as Arc<dyn BaseNode>;
        let (workflow, ctx) = workflow_with_node(Arc::clone(&node));

        let run_task = tokio::spawn(async move { node.run(&ctx).await });
        workflow
            .choose_node("start", "fail")
            .await
            .expect("activate node");

        let err = run_task
            .await
            .expect("join")
            .expect_err("run should propagate custom error");
        assert!(err.to_string().contains("custom failure"));
    }

    #[tokio::test]
    async fn test_custom_node_no_input() {
        struct NoInputLogic;

        #[async_trait]
        impl CustomLogic for NoInputLogic {
            fn input_keys(&self) -> Vec<String> {
                Vec::new()
            }

            fn output_key(&self) -> &str {
                "output"
            }

            async fn execute(
                &self,
                inputs: HashMap<String, Value>,
                _ctx: &NodeContext,
            ) -> Result<Value> {
                assert!(inputs.is_empty());
                Ok(serde_json::json!("ok"))
            }
        }

        let node = Arc::new(CustomNode::new(
            config("no_input", HashMap::new(), false),
            NodeContext::new(std::sync::Weak::new()),
            Arc::new(NoInputLogic),
        )) as Arc<dyn BaseNode>;
        let (workflow, ctx) = workflow_with_node(Arc::clone(&node));

        let run_task = tokio::spawn(async move { node.run(&ctx).await });
        workflow
            .choose_node("start", "no_input")
            .await
            .expect("activate node");

        run_task.await.expect("join").expect("run no-input node");

        let output = workflow
            .parameter_bus
            .get_value("no_input.output")
            .await
            .expect("get output");
        assert_eq!(output, serde_json::json!("ok"));
    }
}
