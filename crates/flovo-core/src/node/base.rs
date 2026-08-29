use crate::error::Result;
use crate::parameter_bus::ParameterType;
/// 节点基础类型
///
/// 定义工作流节点的核心接口和辅助类型
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

/// 节点类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    /// 基础节点
    Base,
    /// 步骤节点（step）
    Step,
    /// 流式节点（stream）
    Stream,
}

/// 节点配置
///
/// 从 JSON 配置转换而来的节点配置
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// 节点 ID
    pub id: u32,

    /// 节点名称
    pub node_name: String,

    /// 输入参数映射 (参数名 -> 来源参数键)
    /// 例如: {"input1": Some("node0.output1"), "input2": None}
    pub input_map: HashMap<String, Option<String>>,

    /// 选择映射 (选择名 -> 目标节点名)
    /// 例如: {"success": "node2", "failure": "node3"}
    pub choice_map: HashMap<String, String>,

    /// 节点属性
    pub attrs: HashMap<String, Value>,

    /// 是否为关键节点
    pub key_node: bool,
}

/// 节点上下文
///
/// 提供节点运行时需要的上下文信息，包括对工作流的访问
#[derive(Clone)]
pub struct NodeContext {
    /// 对工作流的弱引用（避免循环引用）
    pub workflow: Weak<crate::workflow::WorkFlow>,
}

impl NodeContext {
    /// 创建新的节点上下文
    pub fn new(workflow: Weak<crate::workflow::WorkFlow>) -> Self {
        Self { workflow }
    }

    /// 获取工作流的强引用
    pub fn get_workflow(&self) -> Result<Arc<crate::workflow::WorkFlow>> {
        self.workflow
            .upgrade()
            .ok_or_else(|| crate::error::WorkflowError::Other("Workflow has been dropped".into()))
    }
}

/// 节点基础 trait
///
/// 所有工作流节点都必须实现此 trait
#[async_trait]
pub trait BaseNode: Send + Sync {
    /// 获取节点名称
    fn name(&self) -> &str;

    /// 获取节点类型
    fn node_type(&self) -> NodeType;

    /// 获取输入参数定义
    ///
    /// 返回参数名到参数类型的映射
    fn input_parameters(&self) -> &HashMap<String, ParameterType>;

    /// 获取输出参数定义
    ///
    /// 返回参数名到参数类型的映射
    fn output_parameters(&self) -> &HashMap<String, ParameterType>;

    /// 获取可能的选择列表
    ///
    /// 返回所有可能的分支选择名称
    fn choices(&self) -> &[String];

    /// 是否为关键节点
    ///
    /// 关键节点不会被取消
    fn is_key_node(&self) -> bool;

    /// 是否可以运行
    ///
    /// 某些节点可能需要等待特定条件才能运行
    fn runable(&self) -> bool {
        true
    }

    /// 是否支持重复执行
    ///
    /// 可重入节点会在每次执行完成后继续等待下一次激活事件。
    fn is_reentrant(&self) -> bool {
        false
    }

    /// 运行节点
    ///
    /// 这是节点的核心逻辑，由具体节点实现
    async fn run(&self, ctx: &NodeContext) -> Result<()>;
}

/// 节点辅助工具
///
/// 提供节点常用操作的辅助方法
pub struct NodeHelper;

impl NodeHelper {
    /// 等待节点被激活
    ///
    /// # 参数
    /// - node_name: 节点名称
    /// - ctx: 节点上下文
    pub async fn wait_for_event(node_name: &str, ctx: &NodeContext) -> Result<()> {
        let workflow = ctx.get_workflow()?;
        workflow.wait_for_event(node_name).await
    }

    /// 获取输入参数
    ///
    /// # 参数
    /// - param_name: 参数名称（在本节点的输入参数中的名称）
    /// - input_map: 输入映射
    /// - ctx: 节点上下文
    ///
    /// # 返回
    /// 返回参数值，如果映射为 None 则返回 None
    pub async fn get_input(
        param_name: &str,
        input_map: &HashMap<String, Option<String>>,
        ctx: &NodeContext,
    ) -> Result<Option<Value>> {
        // 获取映射的源参数键
        let source_key = input_map.get(param_name).ok_or_else(|| {
            crate::error::WorkflowError::ConfigError(format!(
                "Input parameter '{}' not found in input_map",
                param_name
            ))
        })?;

        // 如果映射为 None，返回 None（允许空参数）
        if source_key.is_none() {
            return Ok(None);
        }

        let source_key = source_key.as_ref().unwrap();
        let workflow = ctx.get_workflow()?;

        // 约定：input_map 支持 context.<field>，直接从 workflow context 读取。
        if let Some(context_key) = source_key.strip_prefix("context.") {
            if context_key.is_empty() {
                return Err(crate::error::WorkflowError::ConfigError(
                    "context source key must be in format 'context.<field>'".to_string(),
                ));
            }
            let value = workflow.get_context(context_key).ok_or_else(|| {
                crate::error::WorkflowError::ConfigError(format!(
                    "Context key '{}' not found for source '{}'",
                    context_key, source_key
                ))
            })?;
            return Ok(Some(value));
        }

        let value = workflow.parameter_bus.get_value(source_key).await?;
        Ok(Some(value))
    }

    /// 设置输出参数
    ///
    /// # 参数
    /// - node_name: 节点名称
    /// - param_name: 参数名称
    /// - value: 参数值
    /// - ctx: 节点上下文
    pub async fn set_output(
        node_name: &str,
        param_name: &str,
        value: Value,
        ctx: &NodeContext,
    ) -> Result<()> {
        let workflow = ctx.get_workflow()?;
        workflow
            .parameter_bus
            .set_value(node_name, param_name, value)
            .await
    }

    /// 选择下一个节点
    ///
    /// # 参数
    /// - node_name: 当前节点名称
    /// - choice_name: 选择名称
    /// - choice_map: 选择映射
    /// - ctx: 节点上下文
    ///
    /// # 返回
    /// 返回选择的目标节点名称
    pub async fn set_choice(
        node_name: &str,
        choice_name: &str,
        choice_map: &HashMap<String, String>,
        ctx: &NodeContext,
    ) -> Result<String> {
        Self::set_choice_with_policy(node_name, choice_name, choice_map, ctx, true).await
    }

    /// 选择下一个节点并按策略决定是否取消其余分支
    pub async fn set_choice_with_policy(
        node_name: &str,
        choice_name: &str,
        choice_map: &HashMap<String, String>,
        ctx: &NodeContext,
        cancel_others: bool,
    ) -> Result<String> {
        // 获取目标节点名称
        let target_node = choice_map.get(choice_name).ok_or_else(|| {
            crate::error::WorkflowError::ConfigError(format!(
                "Choice '{}' not found in choice_map",
                choice_name
            ))
        })?;

        let workflow = ctx.get_workflow()?;

        // 通知工作流选择了这个节点
        workflow.choose_node(node_name, target_node).await?;

        if cancel_others {
            // 取消其他分支的节点（非关键节点）
            for other_target in choice_map.values() {
                if other_target != target_node {
                    workflow.cancel_node(other_target).await?;
                }
            }
        }

        Ok(target_node.clone())
    }

    /// 获取外部消息
    ///
    /// # 参数
    /// - ctx: 节点上下文
    ///
    /// # 返回
    /// 返回从外部接收的消息
    pub async fn get_message(ctx: &NodeContext) -> Result<Value> {
        let workflow = ctx.get_workflow()?;
        workflow.get_message().await
    }

    /// 读取 memory
    pub fn get_memory(key: &str, ctx: &NodeContext) -> Result<Option<Value>> {
        let workflow = ctx.get_workflow()?;
        Ok(workflow.get_memory(key))
    }

    /// 写入 memory
    pub fn set_memory(key: &str, value: Value, ctx: &NodeContext) -> Result<()> {
        let workflow = ctx.get_workflow()?;
        workflow.set_memory(key, value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameter_bus::ParameterType;
    use crate::workflow::WorkFlow;

    #[test]
    fn test_node_context_get_workflow_success() {
        // 验证：NodeContext 可以从弱引用升级为有效的工作流强引用。
        let workflow = WorkFlow::new("ctx_ok".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));
        let upgraded = ctx.get_workflow().unwrap();
        assert_eq!(upgraded.name, "ctx_ok");
    }

    #[test]
    fn test_node_context_get_workflow_dropped() {
        // 验证：当工作流已被释放时，返回明确错误而不是 panic。
        let workflow = WorkFlow::new("ctx_dropped".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));
        drop(workflow);

        let result = ctx.get_workflow();
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("Workflow has been dropped"));
        }
    }

    #[tokio::test]
    async fn test_node_helper_get_input_success() {
        // 验证：get_input 能按 input_map 正确读取参数总线中的 step 参数。
        let workflow = WorkFlow::new("input_ok".to_string());
        let mut outputs = HashMap::new();
        outputs.insert("value".to_string(), ParameterType::Step);
        workflow
            .parameter_bus
            .init_output_parameters("producer", &outputs);
        workflow
            .parameter_bus
            .set_value("producer", "value", serde_json::json!({"k": 1}))
            .await
            .unwrap();

        let mut input_map = HashMap::new();
        input_map.insert("input".to_string(), Some("producer.value".to_string()));
        let ctx = NodeContext::new(Arc::downgrade(&workflow));

        let value = NodeHelper::get_input("input", &input_map, &ctx)
            .await
            .unwrap();
        assert_eq!(value, Some(serde_json::json!({"k": 1})));
    }

    #[tokio::test]
    async fn test_node_helper_get_input_none_mapping() {
        // 验证：input_map 显式配置为 None 时，返回 Ok(None)。
        let workflow = WorkFlow::new("input_none".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));

        let mut input_map = HashMap::new();
        input_map.insert("optional".to_string(), None);

        let value = NodeHelper::get_input("optional", &input_map, &ctx)
            .await
            .unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_node_helper_get_input_missing_mapping_error() {
        // 验证：请求未在 input_map 定义的参数时，返回配置错误。
        let workflow = WorkFlow::new("input_missing".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));
        let input_map = HashMap::new();

        let err = NodeHelper::get_input("missing", &input_map, &ctx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found in input_map"));
    }

    #[tokio::test]
    async fn test_node_helper_set_output_success() {
        // 验证：set_output 可以写入参数总线并被后续读取。
        let workflow = WorkFlow::new("set_output".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));

        let mut outputs = HashMap::new();
        outputs.insert("result".to_string(), ParameterType::Step);
        workflow
            .parameter_bus
            .init_output_parameters("node1", &outputs);

        NodeHelper::set_output("node1", "result", serde_json::json!("ok"), &ctx)
            .await
            .unwrap();

        let value = workflow
            .parameter_bus
            .get_value("node1.result")
            .await
            .unwrap();
        assert_eq!(value, serde_json::json!("ok"));
    }

    #[tokio::test]
    async fn test_node_helper_set_choice_missing_choice_error() {
        // 验证：set_choice 在 choice_map 缺少选择项时返回配置错误。
        let workflow = WorkFlow::new("choice_error".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));
        let choice_map = HashMap::new();

        let err = NodeHelper::set_choice("node1", "missing", &choice_map, &ctx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found in choice_map"));
    }

    #[tokio::test]
    async fn test_node_helper_get_message_success() {
        // 验证：get_message 能正确读取由工作流外部注入的消息。
        let workflow = WorkFlow::new("message_ok".to_string());
        let ctx = NodeContext::new(Arc::downgrade(&workflow));
        let expected = serde_json::json!({"cmd": "ping"});
        workflow.add_message(expected.clone()).await.unwrap();

        let actual = NodeHelper::get_message(&ctx).await.unwrap();
        assert_eq!(actual, expected);
    }
}
