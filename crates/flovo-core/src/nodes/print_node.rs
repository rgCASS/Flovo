/// 打印节点
///
/// 接收输入并打印到标准输出，然后传递给下一个节点
use crate::error::Result;
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// 打印节点
///
/// 用于调试和日志记录，打印输入参数并传递给下一个节点
pub struct PrintNode {
    name: String,
    config: NodeConfig,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
}

impl PrintNode {
    /// 创建新的打印节点
    ///
    /// # 参数
    /// - config: 节点配置
    /// - _ctx: 节点上下文
    ///
    /// # 配置属性（attrs）
    /// - prefix: 打印前缀（可选，默认为节点名称）
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        // 定义输入参数：接收任意数据
        let mut input_params = HashMap::new();
        input_params.insert("input".to_string(), ParameterType::Step);

        // 定义输出参数：传递输入数据
        let mut output_params = HashMap::new();
        output_params.insert("output".to_string(), ParameterType::Step);

        // 定义选择项
        let choices = vec!["default".to_string()];

        Ok(Self {
            name: config.node_name.clone(),
            config,
            input_params,
            output_params,
            choices,
        })
    }

    /// 获取打印前缀
    fn get_prefix(&self) -> String {
        self.config
            .attrs
            .get("prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("[{}]", self.name))
    }
}

#[async_trait]
impl BaseNode for PrintNode {
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
        true
    }

    async fn run(&self, ctx: &NodeContext) -> Result<()> {
        // 等待激活
        NodeHelper::wait_for_event(&self.name, ctx).await?;

        // 获取输入
        let input = NodeHelper::get_input("input", &self.config.input_map, ctx)
            .await?
            .unwrap_or(Value::Null);

        // 打印输入
        let prefix = self.get_prefix();
        println!(
            "{} {}",
            prefix,
            serde_json::to_string_pretty(&input).unwrap()
        );

        // 将输入传递到输出
        NodeHelper::set_output(&self.name, "output", input, ctx).await?;

        // 选择下一个节点
        NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print_node_creation() {
        let config = NodeConfig {
            id: 1,
            node_name: "test_print".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs: HashMap::new(),
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = PrintNode::new(config, ctx).unwrap();

        assert_eq!(node.name(), "test_print");
        assert_eq!(node.choices(), &["default"]);
        assert!(node.input_parameters().contains_key("input"));
        assert!(node.output_parameters().contains_key("output"));
    }

    #[test]
    fn test_print_node_with_prefix() {
        let mut attrs = HashMap::new();
        attrs.insert("prefix".to_string(), serde_json::json!(">>> DEBUG >>>"));

        let config = NodeConfig {
            id: 1,
            node_name: "test_print".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = PrintNode::new(config, ctx).unwrap();

        assert_eq!(node.get_prefix(), ">>> DEBUG >>>");
    }
}
