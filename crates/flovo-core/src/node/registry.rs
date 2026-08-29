/// 节点注册表
///
/// 实现动态节点注册和创建功能
use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, CustomLogic, NodeConfig, NodeContext};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 节点工厂函数类型
///
/// 接收节点配置和上下文，返回节点实例
pub type NodeFactory =
    Arc<dyn Fn(NodeConfig, NodeContext) -> Result<Arc<dyn BaseNode>> + Send + Sync>;

/// 节点注册表
///
/// 管理节点类型和对应的工厂函数
pub struct NodeRegistry {
    /// 节点类型到工厂函数的映射
    factories: RwLock<HashMap<String, NodeFactory>>,
}

impl NodeRegistry {
    /// 创建新的节点注册表
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(HashMap::new()),
        }
    }

    /// 注册节点类型
    ///
    /// # 参数
    /// - node_type: 节点类型名称（与 JSON 配置中的 node_type 对应）
    /// - factory: 节点工厂函数
    ///
    /// # 示例
    /// ```no_run
    /// use flovo_core::node::NodeRegistry;
    ///
    /// let registry = NodeRegistry::new();
    /// registry.register("simple_node", |config, ctx| {
    ///     // 创建节点实例
    ///     # todo!()
    /// });
    /// ```
    pub fn register<F>(&self, node_type: &str, factory: F)
    where
        F: Fn(NodeConfig, NodeContext) -> Result<Arc<dyn BaseNode>> + Send + Sync + 'static,
    {
        let mut factories = self.factories.write();
        factories.insert(node_type.to_string(), Arc::new(factory));
    }

    /// 注册自定义逻辑节点。
    ///
    /// # 参数
    /// - `name`: 节点类型名称（与 JSON 配置中的 `node_type` 对应）。
    /// - `logic`: 实现了 `CustomLogic` 的业务逻辑对象。
    pub fn register_custom(&self, name: &str, logic: Arc<dyn CustomLogic>) {
        self.register(name, move |config, ctx| {
            Ok(Arc::new(crate::node::custom::CustomNode::new(
                config,
                ctx,
                logic.clone(),
            )))
        });
    }

    /// 创建节点实例
    ///
    /// # 参数
    /// - node_type: 节点类型名称
    /// - config: 节点配置
    /// - ctx: 节点上下文
    ///
    /// # 返回
    /// 返回创建的节点实例
    ///
    /// # 错误
    /// - 如果节点类型未注册，返回 `NodeTypeNotRegistered` 错误
    pub fn create_node(
        &self,
        node_type: &str,
        config: NodeConfig,
        ctx: NodeContext,
    ) -> Result<Arc<dyn BaseNode>> {
        let factories = self.factories.read();
        let factory = factories.get(node_type).ok_or_else(|| {
            WorkflowError::NodeTypeNotRegistered(format!(
                "Node type '{}' not registered",
                node_type
            ))
        })?;

        factory(config, ctx)
    }

    /// 检查节点类型是否已注册
    ///
    /// # 参数
    /// - node_type: 节点类型名称
    ///
    /// # 返回
    /// 如果节点类型已注册返回 true，否则返回 false
    pub fn is_registered(&self, node_type: &str) -> bool {
        self.factories.read().contains_key(node_type)
    }

    /// 获取所有已注册的节点类型
    ///
    /// # 返回
    /// 返回所有已注册的节点类型名称列表
    pub fn registered_types(&self) -> Vec<String> {
        self.factories.read().keys().cloned().collect()
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{BaseNode, NodeType};
    use crate::parameter_bus::ParameterType;
    use async_trait::async_trait;
    use std::collections::HashMap;

    // 测试节点
    struct TestNode {
        name: String,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
    }

    #[async_trait]
    impl BaseNode for TestNode {
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
            false
        }

        async fn run(&self, _ctx: &NodeContext) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_register_and_create() {
        let registry = NodeRegistry::new();

        // 注册测试节点
        registry.register("test_node", |config, _ctx| {
            Ok(Arc::new(TestNode {
                name: config.node_name,
                input_params: HashMap::new(),
                output_params: HashMap::new(),
                choices: Vec::new(),
            }))
        });

        // 验证注册成功
        assert!(registry.is_registered("test_node"));
        assert!(!registry.is_registered("unknown_node"));

        // 创建节点实例
        let config = NodeConfig {
            id: 1,
            node_name: "test1".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs: HashMap::new(),
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = registry.create_node("test_node", config, ctx).unwrap();

        assert_eq!(node.name(), "test1");
    }

    #[test]
    fn test_unregistered_type() {
        let registry = NodeRegistry::new();

        let config = NodeConfig {
            id: 1,
            node_name: "test1".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs: HashMap::new(),
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let result = registry.create_node("unknown", config, ctx);

        assert!(result.is_err());
    }

    #[test]
    fn test_registered_types_and_default() {
        // 验证：默认构造可用，registered_types 返回已注册节点类型列表。
        let registry = NodeRegistry::default();
        registry.register("alpha_node", |config, _ctx| {
            Ok(Arc::new(TestNode {
                name: config.node_name,
                input_params: HashMap::new(),
                output_params: HashMap::new(),
                choices: Vec::new(),
            }))
        });
        registry.register("beta_node", |config, _ctx| {
            Ok(Arc::new(TestNode {
                name: config.node_name,
                input_params: HashMap::new(),
                output_params: HashMap::new(),
                choices: Vec::new(),
            }))
        });

        let mut types = registry.registered_types();
        types.sort();
        assert_eq!(
            types,
            vec!["alpha_node".to_string(), "beta_node".to_string()]
        );
    }
}
