#[cfg(feature = "context-sync")]
use crate::config::load_workflow_config;
/// 工作流构建器
///
/// 从 JSON 配置和节点注册表构建完整的工作流实例
use crate::config::{NodeConfigJson, WorkflowConfig};
#[cfg(feature = "context-sync")]
use crate::context_sync::{ContextSyncConfig, ContextSyncManager};
use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, NodeContext, NodeRegistry};
use crate::parameter_bus::ParameterType;
use crate::workflow::WorkFlow;
use std::collections::HashMap;
#[cfg(feature = "context-sync")]
use std::path::Path;
use std::sync::Arc;

/// 工作流构建器
///
/// 负责从配置构建可执行的工作流实例
pub struct WorkflowBuilder {
    /// 节点注册表
    registry: Arc<NodeRegistry>,

    /// 工作流配置
    configs: HashMap<String, WorkflowConfig>,

    /// 上下文同步管理器（可选）。
    #[cfg(feature = "context-sync")]
    context_sync: Option<ContextSyncManager>,

    /// 从统一 JSON 加载的 context_sync 原始配置（可选）。
    #[cfg(feature = "context-sync")]
    context_sync_config: Option<ContextSyncConfig>,
}

impl WorkflowBuilder {
    /// 创建新的工作流构建器
    ///
    /// # 参数
    /// - registry: 节点注册表
    /// - configs: 工作流配置映射
    pub fn new(registry: Arc<NodeRegistry>, configs: HashMap<String, WorkflowConfig>) -> Self {
        Self {
            registry,
            configs,
            #[cfg(feature = "context-sync")]
            context_sync: None,
            #[cfg(feature = "context-sync")]
            context_sync_config: None,
        }
    }

    /// 注入上下文同步管理器。
    #[cfg(feature = "context-sync")]
    pub fn with_context_sync(mut self, manager: ContextSyncManager) -> Self {
        self.context_sync = Some(manager);
        self
    }

    /// 从统一 JSON 文件加载 context_sync 配置（不修改已有工作流节点配置）。
    ///
    /// 说明：
    /// - 该方法只负责读取并缓存 `context_sync` 配置；
    /// - 真正的同步执行依赖外部注入的 `ContextSyncManager`（`with_context_sync`）；
    /// - 若 JSON 中 `context_sync.enabled=true` 且未注入 manager，`build` 会返回配置错误。
    #[cfg(feature = "context-sync")]
    pub fn with_context_sync_from_json(mut self, json_path: &Path) -> Result<Self> {
        let config = load_workflow_config(json_path)?;
        self.context_sync_config = config.context_sync;
        Ok(self)
    }

    /// 构建工作流
    ///
    /// # 参数
    /// - name: 工作流名称
    ///
    /// # 返回
    /// 返回构建好的工作流实例
    ///
    /// # 错误
    /// - 工作流名称不存在
    /// - 节点类型未注册
    /// - 节点配置验证失败
    pub fn build(&self, name: &str) -> Result<Arc<WorkFlow>> {
        // 1. 获取配置
        let config = self.configs.get(name).ok_or_else(|| {
            WorkflowError::ConfigError(format!("Workflow '{}' not found in configuration", name))
        })?;

        // 2. 创建 WorkFlow 实例
        let workflow = WorkFlow::new(name.to_string());

        // 3. 初始化输入参数
        self.init_input_parameters(&workflow, &config.input_parameters)?;

        // 4. 创建节点列表
        let nodes = self.create_nodes(&config.nodes, &workflow)?;

        // 5. 验证节点配置
        self.validate_nodes(&nodes, &config.nodes)?;

        // 6. 设置节点列表
        workflow.set_nodes(nodes)?;

        // 7. 设置起始节点
        workflow.set_start_node(&config.start_node);

        #[cfg(feature = "context-sync")]
        {
            if let Some(manager) = &self.context_sync {
                workflow.set_context_sync(manager.clone());
            } else {
                let enabled_in_workflow_config = config
                    .context_sync
                    .as_ref()
                    .map(|ctx| ctx.enabled)
                    .unwrap_or(false);
                let enabled_in_unified_config = self
                    .context_sync_config
                    .as_ref()
                    .map(|ctx| ctx.enabled)
                    .unwrap_or(false);
                if enabled_in_workflow_config || enabled_in_unified_config {
                    return Err(WorkflowError::ConfigError(
                        "context_sync is enabled in workflow configuration, but ContextSyncManager was not injected"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(workflow)
    }

    /// 初始化输入参数
    ///
    /// # 参数
    /// - workflow: 工作流实例
    /// - input_parameters: 输入参数映射 (参数名 -> 参数类型字符串)
    fn init_input_parameters(
        &self,
        workflow: &Arc<WorkFlow>,
        input_parameters: &HashMap<String, String>,
    ) -> Result<()> {
        let mut params = HashMap::new();

        // 将字符串类型转换为 ParameterType
        for (name, type_str) in input_parameters {
            let param_type = ParameterType::from_str(type_str)?;
            params.insert(name.clone(), param_type);
        }

        // 初始化到参数总线
        workflow
            .parameter_bus
            .init_output_parameters("start", &params);

        Ok(())
    }

    /// 创建节点列表
    ///
    /// # 参数
    /// - node_configs: 节点配置列表
    /// - workflow: 工作流实例
    ///
    /// # 返回
    /// 返回创建的节点列表
    fn create_nodes(
        &self,
        node_configs: &[NodeConfigJson],
        workflow: &Arc<WorkFlow>,
    ) -> Result<Vec<Arc<dyn BaseNode>>> {
        let mut nodes = Vec::new();

        // 创建 NodeContext
        let ctx = NodeContext::new(Arc::downgrade(workflow));

        for json_config in node_configs {
            // 检查节点类型是否已注册
            if !self.registry.is_registered(&json_config.node_type) {
                return Err(WorkflowError::NodeTypeNotRegistered(format!(
                    "Node type '{}' not registered for node '{}'",
                    json_config.node_type, json_config.node_name
                )));
            }

            // 转换为 NodeConfig
            let config = json_config.to_node_config();

            // 使用注册表创建节点
            let node = self
                .registry
                .create_node(&json_config.node_type, config, ctx.clone())?;

            nodes.push(node);
        }

        Ok(nodes)
    }

    /// 验证节点配置
    ///
    /// # 参数
    /// - nodes: 节点列表
    /// - node_configs: 节点配置列表
    ///
    /// # 验证项
    /// - choice_map 中的选择名必须在节点的 choices() 中定义
    /// - choice_map 中的目标节点必须存在或为 "finish"
    /// - input_map 中引用的参数必须存在
    fn validate_nodes(
        &self,
        nodes: &[Arc<dyn BaseNode>],
        node_configs: &[NodeConfigJson],
    ) -> Result<()> {
        // 构建节点名称集合
        let mut node_names = std::collections::HashSet::new();
        for node in nodes {
            node_names.insert(node.name().to_string());
        }

        // 验证每个节点
        for (node, config) in nodes.iter().zip(node_configs.iter()) {
            // 验证 choice_map
            let choices = node.choices();
            for (choice_name, target_node) in &config.choice_map {
                // 检查选择名是否在 choices 中定义
                if !choices.contains(choice_name) {
                    return Err(WorkflowError::ConfigError(format!(
                        "Node '{}': choice '{}' in choice_map not found in node's choices {:?}",
                        node.name(),
                        choice_name,
                        choices
                    )));
                }

                // 检查目标节点是否存在
                if target_node != "finish" && !node_names.contains(target_node) {
                    return Err(WorkflowError::ConfigError(format!(
                        "Node '{}': target node '{}' in choice_map does not exist",
                        node.name(),
                        target_node
                    )));
                }
            }

            // 验证 input_map 中的参数键格式
            for (param_name, source_key) in &config.input_map {
                // 检查参数名是否在节点的输入参数中定义
                if !node.input_parameters().contains_key(param_name) {
                    return Err(WorkflowError::ConfigError(format!(
                        "Node '{}': parameter '{}' in input_map not found in node's input_parameters",
                        node.name(),
                        param_name
                    )));
                }

                // 如果 source_key 存在，验证其格式 (应为 "node_name.param_name" 或 "start.param_name")
                if let Some(key) = source_key {
                    if !key.contains('.') {
                        return Err(WorkflowError::ConfigError(format!(
                            "Node '{}': invalid source key '{}' in input_map, expected format 'node_name.param_name'",
                            node.name(),
                            key
                        )));
                    }

                    // 提取源节点名称
                    let source_node_name = key.split('.').next().unwrap();
                    if source_node_name != "start"
                        && source_node_name != "context"
                        && !node_names.contains(source_node_name)
                    {
                        return Err(WorkflowError::ConfigError(format!(
                            "Node '{}': source node '{}' in input_map does not exist",
                            node.name(),
                            source_node_name
                        )));
                    }
                }
            }

            if config.node_type == "while_node" {
                self.validate_while_node_config(config)?;
            }
        }

        Ok(())
    }

    fn validate_while_node_config(&self, config: &NodeConfigJson) -> Result<()> {
        if !config.choice_map.contains_key("continue") || !config.choice_map.contains_key("exit") {
            return Err(WorkflowError::ConfigError(format!(
                "Node '{}': while_node choice_map must contain 'continue' and 'exit'",
                config.node_name
            )));
        }

        let memory_key = config.attrs.get("memory_key").and_then(|v| v.as_str());
        if memory_key.is_none() {
            return Err(WorkflowError::ConfigError(format!(
                "Node '{}': while_node requires string attrs.memory_key",
                config.node_name
            )));
        }

        let condition_type = config
            .attrs
            .get("condition_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WorkflowError::ConfigError(format!(
                    "Node '{}': while_node requires string attrs.condition_type",
                    config.node_name
                ))
            })?;

        let max_iterations = config.attrs.get("max_iterations").and_then(|v| v.as_u64());
        if max_iterations.unwrap_or(0) == 0 {
            return Err(WorkflowError::ConfigError(format!(
                "Node '{}': while_node requires attrs.max_iterations > 0",
                config.node_name
            )));
        }

        let condition_needs_compare = matches!(
            condition_type.to_lowercase().as_str(),
            "equals"
                | "eq"
                | "not_equals"
                | "ne"
                | "greater_than"
                | "gt"
                | "less_than"
                | "lt"
                | "contains"
        );
        if condition_needs_compare && !config.attrs.contains_key("compare_value") {
            return Err(WorkflowError::ConfigError(format!(
                "Node '{}': while_node condition_type '{}' requires attrs.compare_value",
                config.node_name, condition_type
            )));
        }

        if let Some(inc) = config.attrs.get("increment") {
            if !inc.is_number() {
                return Err(WorkflowError::ConfigError(format!(
                    "Node '{}': while_node attrs.increment must be numeric",
                    config.node_name
                )));
            }
        }

        Ok(())
    }

    /// 获取所有可用的工作流名称
    pub fn workflow_names(&self) -> Vec<String> {
        self.configs.keys().cloned().collect()
    }

    /// 检查工作流是否存在
    pub fn has_workflow(&self, name: &str) -> bool {
        self.configs.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{BaseNode, NodeContext, NodeType};
    use crate::parameter_bus::ParameterType;
    use async_trait::async_trait;
    use std::collections::HashMap;

    // 简单测试节点
    struct TestNode {
        name: String,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
        key_node: bool,
    }

    impl TestNode {
        fn new(
            name: String,
            input_params: HashMap<String, ParameterType>,
            output_params: HashMap<String, ParameterType>,
            choices: Vec<String>,
            key_node: bool,
        ) -> Self {
            Self {
                name,
                input_params,
                output_params,
                choices,
                key_node,
            }
        }
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
            self.key_node
        }

        async fn run(&self, _ctx: &NodeContext) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_workflow_builder_basic() {
        // 创建注册表
        let registry = Arc::new(NodeRegistry::new());

        // 注册测试节点类型
        registry.register("test_node", |config, _ctx| {
            let mut output_params = HashMap::new();
            output_params.insert("result".to_string(), ParameterType::Step);

            Ok(Arc::new(TestNode::new(
                config.node_name,
                HashMap::new(),
                output_params,
                vec!["next".to_string()],
                config.key_node,
            )))
        });

        // 创建配置
        let mut configs = HashMap::new();
        let mut input_parameters = HashMap::new();
        input_parameters.insert("input".to_string(), "step".to_string());

        let mut node1_choice_map = HashMap::new();
        node1_choice_map.insert("next".to_string(), "finish".to_string());

        let node1_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map: HashMap::new(),
            choice_map: node1_choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };

        let workflow_config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: Some(false),
            input_parameters,
            nodes: vec![node1_config],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        configs.insert("test_workflow".to_string(), workflow_config);

        // 创建构建器
        let builder = WorkflowBuilder::new(registry, configs);

        // 构建工作流
        let workflow = builder.build("test_workflow").unwrap();

        assert_eq!(workflow.name, "test_workflow");
    }

    #[test]
    fn test_workflow_builder_validation_invalid_choice() {
        let registry = Arc::new(NodeRegistry::new());

        // 注册节点，但 choices 中只有 "next"
        registry.register("test_node", |config, _ctx| {
            Ok(Arc::new(TestNode::new(
                config.node_name,
                HashMap::new(),
                HashMap::new(),
                vec!["next".to_string()], // 只定义了 "next"
                false,
            )))
        });

        let mut configs = HashMap::new();
        let mut node1_choice_map = HashMap::new();
        // 但配置中使用了 "invalid_choice"
        node1_choice_map.insert("invalid_choice".to_string(), "finish".to_string());

        let node1_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map: HashMap::new(),
            choice_map: node1_choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };

        let workflow_config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![node1_config],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        configs.insert("test_workflow".to_string(), workflow_config);

        let builder = WorkflowBuilder::new(registry, configs);

        // 应该验证失败
        let result = builder.build("test_workflow");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e
                .to_string()
                .contains("choice 'invalid_choice' in choice_map not found"));
        }
    }

    #[test]
    fn test_workflow_builder_validation_invalid_target_node() {
        let registry = Arc::new(NodeRegistry::new());

        registry.register("test_node", |config, _ctx| {
            Ok(Arc::new(TestNode::new(
                config.node_name,
                HashMap::new(),
                HashMap::new(),
                vec!["next".to_string()],
                false,
            )))
        });

        let mut configs = HashMap::new();
        let mut node1_choice_map = HashMap::new();
        // 目标节点 "node2" 不存在
        node1_choice_map.insert("next".to_string(), "node2".to_string());

        let node1_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map: HashMap::new(),
            choice_map: node1_choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };

        let workflow_config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![node1_config],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        configs.insert("test_workflow".to_string(), workflow_config);

        let builder = WorkflowBuilder::new(registry, configs);

        // 应该验证失败
        let result = builder.build("test_workflow");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e
                .to_string()
                .contains("target node 'node2' in choice_map does not exist"));
        }
    }

    #[test]
    fn test_workflow_names_and_has_workflow() {
        // 验证：workflow_names 和 has_workflow 能反映配置集合状态。
        let registry = Arc::new(NodeRegistry::new());
        let mut configs = HashMap::new();
        configs.insert(
            "wf_a".to_string(),
            WorkflowConfig {
                start_node: "node1".to_string(),
                listen_at_start: None,
                input_parameters: HashMap::new(),
                nodes: Vec::new(),
                #[cfg(feature = "context-sync")]
                context_sync: None,
            },
        );
        configs.insert(
            "wf_b".to_string(),
            WorkflowConfig {
                start_node: "node1".to_string(),
                listen_at_start: Some(false),
                input_parameters: HashMap::new(),
                nodes: Vec::new(),
                #[cfg(feature = "context-sync")]
                context_sync: None,
            },
        );

        let builder = WorkflowBuilder::new(registry, configs);
        let names = builder.workflow_names();
        assert_eq!(names.len(), 2);
        assert!(builder.has_workflow("wf_a"));
        assert!(!builder.has_workflow("wf_missing"));
    }

    #[test]
    fn test_build_workflow_not_found() {
        // 验证：构建不存在的工作流返回配置错误。
        let builder = WorkflowBuilder::new(Arc::new(NodeRegistry::new()), HashMap::new());
        let result = builder.build("missing_workflow");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("not found in configuration"));
        }
    }

    #[test]
    fn test_workflow_builder_validation_invalid_input_param_name() {
        // 验证：input_map 中未在节点输入定义里的参数名会触发校验错误。
        let registry = Arc::new(NodeRegistry::new());
        registry.register("test_node", |config, _ctx| {
            let mut input_params = HashMap::new();
            input_params.insert("expected".to_string(), ParameterType::Step);
            Ok(Arc::new(TestNode::new(
                config.node_name,
                input_params,
                HashMap::new(),
                vec!["next".to_string()],
                false,
            )))
        });

        let mut configs = HashMap::new();
        let mut input_map = HashMap::new();
        input_map.insert(
            "unexpected".to_string(),
            Some("start.user_input".to_string()),
        );
        let mut choice_map = HashMap::new();
        choice_map.insert("next".to_string(), "finish".to_string());

        let node_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map,
            choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };

        configs.insert(
            "wf_invalid_input_param".to_string(),
            WorkflowConfig {
                start_node: "node1".to_string(),
                listen_at_start: None,
                input_parameters: HashMap::new(),
                nodes: vec![node_config],
                #[cfg(feature = "context-sync")]
                context_sync: None,
            },
        );

        let builder = WorkflowBuilder::new(registry, configs);
        let result = builder.build("wf_invalid_input_param");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err
                .to_string()
                .contains("in input_map not found in node's input_parameters"));
        }
    }

    #[test]
    fn test_workflow_builder_validation_invalid_source_key_format() {
        // 验证：input_map 的 source key 不是 node.param 格式时会被拒绝。
        let registry = Arc::new(NodeRegistry::new());
        registry.register("test_node", |config, _ctx| {
            let mut input_params = HashMap::new();
            input_params.insert("expected".to_string(), ParameterType::Step);
            Ok(Arc::new(TestNode::new(
                config.node_name,
                input_params,
                HashMap::new(),
                vec!["next".to_string()],
                false,
            )))
        });

        let mut configs = HashMap::new();
        let mut input_map = HashMap::new();
        input_map.insert("expected".to_string(), Some("invalid_key".to_string()));
        let mut choice_map = HashMap::new();
        choice_map.insert("next".to_string(), "finish".to_string());

        let node_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map,
            choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };

        configs.insert(
            "wf_invalid_source_key".to_string(),
            WorkflowConfig {
                start_node: "node1".to_string(),
                listen_at_start: None,
                input_parameters: HashMap::new(),
                nodes: vec![node_config],
                #[cfg(feature = "context-sync")]
                context_sync: None,
            },
        );

        let builder = WorkflowBuilder::new(registry, configs);
        let result = builder.build("wf_invalid_source_key");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err
                .to_string()
                .contains("expected format 'node_name.param_name'"));
        }
    }

    #[test]
    fn test_while_node_requires_continue_and_exit_choices() {
        let registry = Arc::new(NodeRegistry::new());
        registry.register("while_node", |config, _ctx| {
            let mut output = HashMap::new();
            output.insert("current".to_string(), ParameterType::Step);
            Ok(Arc::new(TestNode::new(
                config.node_name,
                HashMap::new(),
                output,
                vec!["continue".to_string(), "exit".to_string()],
                false,
            )))
        });

        let mut attrs = HashMap::new();
        attrs.insert("memory_key".to_string(), serde_json::json!("counter"));
        attrs.insert("condition_type".to_string(), serde_json::json!("less_than"));
        attrs.insert("compare_value".to_string(), serde_json::json!(3));
        attrs.insert("max_iterations".to_string(), serde_json::json!(10));

        let mut choice_map = HashMap::new();
        choice_map.insert("continue".to_string(), "finish".to_string());

        let node_config = NodeConfigJson {
            id: 1,
            node_type: "while_node".to_string(),
            node_name: "loop".to_string(),
            input_map: HashMap::new(),
            choice_map,
            attrs,
            key_node: false,
        };

        let mut configs = HashMap::new();
        configs.insert(
            "wf_while_invalid_choice".to_string(),
            WorkflowConfig {
                start_node: "loop".to_string(),
                listen_at_start: None,
                input_parameters: HashMap::new(),
                nodes: vec![node_config],
                #[cfg(feature = "context-sync")]
                context_sync: None,
            },
        );

        let builder = WorkflowBuilder::new(registry, configs);
        let result = builder.build("wf_while_invalid_choice");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err
                .to_string()
                .contains("must contain 'continue' and 'exit'"));
        }
    }

    #[cfg(feature = "context-sync")]
    #[test]
    fn test_with_context_sync_from_json_requires_manager_when_enabled() {
        let registry = Arc::new(NodeRegistry::new());
        registry.register("test_node", |config, _ctx| {
            Ok(Arc::new(TestNode::new(
                config.node_name,
                HashMap::new(),
                HashMap::new(),
                vec!["next".to_string()],
                false,
            )))
        });

        let mut choice_map = HashMap::new();
        choice_map.insert("next".to_string(), "finish".to_string());
        let node_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map: HashMap::new(),
            choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };
        let mut configs = HashMap::new();
        configs.insert(
            "wf".to_string(),
            WorkflowConfig {
                start_node: "node1".to_string(),
                listen_at_start: None,
                input_parameters: HashMap::new(),
                nodes: vec![node_config],
                context_sync: None,
            },
        );

        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("workflow_unified.json");
        std::fs::write(
            &path,
            r#"
{
  "workflow": {
    "name": "wf",
    "version": "1.0"
  },
  "context_sync": {
    "enabled": true,
    "fetch_on_start": [],
    "push_on_complete": []
  }
}
"#,
        )
        .expect("write unified json");

        let builder = WorkflowBuilder::new(registry, configs)
            .with_context_sync_from_json(&path)
            .expect("load context sync config");
        let result = builder.build("wf");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err
                .to_string()
                .contains("ContextSyncManager was not injected"));
        }
    }

    #[cfg(feature = "context-sync")]
    #[test]
    fn test_build_requires_manager_when_context_sync_enabled_in_workflow_config() {
        let registry = Arc::new(NodeRegistry::new());
        registry.register("test_node", |config, _ctx| {
            Ok(Arc::new(TestNode::new(
                config.node_name,
                HashMap::new(),
                HashMap::new(),
                vec!["next".to_string()],
                false,
            )))
        });

        let mut choice_map = HashMap::new();
        choice_map.insert("next".to_string(), "finish".to_string());
        let node_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map: HashMap::new(),
            choice_map,
            attrs: HashMap::new(),
            key_node: false,
        };

        let mut configs = HashMap::new();
        configs.insert(
            "wf".to_string(),
            WorkflowConfig {
                start_node: "node1".to_string(),
                listen_at_start: None,
                input_parameters: HashMap::new(),
                nodes: vec![node_config],
                context_sync: Some(ContextSyncConfig {
                    enabled: true,
                    fetch_on_start: Vec::new(),
                    push_on_complete: Vec::new(),
                }),
            },
        );

        let builder = WorkflowBuilder::new(registry, configs);
        let result = builder.build("wf");
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err
                .to_string()
                .contains("ContextSyncManager was not injected"));
        }
    }
}
