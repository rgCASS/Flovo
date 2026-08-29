//! 工作流配置验证器
//!
//! 在工作流构建前进行全面的配置检查，提前发现错误并提供修复建议。

use crate::config::{NodeConfigJson, WorkflowConfig};
use crate::node::NodeRegistry;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// 验证错误
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// 错误位置（节点ID或字段名）
    pub location: String,
    /// 错误消息
    pub message: String,
    /// 修复建议
    pub suggestion: String,
}

impl ValidationError {
    fn new(
        location: impl Into<String>,
        message: impl Into<String>,
        suggestion: impl Into<String>,
    ) -> Self {
        Self {
            location: location.into(),
            message: message.into(),
            suggestion: suggestion.into(),
        }
    }
}

/// 配置验证器
pub struct Validator<'a> {
    config: &'a WorkflowConfig,
    registry: &'a NodeRegistry,
    errors: Vec<ValidationError>,
}

impl<'a> Validator<'a> {
    /// 创建验证器
    pub fn new(config: &'a WorkflowConfig, registry: &'a NodeRegistry) -> Self {
        Self {
            config,
            registry,
            errors: Vec::new(),
        }
    }

    /// 执行完整验证
    pub fn validate(mut self) -> Result<(), Vec<ValidationError>> {
        self.check_node_ids();
        self.check_start_node();
        self.check_choice_map();
        self.check_input_map();
        self.check_node_types();
        self.check_node_attrs();

        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }

    /// 检查节点ID唯一性
    fn check_node_ids(&mut self) {
        let mut seen_ids = HashSet::new();
        for node in &self.config.nodes {
            if !seen_ids.insert(node.id) {
                self.errors.push(ValidationError::new(
                    format!("node_id={}", node.id),
                    format!("Duplicate node ID: {}", node.id),
                    "Each node must have a unique ID. Change the duplicate ID to a unique value.",
                ));
            }
        }
    }

    /// 检查起始节点存在
    fn check_start_node(&mut self) {
        let node_names: HashSet<_> = self.config.nodes.iter().map(|n| &n.node_name).collect();
        if !node_names.contains(&self.config.start_node) {
            self.errors.push(ValidationError::new(
                "start_node",
                format!(
                    "Start node '{}' not found in nodes list",
                    self.config.start_node
                ),
                format!(
                    "Add a node with name '{}' or change start_node to an existing node name",
                    self.config.start_node
                ),
            ));
        }
    }

    /// 检查choice_map中的目标节点存在
    fn check_choice_map(&mut self) {
        let node_names: HashSet<_> = self.config.nodes.iter().map(|n| &n.node_name).collect();
        for node in &self.config.nodes {
            for (choice, target) in &node.choice_map {
                if target == "finish" {
                    continue;
                }
                if !node_names.contains(target) {
                    self.errors.push(ValidationError::new(
                        format!("node={}, choice_map.{}", node.node_name, choice),
                        format!("Target node '{}' not found", target),
                        format!(
                            "Add a node with name '{}' or change the target to an existing node",
                            target
                        ),
                    ));
                }
            }
        }
    }

    /// 检查input_map中的参数引用
    fn check_input_map(&mut self) {
        let node_names: HashSet<&str> = self
            .config
            .nodes
            .iter()
            .map(|n| n.node_name.as_str())
            .collect();

        for node in &self.config.nodes {
            for (param_name, source) in &node.input_map {
                if let Some(source_key) = source {
                    if source_key.is_empty() {
                        continue;
                    }

                    // 检查是否是工作流输入参数
                    if self.config.input_parameters.contains_key(source_key) {
                        continue;
                    }

                    // 检查是否是节点引用（格式：node_name 或 node_name.param）
                    let parts: Vec<&str> = source_key.split('.').collect();
                    if !parts.is_empty() {
                        // 仅 "start.xxx" 视为工作流输入参数引用；裸 "start" 仍需按节点名校验
                        if (source_key.starts_with("start.") || source_key.starts_with("context."))
                            && parts.len() > 1
                        {
                            continue;
                        }

                        if !node_names.contains(parts[0]) {
                            self.errors.push(ValidationError::new(
                                format!("node={}, input_map.{}", node.node_name, param_name),
                                format!(
                                    "Source node '{}' not found in parameter reference '{}'",
                                    parts[0], source_key
                                ),
                                format!(
                                    "Ensure node '{}' exists or correct the parameter reference",
                                    parts[0]
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }

    /// 检查节点类型已注册
    fn check_node_types(&mut self) {
        for node in &self.config.nodes {
            if !self.registry.is_registered(&node.node_type) {
                self.errors.push(ValidationError::new(
                    format!("node={}", node.node_name),
                    format!("Node type '{}' not registered", node.node_type),
                    format!(
                        "Register the node type '{}' or use a registered type: {:?}",
                        node.node_type,
                        self.registry.registered_types()
                    ),
                ));
            }
        }
    }

    /// 检查节点属性
    fn check_node_attrs(&mut self) {
        for node in &self.config.nodes {
            if node.node_type == "while_node" {
                self.check_while_node_attrs(node);
            }
        }
    }

    /// 检查while节点属性
    fn check_while_node_attrs(&mut self, node: &NodeConfigJson) {
        let attrs = &node.attrs;

        if !attrs.contains_key("memory_key") {
            self.errors.push(ValidationError::new(
                format!("node={}", node.node_name),
                "while_node requires 'memory_key' attribute",
                "Add 'memory_key' to attrs with a string value",
            ));
        }

        if !attrs.contains_key("condition_type") {
            self.errors.push(ValidationError::new(
                format!("node={}", node.node_name),
                "while_node requires 'condition_type' attribute",
                "Add 'condition_type' to attrs (e.g., 'less_than', 'greater_than', 'equals')",
            ));
        }

        if let Some(max_iter) = attrs.get("max_iterations") {
            if let Some(n) = max_iter.as_u64() {
                if n == 0 {
                    self.errors.push(ValidationError::new(
                        format!("node={}", node.node_name),
                        "max_iterations must be greater than 0",
                        "Set max_iterations to a positive integer",
                    ));
                }
            } else {
                self.errors.push(ValidationError::new(
                    format!("node={}", node.node_name),
                    "max_iterations must be a positive integer",
                    "Set max_iterations to a positive integer value (not negative, float, or string)",
                ));
            }
        } else {
            self.errors.push(ValidationError::new(
                format!("node={}", node.node_name),
                "while_node requires 'max_iterations' attribute",
                "Add 'max_iterations' to attrs with a positive integer value",
            ));
        }

        if let Some(condition_type) = attrs.get("condition_type").and_then(|v| v.as_str()) {
            if matches!(
                condition_type,
                "less_than"
                    | "greater_than"
                    | "equals"
                    | "not_equals"
                    | "less_or_equal"
                    | "greater_or_equal"
            ) && !attrs.contains_key("compare_value")
            {
                self.errors.push(ValidationError::new(
                    format!("node={}", node.node_name),
                    format!(
                        "condition_type '{}' requires 'compare_value' attribute",
                        condition_type
                    ),
                    "Add 'compare_value' to attrs",
                ));
            }
        }

        if !node.choice_map.contains_key("continue") || !node.choice_map.contains_key("exit") {
            self.errors.push(ValidationError::new(
                format!("node={}", node.node_name),
                "while_node requires both 'continue' and 'exit' in choice_map",
                "Add both 'continue' and 'exit' keys to choice_map",
            ));
        }
    }
}

/// 验证工作流配置
pub fn validate_config(
    config: &WorkflowConfig,
    registry: &NodeRegistry,
) -> Result<(), Vec<ValidationError>> {
    Validator::new(config, registry).validate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_registry() -> NodeRegistry {
        let registry = NodeRegistry::new();
        registry.register("test_node", |config, _ctx| {
            use crate::node::{BaseNode, NodeType};
            use crate::parameter_bus::ParameterType;
            use async_trait::async_trait;
            use std::sync::Arc;

            #[derive(Debug)]
            struct TestNode {
                name: String,
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
                    static INPUT_PARAMETERS: once_cell::sync::Lazy<HashMap<String, ParameterType>> =
                        once_cell::sync::Lazy::new(HashMap::new);
                    &INPUT_PARAMETERS
                }
                fn output_parameters(&self) -> &HashMap<String, ParameterType> {
                    static OUTPUT_PARAMETERS: once_cell::sync::Lazy<
                        HashMap<String, ParameterType>,
                    > = once_cell::sync::Lazy::new(HashMap::new);
                    &OUTPUT_PARAMETERS
                }
                fn choices(&self) -> &[String] {
                    &[]
                }
                fn is_key_node(&self) -> bool {
                    false
                }
                async fn run(&self, _ctx: &crate::node::NodeContext) -> crate::error::Result<()> {
                    Ok(())
                }
            }

            Ok(Arc::new(TestNode {
                name: config.node_name,
            }))
        });
        registry.register("while_node", |_, _| {
            Ok(Arc::new(
                crate::nodes::WhileNode::new(
                    crate::node::NodeConfig {
                        id: 1,
                        node_name: "test".to_string(),
                        input_map: HashMap::new(),
                        choice_map: HashMap::new(),
                        attrs: HashMap::new(),
                        key_node: false,
                    },
                    crate::node::NodeContext::new(std::sync::Weak::new()),
                )
                .unwrap(),
            ))
        });
        registry
    }

    #[test]
    fn test_duplicate_node_ids() {
        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![
                NodeConfigJson {
                    id: 1,
                    node_type: "test_node".to_string(),
                    node_name: "node1".to_string(),
                    input_map: HashMap::new(),
                    choice_map: HashMap::new(),
                    attrs: HashMap::new(),
                    key_node: false,
                },
                NodeConfigJson {
                    id: 1,
                    node_type: "test_node".to_string(),
                    node_name: "node2".to_string(),
                    input_map: HashMap::new(),
                    choice_map: HashMap::new(),
                    attrs: HashMap::new(),
                    key_node: false,
                },
            ],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.message.contains("Duplicate node ID")));
    }

    #[test]
    fn test_start_node_not_found() {
        let config = WorkflowConfig {
            start_node: "nonexistent".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "test_node".to_string(),
                node_name: "node1".to_string(),
                input_map: HashMap::new(),
                choice_map: HashMap::new(),
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.message.contains("Start node")));
    }

    #[test]
    fn test_choice_map_target_not_found() {
        let mut choice_map = HashMap::new();
        choice_map.insert("default".to_string(), "nonexistent".to_string());

        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "test_node".to_string(),
                node_name: "node1".to_string(),
                input_map: HashMap::new(),
                choice_map,
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.message.contains("Target node")));
    }

    #[test]
    fn test_choice_map_target_finish_is_allowed() {
        let mut choice_map = HashMap::new();
        choice_map.insert("default".to_string(), "finish".to_string());

        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "test_node".to_string(),
                node_name: "node1".to_string(),
                input_map: HashMap::new(),
                choice_map,
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(
            result.is_ok(),
            "finish should be allowed as terminal target"
        );
    }

    #[test]
    fn test_input_map_start_prefix_requires_field_name() {
        let mut input_map = HashMap::new();
        input_map.insert("text".to_string(), Some("start".to_string()));

        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "test_node".to_string(),
                node_name: "node1".to_string(),
                input_map,
                choice_map: HashMap::new(),
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.message.contains("Source node 'start' not found")));
    }

    #[test]
    fn test_input_map_start_dot_field_is_allowed() {
        let mut input_map = HashMap::new();
        input_map.insert("text".to_string(), Some("start.text".to_string()));

        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "test_node".to_string(),
                node_name: "node1".to_string(),
                input_map,
                choice_map: HashMap::new(),
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_ok());
    }

    #[test]
    fn test_unregistered_node_type() {
        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "unknown_type".to_string(),
                node_name: "node1".to_string(),
                input_map: HashMap::new(),
                choice_map: HashMap::new(),
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.message.contains("not registered")));
    }

    #[test]
    fn test_while_node_missing_attrs() {
        let config = WorkflowConfig {
            start_node: "while1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "while_node".to_string(),
                node_name: "while1".to_string(),
                input_map: HashMap::new(),
                choice_map: HashMap::new(),
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.message.contains("memory_key")));
        assert!(errors.iter().any(|e| e.message.contains("condition_type")));
        assert!(errors.iter().any(|e| e.message.contains("max_iterations")));
    }

    #[test]
    fn test_valid_config() {
        let config = WorkflowConfig {
            start_node: "node1".to_string(),
            listen_at_start: None,
            input_parameters: HashMap::new(),
            nodes: vec![NodeConfigJson {
                id: 1,
                node_type: "test_node".to_string(),
                node_name: "node1".to_string(),
                input_map: HashMap::new(),
                choice_map: HashMap::new(),
                attrs: HashMap::new(),
                key_node: false,
            }],
            #[cfg(feature = "context-sync")]
            context_sync: None,
        };

        let registry = create_test_registry();
        let result = validate_config(&config, &registry);
        assert!(result.is_ok());
    }
}
