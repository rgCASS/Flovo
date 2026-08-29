/// 条件节点
///
/// 根据输入条件选择不同的执行分支
use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// 条件类型
#[derive(Debug, Clone, Copy)]
pub enum ConditionType {
    /// 等于
    Equals,
    /// 不等于
    NotEquals,
    /// 大于
    GreaterThan,
    /// 小于
    LessThan,
    /// 包含
    Contains,
    /// 真值检查
    IsTrue,
    /// 假值检查
    IsFalse,
}

impl ConditionType {
    /// 从字符串解析条件类型
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "equals" | "eq" => Ok(Self::Equals),
            "not_equals" | "ne" => Ok(Self::NotEquals),
            "greater_than" | "gt" => Ok(Self::GreaterThan),
            "less_than" | "lt" => Ok(Self::LessThan),
            "contains" => Ok(Self::Contains),
            "is_true" => Ok(Self::IsTrue),
            "is_false" => Ok(Self::IsFalse),
            _ => Err(WorkflowError::ConfigError(format!(
                "Unknown condition type: {}",
                s
            ))),
        }
    }
}

/// 条件节点
///
/// 根据条件判断结果选择 true_choice 或 false_choice 分支
pub struct ConditionNode {
    name: String,
    config: NodeConfig,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
}

impl ConditionNode {
    /// 创建新的条件节点
    ///
    /// # 参数
    /// - config: 节点配置
    /// - _ctx: 节点上下文
    ///
    /// # 配置属性（attrs）
    /// - condition_type: 条件类型（equals, not_equals, greater_than, less_than, contains, is_true, is_false）
    /// - compare_value: 比较值（用于 equals, not_equals, greater_than, less_than, contains）
    /// - field_path: JSON 字段路径（可选，用于从对象中提取值）
    ///
    /// # 选择项（choices）
    /// - true_choice: 条件为真时的分支
    /// - false_choice: 条件为假时的分支
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        // 定义输入参数
        let mut input_params = HashMap::new();
        input_params.insert("input".to_string(), ParameterType::Step);

        // 定义输出参数：传递输入和判断结果
        let mut output_params = HashMap::new();
        output_params.insert("input".to_string(), ParameterType::Step);
        output_params.insert("result".to_string(), ParameterType::Step);

        // 定义选择项
        let choices = vec!["true_choice".to_string(), "false_choice".to_string()];

        Ok(Self {
            name: config.node_name.clone(),
            config,
            input_params,
            output_params,
            choices,
        })
    }

    /// 从输入中提取值
    fn extract_value(&self, input: &Value) -> Result<Value> {
        if let Some(field_path) = self.config.attrs.get("field_path").and_then(|v| v.as_str()) {
            // 支持简单的点号路径，如 "data.status"
            let parts: Vec<&str> = field_path.split('.').collect();
            let mut current = input;

            for part in parts {
                current = current.get(part).ok_or_else(|| {
                    WorkflowError::ConfigError(format!("Field '{}' not found in path", part))
                })?;
            }

            Ok(current.clone())
        } else {
            Ok(input.clone())
        }
    }

    /// 评估条件
    fn evaluate_condition(&self, value: &Value) -> Result<bool> {
        let condition_type_str = self
            .config
            .attrs
            .get("condition_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WorkflowError::ConfigError("Missing 'condition_type' attribute".to_string())
            })?;

        let condition_type = ConditionType::from_str(condition_type_str)?;

        match condition_type {
            ConditionType::Equals => {
                let compare_value = self.config.attrs.get("compare_value").ok_or_else(|| {
                    WorkflowError::ConfigError("Missing 'compare_value' attribute".to_string())
                })?;
                Ok(value == compare_value)
            }

            ConditionType::NotEquals => {
                let compare_value = self.config.attrs.get("compare_value").ok_or_else(|| {
                    WorkflowError::ConfigError("Missing 'compare_value' attribute".to_string())
                })?;
                Ok(value != compare_value)
            }

            ConditionType::GreaterThan => {
                let compare_value = self
                    .config
                    .attrs
                    .get("compare_value")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Missing or invalid 'compare_value' for numeric comparison".to_string(),
                        )
                    })?;

                let num = value.as_f64().ok_or_else(|| {
                    WorkflowError::ConfigError("Input value is not a number".to_string())
                })?;

                Ok(num > compare_value)
            }

            ConditionType::LessThan => {
                let compare_value = self
                    .config
                    .attrs
                    .get("compare_value")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Missing or invalid 'compare_value' for numeric comparison".to_string(),
                        )
                    })?;

                let num = value.as_f64().ok_or_else(|| {
                    WorkflowError::ConfigError("Input value is not a number".to_string())
                })?;

                Ok(num < compare_value)
            }

            ConditionType::Contains => {
                let compare_value = self
                    .config
                    .attrs
                    .get("compare_value")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Missing or invalid 'compare_value' for contains check".to_string(),
                        )
                    })?;

                if let Value::String(s) = value {
                    Ok(s.contains(compare_value))
                } else if let Value::Array(arr) = value {
                    Ok(arr
                        .iter()
                        .any(|v| v.as_str().map(|s| s == compare_value).unwrap_or(false)))
                } else {
                    Err(WorkflowError::ConfigError(
                        "Input must be string or array for contains check".to_string(),
                    ))
                }
            }

            ConditionType::IsTrue => Ok(value.as_bool().unwrap_or(false)),

            ConditionType::IsFalse => Ok(!value.as_bool().unwrap_or(true)),
        }
    }
}

#[async_trait]
impl BaseNode for ConditionNode {
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

        // 提取要比较的值
        let value = self.extract_value(&input)?;

        // 评估条件
        let condition_result = self.evaluate_condition(&value)?;

        // 设置输出
        NodeHelper::set_output(&self.name, "input", input, ctx).await?;
        NodeHelper::set_output(&self.name, "result", Value::Bool(condition_result), ctx).await?;

        // 根据条件选择分支
        let choice = if condition_result {
            "true_choice"
        } else {
            "false_choice"
        };

        NodeHelper::set_choice(&self.name, choice, &self.config.choice_map, ctx).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_condition_type_parsing() {
        assert!(matches!(
            ConditionType::from_str("equals").unwrap(),
            ConditionType::Equals
        ));
        assert!(matches!(
            ConditionType::from_str("GT").unwrap(),
            ConditionType::GreaterThan
        ));
    }

    #[test]
    fn test_condition_node_creation() {
        let mut attrs = HashMap::new();
        attrs.insert("condition_type".to_string(), serde_json::json!("is_true"));

        let config = NodeConfig {
            id: 1,
            node_name: "test_condition".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = ConditionNode::new(config, ctx).unwrap();

        assert_eq!(node.name(), "test_condition");
        assert_eq!(node.choices(), &["true_choice", "false_choice"]);
    }

    #[test]
    fn test_evaluate_equals() {
        let mut attrs = HashMap::new();
        attrs.insert("condition_type".to_string(), serde_json::json!("equals"));
        attrs.insert("compare_value".to_string(), serde_json::json!("test"));

        let config = NodeConfig {
            id: 1,
            node_name: "test".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = ConditionNode::new(config, ctx).unwrap();

        assert!(node
            .evaluate_condition(&Value::String("test".to_string()))
            .unwrap());
        assert!(!node
            .evaluate_condition(&Value::String("other".to_string()))
            .unwrap());
    }

    #[test]
    fn test_evaluate_greater_than() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "condition_type".to_string(),
            serde_json::json!("greater_than"),
        );
        attrs.insert("compare_value".to_string(), serde_json::json!(10));

        let config = NodeConfig {
            id: 1,
            node_name: "test".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = ConditionNode::new(config, ctx).unwrap();

        assert!(node.evaluate_condition(&serde_json::json!(15)).unwrap());
        assert!(!node.evaluate_condition(&serde_json::json!(5)).unwrap());
    }

    #[test]
    fn test_extract_value_with_path() {
        let mut attrs = HashMap::new();
        attrs.insert("condition_type".to_string(), serde_json::json!("is_true"));
        attrs.insert("field_path".to_string(), serde_json::json!("data.status"));

        let config = NodeConfig {
            id: 1,
            node_name: "test".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = ConditionNode::new(config, ctx).unwrap();

        let input = serde_json::json!({
            "data": {
                "status": "active"
            }
        });

        let value = node.extract_value(&input).unwrap();
        assert_eq!(value, Value::String("active".to_string()));
    }
}
