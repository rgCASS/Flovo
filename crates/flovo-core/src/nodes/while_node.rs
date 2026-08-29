/// While 循环节点
///
/// 支持经典图级循环：while_node -> body -> while_node
use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
enum ConditionType {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    Contains,
    IsTrue,
    IsFalse,
}

impl ConditionType {
    fn from_str(s: &str) -> Result<Self> {
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

pub struct WhileNode {
    name: String,
    config: NodeConfig,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
    memory_key: String,
    condition_type: ConditionType,
    compare_value: Option<Value>,
    max_iterations: u64,
    init_value: Option<Value>,
    increment: Option<f64>,
}

impl WhileNode {
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        let memory_key = config
            .attrs
            .get("memory_key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WorkflowError::ConfigError("Missing 'memory_key' attribute".to_string())
            })?
            .to_string();

        let condition_type_str = config
            .attrs
            .get("condition_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WorkflowError::ConfigError("Missing 'condition_type' attribute".to_string())
            })?;
        let condition_type = ConditionType::from_str(condition_type_str)?;

        let max_iterations = config
            .attrs
            .get("max_iterations")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                WorkflowError::ConfigError(
                    "Missing or invalid 'max_iterations' attribute".to_string(),
                )
            })?;
        if max_iterations == 0 {
            return Err(WorkflowError::ConfigError(
                "'max_iterations' must be greater than 0".to_string(),
            ));
        }

        let compare_value = config.attrs.get("compare_value").cloned();
        match condition_type {
            ConditionType::Equals
            | ConditionType::NotEquals
            | ConditionType::GreaterThan
            | ConditionType::LessThan
            | ConditionType::Contains => {
                if compare_value.is_none() {
                    return Err(WorkflowError::ConfigError(
                        "Missing 'compare_value' attribute".to_string(),
                    ));
                }
            }
            ConditionType::IsTrue | ConditionType::IsFalse => {}
        }

        let increment = config.attrs.get("increment").and_then(|v| v.as_f64());
        let init_value = config.attrs.get("init_value").cloned();

        let mut input_params = HashMap::new();
        input_params.insert("seed".to_string(), ParameterType::Step);

        let mut output_params = HashMap::new();
        output_params.insert("current".to_string(), ParameterType::Step);
        output_params.insert("iteration".to_string(), ParameterType::Step);
        output_params.insert("condition".to_string(), ParameterType::Step);

        Ok(Self {
            name: config.node_name.clone(),
            config,
            input_params,
            output_params,
            choices: vec!["continue".to_string(), "exit".to_string()],
            memory_key,
            condition_type,
            compare_value,
            max_iterations,
            init_value,
            increment,
        })
    }

    fn iteration_key(&self) -> String {
        format!("{}.__iteration", self.memory_key)
    }

    fn evaluate_condition(&self, value: &Value) -> Result<bool> {
        match self.condition_type {
            ConditionType::Equals => Ok(value == self.compare_value.as_ref().unwrap()),
            ConditionType::NotEquals => Ok(value != self.compare_value.as_ref().unwrap()),
            ConditionType::GreaterThan => {
                let compare = self
                    .compare_value
                    .as_ref()
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Invalid 'compare_value' for greater_than".to_string(),
                        )
                    })?;
                let num = value.as_f64().ok_or_else(|| {
                    WorkflowError::ConfigError("Memory value is not a number".to_string())
                })?;
                Ok(num > compare)
            }
            ConditionType::LessThan => {
                let compare = self
                    .compare_value
                    .as_ref()
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Invalid 'compare_value' for less_than".to_string(),
                        )
                    })?;
                let num = value.as_f64().ok_or_else(|| {
                    WorkflowError::ConfigError("Memory value is not a number".to_string())
                })?;
                Ok(num < compare)
            }
            ConditionType::Contains => {
                let compare = self
                    .compare_value
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Invalid 'compare_value' for contains".to_string(),
                        )
                    })?;
                if let Value::String(s) = value {
                    Ok(s.contains(compare))
                } else if let Value::Array(arr) = value {
                    Ok(arr.iter().any(|v| v.as_str() == Some(compare)))
                } else {
                    Err(WorkflowError::ConfigError(
                        "Memory value must be string or array for contains".to_string(),
                    ))
                }
            }
            ConditionType::IsTrue => Ok(value.as_bool().unwrap_or(false)),
            ConditionType::IsFalse => Ok(!value.as_bool().unwrap_or(true)),
        }
    }

    async fn get_or_init_current(&self, ctx: &NodeContext) -> Result<Value> {
        if let Some(value) = NodeHelper::get_memory(&self.memory_key, ctx)? {
            return Ok(value);
        }

        let seed = NodeHelper::get_input("seed", &self.config.input_map, ctx).await?;
        let initial = if let Some(seed_value) = seed {
            seed_value
        } else if let Some(init_value) = self.init_value.clone() {
            init_value
        } else {
            Value::Null
        };

        NodeHelper::set_memory(&self.memory_key, initial.clone(), ctx)?;
        Ok(initial)
    }

    fn get_iteration(&self, ctx: &NodeContext) -> Result<u64> {
        let key = self.iteration_key();
        let raw = NodeHelper::get_memory(&key, ctx)?;
        match raw {
            Some(Value::Number(n)) => n
                .as_u64()
                .ok_or_else(|| WorkflowError::ConfigError("Invalid iteration value".to_string())),
            Some(_) => Err(WorkflowError::ConfigError(
                "Iteration value in memory must be an unsigned integer".to_string(),
            )),
            None => Ok(0),
        }
    }

    fn set_iteration(&self, iteration: u64, ctx: &NodeContext) -> Result<()> {
        NodeHelper::set_memory(&self.iteration_key(), Value::from(iteration), ctx)
    }

    fn increment_current(&self, current: &Value, ctx: &NodeContext) -> Result<()> {
        if let Some(increment) = self.increment {
            let current_num = current.as_f64().ok_or_else(|| {
                WorkflowError::ConfigError(
                    "Memory value must be numeric when 'increment' is configured".to_string(),
                )
            })?;
            let next = current_num + increment;
            NodeHelper::set_memory(&self.memory_key, Value::from(next), ctx)?;
        }
        Ok(())
    }
}

#[async_trait]
impl BaseNode for WhileNode {
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
        NodeHelper::wait_for_event(&self.name, ctx).await?;

        let current = self.get_or_init_current(ctx).await?;
        let iteration = self.get_iteration(ctx)?;
        if iteration >= self.max_iterations {
            return Err(WorkflowError::Other(format!(
                "while node '{}' exceeded max_iterations={}",
                self.name, self.max_iterations
            )));
        }

        let condition = self.evaluate_condition(&current)?;

        NodeHelper::set_output(&self.name, "current", current.clone(), ctx).await?;
        NodeHelper::set_output(&self.name, "iteration", Value::from(iteration), ctx).await?;
        NodeHelper::set_output(&self.name, "condition", Value::Bool(condition), ctx).await?;

        if condition {
            self.set_iteration(iteration + 1, ctx)?;
            self.increment_current(&current, ctx)?;
            NodeHelper::set_choice_with_policy(
                &self.name,
                "continue",
                &self.config.choice_map,
                ctx,
                false,
            )
            .await?;
        } else {
            NodeHelper::set_choice_with_policy(
                &self.name,
                "exit",
                &self.config.choice_map,
                ctx,
                false,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> NodeConfig {
        let mut attrs = HashMap::new();
        attrs.insert("memory_key".to_string(), serde_json::json!("counter"));
        attrs.insert("condition_type".to_string(), serde_json::json!("less_than"));
        attrs.insert("compare_value".to_string(), serde_json::json!(3));
        attrs.insert("max_iterations".to_string(), serde_json::json!(10));
        attrs.insert("init_value".to_string(), serde_json::json!(0));
        attrs.insert("increment".to_string(), serde_json::json!(1));

        NodeConfig {
            id: 1,
            node_name: "while_check".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        }
    }

    #[test]
    fn test_while_node_creation() {
        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = WhileNode::new(base_config(), ctx).unwrap();
        assert_eq!(node.name(), "while_check");
        assert_eq!(node.choices(), &["continue", "exit"]);
        assert!(node.is_reentrant());
    }

    #[test]
    fn test_condition_eval_less_than() {
        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = WhileNode::new(base_config(), ctx).unwrap();
        assert!(node.evaluate_condition(&serde_json::json!(2)).unwrap());
        assert!(!node.evaluate_condition(&serde_json::json!(3)).unwrap());
    }

    #[test]
    fn test_max_iterations_must_be_positive() {
        let mut config = base_config();
        config
            .attrs
            .insert("max_iterations".to_string(), serde_json::json!(0));
        let ctx = NodeContext::new(std::sync::Weak::new());
        assert!(WhileNode::new(config, ctx).is_err());
    }
}
