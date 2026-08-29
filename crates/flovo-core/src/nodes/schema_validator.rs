//! 接收消息的轻量字段校验器。
//!
//! 仅支持字段存在性和顶层 JSON 类型校验，不实现完整 JSON Schema 语义。

use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;

/// 将轻量字段规则接入工作流的校验节点。
pub struct SchemaValidatorNode {
    name: String,
    config: NodeConfig,
    schema: ReceiveSchema,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
}

impl SchemaValidatorNode {
    /// 从 `attrs.schema` 创建校验节点。
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        let schema = config.attrs.get("schema").cloned().ok_or_else(|| {
            WorkflowError::ConfigError("schema_validator requires attrs.schema".to_string())
        })?;
        let schema = serde_json::from_value(schema).map_err(|error| {
            WorkflowError::ConfigError(format!("invalid schema_validator schema: {error}"))
        })?;
        let mut input_params = HashMap::new();
        input_params.insert("input".to_string(), ParameterType::Step);
        let mut output_params = HashMap::new();
        output_params.insert("output".to_string(), ParameterType::Step);
        output_params.insert("errors".to_string(), ParameterType::Step);
        Ok(Self {
            name: config.node_name.clone(),
            config,
            schema,
            input_params,
            output_params,
            choices: vec!["valid".to_string(), "invalid".to_string()],
        })
    }
}

#[async_trait]
impl BaseNode for SchemaValidatorNode {
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
        let input = NodeHelper::get_input("input", &self.config.input_map, ctx)
            .await?
            .unwrap_or(Value::Null);
        match validate(&input, &self.schema) {
            Ok(()) => {
                NodeHelper::set_output(&self.name, "output", input, ctx).await?;
                NodeHelper::set_output(&self.name, "errors", Value::Array(Vec::new()), ctx).await?;
                NodeHelper::set_choice(&self.name, "valid", &self.config.choice_map, ctx).await?;
            }
            Err(errors) => {
                let errors = errors.into_iter().map(Value::String).collect();
                NodeHelper::set_output(&self.name, "output", input, ctx).await?;
                NodeHelper::set_output(&self.name, "errors", Value::Array(errors), ctx).await?;
                NodeHelper::set_choice(&self.name, "invalid", &self.config.choice_map, ctx).await?;
            }
        }
        Ok(())
    }
}

/// 接收消息字段规则集合，键为点分字段路径。
#[derive(Debug, Clone, Deserialize)]
pub struct ReceiveSchema {
    pub fields: HashMap<String, FieldRule>,
}

/// 单个字段的存在性和类型规则。
#[derive(Debug, Clone, Deserialize)]
pub struct FieldRule {
    /// 字段存在时需要满足的 JSON 类型。
    #[serde(rename = "type")]
    pub field_type: Option<FieldType>,
    /// 是否要求字段必须存在。
    #[serde(default)]
    pub required: bool,
}

/// 支持校验的 JSON 顶层类型。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Object,
    Array,
    String,
    Number,
    Boolean,
}

impl FieldType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Array => "array",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
        }
    }

    fn matches(self, value: &Value) -> bool {
        match self {
            Self::Object => value.is_object(),
            Self::Array => value.is_array(),
            Self::String => value.is_string(),
            Self::Number => value.is_number(),
            Self::Boolean => value.is_boolean(),
        }
    }
}

/// 校验 JSON 值是否符合接收规则。
///
/// # 参数
/// - `value`：实际写入节点接收输出的 JSON 值。
/// - `schema`：以点分路径描述的字段规则集合。
///
/// # 返回值
/// - 全部规则通过时返回 `Ok(())`。
/// - 存在缺失字段或类型错误时返回全部错误，不在首个错误处短路。
pub fn validate(value: &Value, schema: &ReceiveSchema) -> std::result::Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let mut paths: Vec<&String> = schema.fields.keys().collect();
    paths.sort_unstable();

    for path in paths {
        let rule = &schema.fields[path];
        let field = resolve_path(value, path);

        match field {
            None if rule.required => {
                errors.push(format!("missing required field: {path}"));
            }
            Some(field) => {
                if let Some(expected) = rule.field_type {
                    if !expected.matches(field) {
                        errors.push(format!(
                            "field {path}: expected {}, got {}",
                            expected.as_str(),
                            value_type_name(field)
                        ));
                    }
                }
            }
            None => {}
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// 按点分路径逐级读取对象字段；任一级不存在或不是对象时均视为缺失。
fn resolve_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(value, |current, segment| current.as_object()?.get(segment))
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema(value: Value) -> ReceiveSchema {
        serde_json::from_value(value).expect("parse receive schema")
    }

    #[test]
    fn valid_nested_message_passes() {
        let rules = schema(json!({
            "fields": {
                "info": {"type": "object", "required": true},
                "info.name": {"type": "string", "required": true},
                "info.metrics": {"type": "array", "required": true},
                "accepted": {"type": "boolean", "required": true}
            }
        }));

        assert!(validate(
            &json!({"info": {"name": "squat", "metrics": [1, 2]}, "accepted": true}),
            &rules
        )
        .is_ok());
    }

    #[test]
    fn missing_required_field_reports_full_path() {
        let rules = schema(json!({
            "fields": {"info.name": {"required": true}}
        }));

        assert_eq!(
            validate(&json!({"info": {}}), &rules),
            Err(vec!["missing required field: info.name".to_string()])
        );
    }

    #[test]
    fn type_mismatch_reports_expected_and_actual_types() {
        let rules = schema(json!({
            "fields": {"info.items": {"type": "array"}}
        }));

        assert_eq!(
            validate(&json!({"info": {"items": "invalid"}}), &rules),
            Err(vec![
                "field info.items: expected array, got string".to_string()
            ])
        );
    }

    #[test]
    fn multiple_missing_fields_are_aggregated() {
        let rules = schema(json!({
            "fields": {
                "info.count": {"required": true},
                "info.name": {"required": true}
            }
        }));

        assert_eq!(
            validate(&json!({"info": {}}), &rules),
            Err(vec![
                "missing required field: info.count".to_string(),
                "missing required field: info.name".to_string()
            ])
        );
    }

    #[test]
    fn resolves_three_level_dotted_path() {
        let rules = schema(json!({
            "fields": {"a.b.c": {"type": "number", "required": true}}
        }));

        assert!(validate(&json!({"a": {"b": {"c": 3}}}), &rules).is_ok());
    }

    #[test]
    fn optional_field_without_type_is_not_checked() {
        let rules = schema(json!({
            "fields": {"unused": {"required": false}}
        }));

        assert!(validate(&json!({}), &rules).is_ok());
        assert!(validate(&json!({"unused": null}), &rules).is_ok());
    }
}
