/// 转换节点
///
/// 对输入数据进行转换并输出
use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;
use crate::workflow::WorkFlow;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// 转换类型
#[derive(Debug, Clone, Copy)]
pub enum TransformType {
    /// 添加字段
    AddField,
    /// 提取字段
    ExtractField,
    /// 按字段路径顺序提取并拼接文本
    Concat,
    /// 转换为大写
    ToUpper,
    /// 转换为小写
    ToLower,
    /// 包装为对象
    Wrap,
}

impl TransformType {
    /// 从字符串解析转换类型
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "add_field" => Ok(Self::AddField),
            "extract_field" => Ok(Self::ExtractField),
            "concat" => Ok(Self::Concat),
            "to_upper" => Ok(Self::ToUpper),
            "to_lower" => Ok(Self::ToLower),
            "wrap" => Ok(Self::Wrap),
            _ => Err(WorkflowError::ConfigError(format!(
                "Unknown transform type: {}",
                s
            ))),
        }
    }
}

/// 转换节点
///
/// 对输入数据执行指定的转换操作
pub struct TransformNode {
    name: String,
    config: NodeConfig,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
}

impl TransformNode {
    /// 创建新的转换节点
    ///
    /// # 参数
    /// - config: 节点配置
    /// - _ctx: 节点上下文
    ///
    /// # 配置属性（attrs）
    /// - transform_type: 转换类型（add_field, extract_field, concat, to_upper, to_lower, wrap）
    /// - field_name: 字段名称（用于 add_field 和 extract_field）
    /// - field_path: 点号字段路径（用于 extract_field，优先级高于 field_name）
    /// - fields: 字段路径数组（用于 concat；支持 `input`、`context.<field>` 和对象字段路径，缺失字段会被跳过）
    /// - separator: 字段拼接分隔符（用于 concat，默认为空字符串）
    /// - field_value: 字段值（用于 add_field）
    /// - wrap_key: 包装键名（用于 wrap）
    /// - context_key: 可选，设置后将转换结果同步写入 workflow context
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        // 定义输入参数
        let mut input_params = HashMap::new();
        input_params.insert("input".to_string(), ParameterType::Step);

        // 定义输出参数
        let mut output_params = HashMap::new();
        output_params.insert("result".to_string(), ParameterType::Step);

        // 定义选择项
        let choices = vec!["default".to_string()];

        let node = Self {
            name: config.node_name.clone(),
            config,
            input_params,
            output_params,
            choices,
        };

        tracing::info!("transform node created: node={}", node.name);

        Ok(node)
    }

    /// 执行转换
    #[allow(dead_code)]
    fn transform(&self, input: Value) -> Result<Value> {
        self.transform_with_context(input, None)
    }

    /// 执行转换，并在 concat 场景下按需读取 workflow context。
    ///
    /// `transform` 保持无上下文的纯函数入口，供现有调用方和单测继续使用；
    /// 运行时由 `run` 传入工作流引用，以支持 `input` 与 `context.<field>` 字段。
    fn transform_with_context(&self, input: Value, workflow: Option<&WorkFlow>) -> Result<Value> {
        let transform_type_str = self
            .config
            .attrs
            .get("transform_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WorkflowError::ConfigError("Missing 'transform_type' attribute".to_string())
            })?;

        tracing::info!(
            "transform node action: node={}, transform_type={}, input={:?}",
            self.name,
            transform_type_str,
            input
        );

        let transform_type = TransformType::from_str(transform_type_str)?;

        let result = match transform_type {
            TransformType::AddField => {
                let field_name = self
                    .config
                    .attrs
                    .get("field_name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError("Missing 'field_name' attribute".to_string())
                    })?;

                let field_value = self
                    .config
                    .attrs
                    .get("field_value")
                    .ok_or_else(|| {
                        WorkflowError::ConfigError("Missing 'field_value' attribute".to_string())
                    })?
                    .clone();

                if let Value::Object(mut map) = input {
                    map.insert(field_name.to_string(), field_value);
                    Ok(Value::Object(map))
                } else {
                    Err(WorkflowError::ConfigError(
                        "Input must be an object for add_field transform".to_string(),
                    ))
                }
            }

            TransformType::ExtractField => {
                let field_path = self
                    .config
                    .attrs
                    .get("field_path")
                    .or_else(|| self.config.attrs.get("field_name"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Missing 'field_path' or 'field_name' attribute".to_string(),
                        )
                    })?;

                let input = parse_json_input(input, "extract_field")?;

                extract_path(&input, field_path)
            }

            TransformType::Concat => {
                let fields = self
                    .config
                    .attrs
                    .get("fields")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        WorkflowError::ConfigError(
                            "Missing or invalid 'fields' attribute: expected an array".to_string(),
                        )
                    })?;
                let field_paths = fields
                    .iter()
                    .map(|field| {
                        field.as_str().ok_or_else(|| {
                            WorkflowError::ConfigError(
                                "Invalid 'fields' attribute: every item must be a string"
                                    .to_string(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let separator = self
                    .config
                    .attrs
                    .get("separator")
                    .map(|value| {
                        value.as_str().ok_or_else(|| {
                            WorkflowError::ConfigError(
                                "Invalid 'separator' attribute: expected a string".to_string(),
                            )
                        })
                    })
                    .transpose()?
                    .unwrap_or("");
                // `input` 必须保留原始值：字符串问题不应被当作 JSON 重新解析，
                // 对象/数组则在拼接时按 JSON 文本格式化。只有需要读取对象路径时才解析字符串输入，
                // 从而兼容旧的 JSON 字符串输入行为，同时允许 concat 直接引用普通文本。
                let parsed_input = if field_paths
                    .iter()
                    .any(|field_path| *field_path != "input" && !field_path.starts_with("context."))
                {
                    Some(parse_json_input(input.clone(), "concat")?)
                } else {
                    None
                };

                let values = field_paths
                    .iter()
                    .filter_map(|field_path| {
                        if *field_path == "input" {
                            return Some(input.clone());
                        }
                        if let Some(context_key) = field_path.strip_prefix("context.") {
                            return workflow.and_then(|workflow| workflow.get_context(context_key));
                        }
                        parsed_input
                            .as_ref()
                            .and_then(|parsed| extract_path(parsed, field_path).ok())
                    })
                    .map(|value| match value {
                        Value::String(text) => text,
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>();

                Ok(Value::String(values.join(separator)))
            }

            TransformType::ToUpper => {
                if let Value::String(s) = input {
                    Ok(Value::String(s.to_uppercase()))
                } else {
                    Err(WorkflowError::ConfigError(
                        "Input must be a string for to_upper transform".to_string(),
                    ))
                }
            }

            TransformType::ToLower => {
                if let Value::String(s) = input {
                    Ok(Value::String(s.to_lowercase()))
                } else {
                    Err(WorkflowError::ConfigError(
                        "Input must be a string for to_lower transform".to_string(),
                    ))
                }
            }

            TransformType::Wrap => {
                let wrap_key = self
                    .config
                    .attrs
                    .get("wrap_key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("data");

                Ok(serde_json::json!({
                    wrap_key: input
                }))
            }
        };

        tracing::info!(
            "transform node result: node={}, transform_type={}, output={:?}",
            self.name,
            transform_type_str,
            result
        );

        result
    }
}

#[async_trait]
impl BaseNode for TransformNode {
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
        tracing::info!("transform node wait_for_event: node={}", self.name);
        NodeHelper::wait_for_event(&self.name, ctx).await?;

        tracing::info!("transform node triggered: node={}", self.name);

        let input = NodeHelper::get_input("input", &self.config.input_map, ctx)
            .await?
            .unwrap_or(Value::Null);

        tracing::debug!(
            "transform node input: node={}, input={:?}",
            self.name,
            input
        );

        let workflow = ctx.get_workflow()?;
        let result = self.transform_with_context(input, Some(&workflow))?;

        if let Some(context_key) = self
            .config
            .attrs
            .get("context_key")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            workflow.set_context(context_key, result.clone());
        }

        // add_field 场景保留旧的 context 同步行为，保持历史配置兼容。
        if let Some(transform_type) = self
            .config
            .attrs
            .get("transform_type")
            .and_then(|v| v.as_str())
            .map(|v| v.to_lowercase())
        {
            if transform_type == "add_field" {
                if let (Some(field_name), Some(field_value)) = (
                    self.config.attrs.get("field_name").and_then(|v| v.as_str()),
                    self.config.attrs.get("field_value"),
                ) {
                    workflow.set_context(field_name, field_value.clone());
                }
            }
        }

        tracing::debug!(
            "transform node set_output: node={}, output={:?}",
            self.name,
            result
        );
        NodeHelper::set_output(&self.name, "result", result, ctx).await?;

        tracing::info!(
            "transform node set_choice: node={}, choice=default",
            self.name
        );
        NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;

        Ok(())
    }
}

/// 将字符串形式的 JSON 输入解析为值，其他 JSON 值保持不变。
///
/// `transform_type` 仅用于构造可定位具体转换类型的配置错误信息。
fn parse_json_input(input: Value, transform_type: &str) -> Result<Value> {
    if let Value::String(text) = input {
        serde_json::from_str::<Value>(&text).map_err(|error| {
            WorkflowError::ConfigError(format!(
                "Input string must be valid JSON for {transform_type} transform: {error}"
            ))
        })
    } else {
        Ok(input)
    }
}

/// 按点分路径提取 JSON 值；任一路径片段不存在时返回配置错误。
fn extract_path(input: &Value, field_path: &str) -> Result<Value> {
    let mut current = input;
    for part in field_path.split('.') {
        current = current.get(part).ok_or_else(|| {
            WorkflowError::ConfigError(format!(
                "Field '{}' not found in path '{}'",
                part, field_path
            ))
        })?;
    }

    Ok(current.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transform_node(transform_type: &str, extra_attrs: &[(&str, Value)]) -> TransformNode {
        let mut attrs = HashMap::new();
        attrs.insert(
            "transform_type".to_string(),
            Value::String(transform_type.to_string()),
        );
        attrs.extend(
            extra_attrs
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone())),
        );

        let config = NodeConfig {
            id: 1,
            node_name: "test_transform".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        TransformNode::new(config, NodeContext::new(std::sync::Weak::new())).unwrap()
    }

    #[test]
    fn test_transform_type_parsing() {
        assert!(matches!(
            TransformType::from_str("add_field").unwrap(),
            TransformType::AddField
        ));
        assert!(matches!(
            TransformType::from_str("TO_UPPER").unwrap(),
            TransformType::ToUpper
        ));
        assert!(matches!(
            TransformType::from_str("concat").unwrap(),
            TransformType::Concat
        ));
    }

    #[test]
    fn test_transform_node_creation() {
        let mut attrs = HashMap::new();
        attrs.insert("transform_type".to_string(), serde_json::json!("to_upper"));

        let config = NodeConfig {
            id: 1,
            node_name: "test_transform".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = TransformNode::new(config, ctx).unwrap();

        assert_eq!(node.name(), "test_transform");
    }

    #[test]
    fn test_transform_to_upper() {
        let mut attrs = HashMap::new();
        attrs.insert("transform_type".to_string(), serde_json::json!("to_upper"));

        let config = NodeConfig {
            id: 1,
            node_name: "test".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = TransformNode::new(config, ctx).unwrap();

        let result = node.transform(Value::String("hello".to_string())).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_transform_wrap() {
        let mut attrs = HashMap::new();
        attrs.insert("transform_type".to_string(), serde_json::json!("wrap"));
        attrs.insert("wrap_key".to_string(), serde_json::json!("payload"));

        let config = NodeConfig {
            id: 1,
            node_name: "test".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs,
            key_node: false,
        };

        let ctx = NodeContext::new(std::sync::Weak::new());
        let node = TransformNode::new(config, ctx).unwrap();

        let result = node.transform(Value::String("test".to_string())).unwrap();
        assert_eq!(result, serde_json::json!({"payload": "test"}));
    }

    #[test]
    fn test_concat_single_field() {
        let node = transform_node(
            "concat",
            &[("fields", serde_json::json!(["feedback.suggestion"]))],
        );

        let result = node
            .transform(serde_json::json!({
                "feedback": {"suggestion": "保持核心收紧"}
            }))
            .unwrap();

        assert_eq!(result, Value::String("保持核心收紧".to_string()));
    }

    #[test]
    fn test_concat_multiple_fields_with_separator() {
        let input = serde_json::json!({"first": "动作稳定", "second": "继续保持"});
        let space_node = transform_node(
            "concat",
            &[
                ("fields", serde_json::json!(["first", "second"])),
                ("separator", serde_json::json!(" ")),
            ],
        );
        let punctuation_node = transform_node(
            "concat",
            &[
                ("fields", serde_json::json!(["first", "second"])),
                ("separator", serde_json::json!("。")),
            ],
        );

        assert_eq!(
            space_node.transform(input.clone()).unwrap(),
            Value::String("动作稳定 继续保持".to_string())
        );
        assert_eq!(
            punctuation_node.transform(input).unwrap(),
            Value::String("动作稳定。继续保持".to_string())
        );
    }

    #[test]
    fn test_concat_nested_paths_and_skips_missing_fields() {
        let node = transform_node(
            "concat",
            &[
                (
                    "fields",
                    serde_json::json!([
                        "feedback.suggestion",
                        "feedback.missing",
                        "feedback.next_set_tip"
                    ]),
                ),
                ("separator", serde_json::json!(" ")),
            ],
        );

        let result = node
            .transform(serde_json::json!({
                "feedback": {
                    "suggestion": "膝盖对准脚尖",
                    "next_set_tip": "下一组降低速度"
                }
            }))
            .unwrap();

        assert_eq!(
            result,
            Value::String("膝盖对准脚尖 下一组降低速度".to_string())
        );
    }

    #[test]
    fn test_concat_all_fields_missing_returns_empty_string() {
        let node = transform_node(
            "concat",
            &[("fields", serde_json::json!(["missing", "also_missing"]))],
        );

        let result = node
            .transform(serde_json::json!({"present": true}))
            .unwrap();

        assert_eq!(result, Value::String(String::new()));
    }

    #[test]
    fn test_concat_parses_json_string_input() {
        let node = transform_node(
            "concat",
            &[
                (
                    "fields",
                    serde_json::json!(["feedback.suggestion", "feedback.next_set_tip"]),
                ),
                ("separator", serde_json::json!(" ")),
            ],
        );
        let input = serde_json::json!({
            "feedback": {
                "suggestion": "挺胸",
                "next_set_tip": "下一组保持节奏"
            }
        })
        .to_string();

        let result = node.transform(Value::String(input)).unwrap();

        assert_eq!(result, Value::String("挺胸 下一组保持节奏".to_string()));
    }

    #[test]
    fn test_concat_rejects_missing_or_non_array_fields() {
        let missing_fields = transform_node("concat", &[]);
        let non_array_fields = transform_node(
            "concat",
            &[("fields", serde_json::json!("feedback.summary"))],
        );

        assert!(matches!(
            missing_fields.transform(serde_json::json!({})),
            Err(WorkflowError::ConfigError(_))
        ));
        assert!(matches!(
            non_array_fields.transform(serde_json::json!({})),
            Err(WorkflowError::ConfigError(_))
        ));
    }

    #[test]
    fn test_concat_rejects_invalid_json_string_input() {
        let node = transform_node(
            "concat",
            &[("fields", serde_json::json!(["feedback.summary"]))],
        );

        assert!(matches!(
            node.transform(Value::String("not-json".to_string())),
            Err(WorkflowError::ConfigError(_))
        ));
    }

    #[test]
    fn test_concat_input_field_returns_original_string() {
        let node = transform_node("concat", &[("fields", serde_json::json!(["input"]))]);

        let result = node
            .transform(Value::String("原始问题".to_string()))
            .unwrap();

        assert_eq!(result, Value::String("原始问题".to_string()));
    }

    #[test]
    fn test_concat_input_and_context_fields() {
        let node = transform_node(
            "concat",
            &[
                ("fields", serde_json::json!(["input", "context.user_name"])),
                ("separator", serde_json::json!(" ")),
            ],
        );
        let workflow = crate::workflow::WorkFlow::new("concat_context".to_string());
        workflow.set_context("user_name", Value::String("alice".to_string()));

        let result = node
            .transform_with_context(Value::String("hello".to_string()), Some(&workflow))
            .unwrap();

        assert_eq!(result, Value::String("hello alice".to_string()));
    }

    #[test]
    fn test_concat_missing_context_field_is_skipped() {
        let node = transform_node(
            "concat",
            &[
                (
                    "fields",
                    serde_json::json!(["input", "context.user_name", "context.tone"]),
                ),
                ("separator", serde_json::json!(" ")),
            ],
        );
        let workflow = crate::workflow::WorkFlow::new("concat_missing_context".to_string());

        assert_eq!(
            node.transform_with_context(Value::String("hello".to_string()), Some(&workflow))
                .unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(
            node.transform_with_context(Value::String("hello".to_string()), None)
                .unwrap(),
            Value::String("hello".to_string())
        );
    }
}
