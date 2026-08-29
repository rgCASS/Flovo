//! 通用数据处理流水线：接收、校验、转换、分支和输出。

use flovo_core::config::{NodeConfigJson, WorkflowConfig};
use flovo_core::node::NodeRegistry;
use flovo_core::nodes::register_builtin_nodes;
use flovo_core::{Result, WorkflowBuilder};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn node(
    id: u32,
    node_type: &str,
    name: &str,
    input_map: &[(&str, Option<&str>)],
    choices: &[(&str, &str)],
    attrs: Value,
) -> NodeConfigJson {
    NodeConfigJson {
        id,
        node_type: node_type.to_string(),
        node_name: name.to_string(),
        input_map: input_map
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.map(str::to_string)))
            .collect(),
        choice_map: choices
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        attrs: attrs
            .as_object()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect(),
        key_node: false,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let registry = Arc::new(NodeRegistry::new());
    register_builtin_nodes(&registry);

    let config = WorkflowConfig {
        start_node: "receive".to_string(),
        listen_at_start: None,
        input_parameters: HashMap::new(),
        nodes: vec![
            node(
                1,
                "send_recv_node",
                "receive",
                &[],
                &[("default", "validate")],
                json!({"mode": "recv"}),
            ),
            node(
                2,
                "schema_validator",
                "validate",
                &[("input", Some("receive.output"))],
                &[("valid", "normalize"), ("invalid", "invalid_output")],
                json!({
                    "schema": {
                        "fields": {
                            "record_id": {"type": "string", "required": true},
                            "priority": {"type": "string", "required": true},
                            "value": {"type": "number", "required": true}
                        }
                    }
                }),
            ),
            node(
                3,
                "transform_node",
                "normalize",
                &[("input", Some("validate.output"))],
                &[("default", "route")],
                json!({"transform_type": "add_field", "field_name": "processed", "field_value": true}),
            ),
            node(
                4,
                "condition_node",
                "route",
                &[("input", Some("normalize.result"))],
                &[
                    ("true_choice", "priority_output"),
                    ("false_choice", "standard_output"),
                ],
                json!({"condition_type": "equals", "field_path": "priority", "compare_value": "high"}),
            ),
            node(
                5,
                "print_node",
                "priority_output",
                &[("input", Some("route.input"))],
                &[("default", "finish")],
                json!({"prefix": "[priority]"}),
            ),
            node(
                6,
                "print_node",
                "standard_output",
                &[("input", Some("route.input"))],
                &[("default", "finish")],
                json!({"prefix": "[standard]"}),
            ),
            node(
                7,
                "print_node",
                "invalid_output",
                &[("input", Some("validate.errors"))],
                &[("default", "finish")],
                json!({"prefix": "[invalid]"}),
            ),
        ],
        #[cfg(feature = "context-sync")]
        context_sync: None,
    };

    let workflow = WorkflowBuilder::new(
        registry,
        HashMap::from([("data_pipeline".to_string(), config)]),
    )
    .build("data_pipeline")?;
    workflow
        .add_message(json!({"record_id": "record-001", "priority": "high", "value": 42}))
        .await?;
    workflow.run_all().await?;
    Ok(())
}
