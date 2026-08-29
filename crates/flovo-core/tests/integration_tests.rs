use flovo_core::config::{NodeConfigJson, WorkflowConfig};
use flovo_core::node::NodeRegistry;
use flovo_core::nodes::register_builtin_nodes;
use flovo_core::{ParameterBus, ParameterType, WorkflowBuilder};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn parameter_bus_preserves_step_and_stream_values() {
    let bus = ParameterBus::new();
    bus.init_output_parameters(
        "producer",
        &HashMap::from([
            ("step".to_string(), ParameterType::Step),
            ("stream".to_string(), ParameterType::Stream),
        ]),
    );

    bus.set_value("producer", "step", json!({"ready": true}))
        .await
        .unwrap();
    bus.set_value("producer", "stream", Value::from("first"))
        .await
        .unwrap();
    bus.set_value("producer", "stream", Value::from("second"))
        .await
        .unwrap();

    assert_eq!(
        bus.get_value("producer.step").await.unwrap(),
        json!({"ready": true})
    );
    let mut stream = bus
        .register_stream_subscriber("producer.stream", "consumer")
        .unwrap();
    assert_eq!(stream.recv().await, Some(Value::from("first")));
    assert_eq!(stream.recv().await, Some(Value::from("second")));
}

#[tokio::test]
async fn builder_runs_event_driven_receive_workflow() {
    let registry = Arc::new(NodeRegistry::new());
    register_builtin_nodes(&registry);
    let config = WorkflowConfig {
        start_node: "receive".to_string(),
        listen_at_start: None,
        input_parameters: HashMap::new(),
        nodes: vec![NodeConfigJson {
            id: 1,
            node_type: "send_recv_node".to_string(),
            node_name: "receive".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::from([("default".to_string(), "finish".to_string())]),
            attrs: HashMap::from([("mode".to_string(), Value::from("recv"))]),
            key_node: false,
        }],
        #[cfg(feature = "context-sync")]
        context_sync: None,
    };
    let workflow = WorkflowBuilder::new(
        registry,
        HashMap::from([("receive_once".to_string(), config)]),
    )
    .build("receive_once")
    .unwrap();

    workflow
        .add_message(json!({"message": "hello"}))
        .await
        .unwrap();
    workflow.run_all().await.unwrap();
    assert_eq!(
        workflow.get_result().unwrap().get("output"),
        Some(&json!({"message": "hello"}))
    );
}
