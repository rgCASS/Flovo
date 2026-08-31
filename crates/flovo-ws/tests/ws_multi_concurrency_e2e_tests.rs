use flovo_core::config::{NodeConfigJson, WorkflowConfig};
use flovo_ws::{WsEnvelope, WsServer, WsServerConfig};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::Message;

fn workflow_configs() -> HashMap<String, WorkflowConfig> {
    let echo = WorkflowConfig {
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
    let context_echo = WorkflowConfig {
        start_node: "receive".to_string(),
        listen_at_start: None,
        input_parameters: HashMap::new(),
        nodes: vec![
            NodeConfigJson {
                id: 1,
                node_type: "send_recv_node".to_string(),
                node_name: "receive".to_string(),
                input_map: HashMap::new(),
                choice_map: HashMap::from([("default".to_string(), "read_context".to_string())]),
                attrs: HashMap::from([("mode".to_string(), Value::from("recv"))]),
                key_node: false,
            },
            NodeConfigJson {
                id: 2,
                node_type: "transform_node".to_string(),
                node_name: "read_context".to_string(),
                input_map: HashMap::from([(
                    "input".to_string(),
                    Some("context.user_name".to_string()),
                )]),
                choice_map: HashMap::from([("default".to_string(), "send_result".to_string())]),
                attrs: HashMap::from([("transform_type".to_string(), Value::from("to_upper"))]),
                key_node: false,
            },
            NodeConfigJson {
                id: 3,
                node_type: "send_recv_node".to_string(),
                node_name: "send_result".to_string(),
                input_map: HashMap::from([(
                    "input".to_string(),
                    Some("read_context.result".to_string()),
                )]),
                choice_map: HashMap::from([("default".to_string(), "finish".to_string())]),
                attrs: HashMap::from([("mode".to_string(), Value::from("send"))]),
                key_node: false,
            },
        ],
        #[cfg(feature = "context-sync")]
        context_sync: None,
    };
    HashMap::from([
        ("echo".to_string(), echo),
        ("context_echo".to_string(), context_echo),
    ])
}

async fn available_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    address
}

async fn read_envelope<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> WsEnvelope
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let frame = timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

async fn connect_client(
    url: &str,
    workflow: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let mut socket = loop {
        match tokio_tungstenite::connect_async(url).await {
            Ok((socket, _)) => break socket,
            Err(_) => sleep(Duration::from_millis(20)).await,
        }
    };
    assert_eq!(read_envelope(&mut socket).await.cmd, "connect_ok");
    socket
        .send(Message::Text(
            json!({
                "type": "service",
                "workflow": workflow,
                "cmd": "init_report",
                "message_id": 1,
                "info": {}
            })
            .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(read_envelope(&mut socket).await.cmd, "init_ok");
    socket
}

#[tokio::test]
async fn serves_two_independent_connections() {
    let address = available_address().await;
    let server = Arc::new(
        WsServer::new_with_config(
            address.clone(),
            workflow_configs(),
            WsServerConfig {
                max_concurrent_connections: 2,
            },
        )
        .unwrap(),
    );
    let server_task = tokio::spawn({
        let server = Arc::clone(&server);
        async move { server.start().await }
    });
    let url = format!("ws://{address}/echo");
    let (mut first, mut second) =
        tokio::join!(connect_client(&url, "echo"), connect_client(&url, "echo"));

    first.send(Message::Text(json!({"type": "service", "workflow": "echo", "cmd": "send_input", "info": {"value": 1}}).to_string())).await.unwrap();
    second.send(Message::Text(json!({"type": "service", "workflow": "echo", "cmd": "send_input", "info": {"value": 2}}).to_string())).await.unwrap();
    assert_eq!(read_envelope(&mut first).await.cmd, "workflow_finished");
    assert_eq!(read_envelope(&mut second).await.cmd, "workflow_finished");

    server_task.abort();
}

#[tokio::test]
async fn test_send_input_context_merged_into_workflow_context() {
    let address = available_address().await;
    let server = Arc::new(
        WsServer::new_with_config(
            address.clone(),
            workflow_configs(),
            WsServerConfig {
                max_concurrent_connections: 1,
            },
        )
        .unwrap(),
    );
    let server_task = tokio::spawn({
        let server = Arc::clone(&server);
        async move { server.start().await }
    });
    let url = format!("ws://{address}/context_echo");
    let mut socket = connect_client(&url, "context_echo").await;

    socket
        .send(Message::Text(
            json!({
                "type": "service",
                "workflow": "context_echo",
                "cmd": "send_input",
                "info": {
                    "context": {"user_name": "alice"},
                    "question": "hi"
                }
            })
            .to_string(),
        ))
        .await
        .unwrap();

    let output = read_envelope(&mut socket).await;
    assert_eq!(output.cmd, "output");
    assert_eq!(output.info, Value::from("ALICE"));
    assert_eq!(read_envelope(&mut socket).await.cmd, "workflow_finished");

    server_task.abort();
}
