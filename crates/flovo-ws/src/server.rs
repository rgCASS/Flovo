//! 通用 WebSocket 工作流服务器。

use crate::llm::OpenaiCompatLlm;
use flovo_core::config::WorkflowConfig;
#[cfg(feature = "context-sync")]
use flovo_core::context_sync::{ContextOps, ContextSyncManager};
use flovo_core::nodes::{register_builtin_nodes, OutboundSender};
use flovo_core::{LlmApi, Result, WorkFlow, WorkflowBuilder, WorkflowError};
use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;

static CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
const LOG_VALUE_LIMIT: usize = 512;

/// 通用二进制资源。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WsBinaryResource {
    #[serde(rename = "type")]
    pub resource_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

/// 图片资源。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WsImageResource {
    #[serde(rename = "type")]
    pub resource_type: String,
    pub url: String,
}

/// 视频资源。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WsVideoResource {
    #[serde(rename = "type")]
    pub resource_type: String,
    pub url: String,
}

/// 结构化文本块。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WsTextBlock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    pub text: String,
}

/// WebSocket 协议信封。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsEnvelope {
    #[serde(rename = "type")]
    pub r#type: String,
    pub workflow: String,
    pub cmd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<u64>,
    #[serde(default)]
    pub info: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<Value>,
}

impl WsEnvelope {
    /// 创建服务类信封。
    pub fn service(
        workflow: impl Into<String>,
        cmd: impl Into<String>,
        message_id: Option<u64>,
        info: Value,
    ) -> Self {
        Self {
            r#type: "service".to_string(),
            workflow: workflow.into(),
            cmd: cmd.into(),
            message_id,
            info,
            resource: None,
        }
    }
}

/// 写任务可处理的消息。
#[derive(Debug, Clone)]
pub enum WsOutboundMessage {
    Envelope(WsEnvelope),
    Pong(Vec<u8>),
    Close,
}

/// 供传输层内部使用的写通道。
pub type WsWriterSender = mpsc::UnboundedSender<WsOutboundMessage>;

/// WebSocket 服务配置。
#[derive(Debug, Clone)]
pub struct WsServerConfig {
    pub max_concurrent_connections: usize,
}

impl Default for WsServerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_connections: 100,
        }
    }
}

/// 多连接 WebSocket 工作流服务器。
pub struct WsServer {
    listen_addr: String,
    workflow_configs: Arc<HashMap<String, WorkflowConfig>>,
    config: WsServerConfig,
    connection_slots: Arc<Semaphore>,
    active_connection_count: Arc<AtomicUsize>,
    active_workflows: Arc<Mutex<HashMap<u64, Arc<WorkFlow>>>>,
    #[cfg(feature = "context-sync")]
    context_client: Option<Arc<dyn ContextOps>>,
}

impl WsServer {
    /// 使用默认连接配置创建服务器。
    pub fn new(
        listen_addr: impl Into<String>,
        configs: HashMap<String, WorkflowConfig>,
    ) -> Result<Self> {
        Self::new_with_config(listen_addr, configs, WsServerConfig::default())
    }

    /// 使用显式连接配置创建服务器。
    pub fn new_with_config(
        listen_addr: impl Into<String>,
        configs: HashMap<String, WorkflowConfig>,
        config: WsServerConfig,
    ) -> Result<Self> {
        if config.max_concurrent_connections == 0 {
            return Err(WorkflowError::ConfigError(
                "max_concurrent_connections must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            listen_addr: listen_addr.into(),
            workflow_configs: Arc::new(configs),
            connection_slots: Arc::new(Semaphore::new(config.max_concurrent_connections)),
            active_connection_count: Arc::new(AtomicUsize::new(0)),
            active_workflows: Arc::new(Mutex::new(HashMap::new())),
            config,
            #[cfg(feature = "context-sync")]
            context_client: None,
        })
    }

    /// 注入外部上下文客户端。
    #[cfg(feature = "context-sync")]
    pub fn with_context_client(mut self, client: Arc<dyn ContextOps>) -> Self {
        self.context_client = Some(client);
        self
    }

    /// 返回当前活跃连接数。
    pub fn active_connection_count(&self) -> usize {
        self.active_connection_count.load(Ordering::Relaxed)
    }

    /// 绑定监听地址并持续接收连接。
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.listen_addr)
            .await
            .map_err(|error| {
                WorkflowError::Other(format!("failed to bind websocket server: {error}"))
            })?;
        tracing::info!(address = %self.listen_addr, "websocket server started");

        loop {
            let (stream, peer) = listener.accept().await.map_err(|error| {
                WorkflowError::Other(format!("failed to accept websocket connection: {error}"))
            })?;
            let permit = match Arc::clone(&self.connection_slots).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let limit = self.config.max_concurrent_connections;
                    tokio::spawn(Self::reject_over_limit(stream, limit));
                    continue;
                }
            };
            let id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
            let configs = Arc::clone(&self.workflow_configs);
            let count = Arc::clone(&self.active_connection_count);
            let workflows = Arc::clone(&self.active_workflows);
            #[cfg(feature = "context-sync")]
            let context_client = self.context_client.clone();

            tokio::spawn(async move {
                let _permit = permit;
                #[cfg(feature = "context-sync")]
                let result = Self::serve_connection(
                    stream,
                    peer,
                    id,
                    configs,
                    count,
                    workflows,
                    context_client,
                )
                .await;
                #[cfg(not(feature = "context-sync"))]
                let result =
                    Self::serve_connection(stream, peer, id, configs, count, workflows).await;
                if let Err(error) = result {
                    tracing::warn!(connection_id = id, %peer, %error, "websocket connection closed with error");
                }
            });
        }
    }

    async fn reject_over_limit(stream: TcpStream, limit: usize) {
        if let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await {
            let envelope = WsEnvelope::service(
                "connection",
                "connect_rejected",
                None,
                json!({"reason": "connection limit reached", "limit": limit}),
            );
            let _ = socket
                .send(Message::Text(envelope_to_text(&envelope)))
                .await;
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Policy,
                    reason: "connection limit reached".into(),
                })))
                .await;
        }
    }

    #[cfg(feature = "context-sync")]
    async fn serve_connection(
        stream: TcpStream,
        peer: SocketAddr,
        id: u64,
        configs: Arc<HashMap<String, WorkflowConfig>>,
        count: Arc<AtomicUsize>,
        workflows: Arc<Mutex<HashMap<u64, Arc<WorkFlow>>>>,
        context_client: Option<Arc<dyn ContextOps>>,
    ) -> Result<()> {
        Self::serve_connection_inner(stream, peer, id, configs, count, workflows, context_client)
            .await
    }

    #[cfg(not(feature = "context-sync"))]
    async fn serve_connection(
        stream: TcpStream,
        peer: SocketAddr,
        id: u64,
        configs: Arc<HashMap<String, WorkflowConfig>>,
        count: Arc<AtomicUsize>,
        workflows: Arc<Mutex<HashMap<u64, Arc<WorkFlow>>>>,
    ) -> Result<()> {
        Self::serve_connection_inner(stream, peer, id, configs, count, workflows).await
    }

    #[cfg(feature = "context-sync")]
    async fn serve_connection_inner(
        stream: TcpStream,
        peer: SocketAddr,
        id: u64,
        configs: Arc<HashMap<String, WorkflowConfig>>,
        count: Arc<AtomicUsize>,
        workflows: Arc<Mutex<HashMap<u64, Arc<WorkFlow>>>>,
        context_client: Option<Arc<dyn ContextOps>>,
    ) -> Result<()> {
        let (socket, endpoint) = accept_with_endpoint(stream).await?;
        let workflow = Self::build_workflow(&endpoint, &configs, context_client)?;
        Self::process_connection(socket, peer, id, endpoint, workflow, count, workflows).await
    }

    #[cfg(not(feature = "context-sync"))]
    async fn serve_connection_inner(
        stream: TcpStream,
        peer: SocketAddr,
        id: u64,
        configs: Arc<HashMap<String, WorkflowConfig>>,
        count: Arc<AtomicUsize>,
        workflows: Arc<Mutex<HashMap<u64, Arc<WorkFlow>>>>,
    ) -> Result<()> {
        let (socket, endpoint) = accept_with_endpoint(stream).await?;
        let workflow = Self::build_workflow(&endpoint, &configs)?;
        Self::process_connection(socket, peer, id, endpoint, workflow, count, workflows).await
    }

    #[cfg(feature = "context-sync")]
    fn build_workflow(
        endpoint: &str,
        configs: &HashMap<String, WorkflowConfig>,
        context_client: Option<Arc<dyn ContextOps>>,
    ) -> Result<Arc<WorkFlow>> {
        let config = configs.get(endpoint).cloned().ok_or_else(|| {
            WorkflowError::ConfigError(format!("workflow endpoint not found: {endpoint}"))
        })?;
        let registry = Arc::new(flovo_core::node::NodeRegistry::new());
        register_builtin_nodes(&registry);
        let mut selected = HashMap::new();
        selected.insert(endpoint.to_string(), config.clone());
        let mut builder = WorkflowBuilder::new(registry, selected);
        if let Some(sync_config) = config.context_sync.filter(|config| config.enabled) {
            let client = context_client.ok_or_else(|| {
                WorkflowError::ConfigError(
                    "context sync is enabled but no client was injected".to_string(),
                )
            })?;
            builder =
                builder.with_context_sync(ContextSyncManager::new_with_client(client, sync_config));
        }
        let workflow = builder.build(endpoint)?;
        if let Some(llm) = OpenaiCompatLlm::from_env() {
            workflow.set_context_object("llm", Arc::new(llm) as Arc<dyn LlmApi>);
        }
        Ok(workflow)
    }

    #[cfg(not(feature = "context-sync"))]
    fn build_workflow(
        endpoint: &str,
        configs: &HashMap<String, WorkflowConfig>,
    ) -> Result<Arc<WorkFlow>> {
        let config = configs.get(endpoint).cloned().ok_or_else(|| {
            WorkflowError::ConfigError(format!("workflow endpoint not found: {endpoint}"))
        })?;
        let registry = Arc::new(flovo_core::node::NodeRegistry::new());
        register_builtin_nodes(&registry);
        let mut selected = HashMap::new();
        selected.insert(endpoint.to_string(), config);
        let workflow = WorkflowBuilder::new(registry, selected).build(endpoint)?;
        if let Some(llm) = OpenaiCompatLlm::from_env() {
            workflow.set_context_object("llm", Arc::new(llm) as Arc<dyn LlmApi>);
        }
        Ok(workflow)
    }

    async fn process_connection(
        socket: tokio_tungstenite::WebSocketStream<TcpStream>,
        peer: SocketAddr,
        id: u64,
        endpoint: String,
        workflow: Arc<WorkFlow>,
        count: Arc<AtomicUsize>,
        workflows: Arc<Mutex<HashMap<u64, Arc<WorkFlow>>>>,
    ) -> Result<()> {
        count.fetch_add(1, Ordering::Relaxed);
        lock_workflows(&workflows).insert(id, Arc::clone(&workflow));
        tracing::info!(connection_id = id, %peer, endpoint = %endpoint, "websocket connected");

        let result = Self::run_socket(socket, &endpoint, Arc::clone(&workflow)).await;
        let _ = workflow.cancel_all_inactive_nodes().await;
        lock_workflows(&workflows).remove(&id);
        count.fetch_sub(1, Ordering::Relaxed);
        tracing::info!(connection_id = id, %peer, endpoint = %endpoint, "websocket disconnected");
        result
    }

    async fn run_socket(
        socket: tokio_tungstenite::WebSocketStream<TcpStream>,
        endpoint: &str,
        workflow: Arc<WorkFlow>,
    ) -> Result<()> {
        let (mut writer, mut reader) = socket.split();
        writer
            .send(Message::Text(envelope_to_text(&WsEnvelope::service(
                endpoint,
                "connect_ok",
                Some(0),
                json!({"accepted": true}),
            ))))
            .await
            .map_err(ws_error)?;

        let init = next_text(&mut reader).await?;
        let init: WsEnvelope = serde_json::from_str(&init).map_err(json_error)?;
        if init.workflow != endpoint || !matches!(init.cmd.as_str(), "init" | "init_report") {
            return Err(WorkflowError::Other(
                "invalid websocket initialization message".to_string(),
            ));
        }
        writer
            .send(Message::Text(envelope_to_text(&WsEnvelope::service(
                endpoint,
                "init_ok",
                init.message_id,
                json!({"accepted": true}),
            ))))
            .await
            .map_err(ws_error)?;

        let (outbound_tx, mut outbound_rx): (OutboundSender, _) = mpsc::unbounded_channel();
        workflow.set_context_object("outbound_sender", outbound_tx);
        let mut workflow_task = tokio::spawn({
            let workflow = Arc::clone(&workflow);
            async move { workflow.run_all().await }
        });

        loop {
            tokio::select! {
                outbound = outbound_rx.recv() => {
                    let Some(value) = outbound else { break; };
                    let envelope = WsEnvelope::service(endpoint, "output", None, value);
                    writer.send(Message::Text(envelope_to_text(&envelope))).await.map_err(ws_error)?;
                }
                inbound = reader.next() => {
                    match inbound {
                        Some(Ok(Message::Text(text))) => {
                            let envelope: WsEnvelope = serde_json::from_str(&text).map_err(json_error)?;
                            tracing::debug!(payload = %truncate(&text), "websocket message received");
                            if envelope.r#type == "service" && envelope.cmd == "send_input" {
                                if let Some(value) = envelope.info.get("user_id") {
                                    workflow.set_context("user_id", value.clone());
                                }
                                if let Some(value) = envelope.info.get("session_id") {
                                    workflow.set_context("session_id", value.clone());
                                }
                                // 将 send_input 携带的上下文对象合并进 workflow context，
                                // 节点可用 input_map 的 "context.<field>" 语法读取。
                                if let Some(Value::Object(fields)) = envelope.info.get("context") {
                                    for (key, value) in fields {
                                        workflow.set_context(key, value.clone());
                                    }
                                }
                                workflow.add_message(envelope.info).await?;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => writer.send(Message::Pong(payload)).await.map_err(ws_error)?,
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(ws_error(error)),
                    }
                }
                finished = &mut workflow_task => {
                    finished.map_err(|error| WorkflowError::Other(format!("workflow task failed: {error}")))??;
                    let envelope = WsEnvelope::service(endpoint, "workflow_finished", None, json!({}));
                    writer.send(Message::Text(envelope_to_text(&envelope))).await.map_err(ws_error)?;
                    break;
                }
            }
        }
        Ok(())
    }
}

async fn accept_with_endpoint(
    stream: TcpStream,
) -> Result<(tokio_tungstenite::WebSocketStream<TcpStream>, String)> {
    let endpoint = Arc::new(Mutex::new(String::new()));
    let endpoint_ref = Arc::clone(&endpoint);
    let socket = tokio_tungstenite::accept_hdr_async(
        stream,
        move |request: &Request, response: Response| {
            *endpoint_ref
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                request.uri().path().trim_matches('/').to_string();
            Ok(response)
        },
    )
    .await
    .map_err(ws_error)?;
    let endpoint = endpoint
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if endpoint.is_empty() {
        return Err(WorkflowError::ConfigError(
            "websocket URL must include a workflow endpoint".to_string(),
        ));
    }
    Ok((socket, endpoint))
}

async fn next_text<S>(reader: &mut S) -> Result<String>
where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(frame) = reader.next().await {
        match frame.map_err(ws_error)? {
            Message::Text(text) => return Ok(text.to_string()),
            Message::Close(_) => break,
            _ => {}
        }
    }
    Err(WorkflowError::Other(
        "websocket closed before initialization".to_string(),
    ))
}

fn lock_workflows(
    workflows: &Mutex<HashMap<u64, Arc<WorkFlow>>>,
) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<WorkFlow>>> {
    workflows
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn envelope_to_text(envelope: &WsEnvelope) -> String {
    serde_json::to_string(envelope).expect("WsEnvelope serialization is infallible")
}

fn truncate(value: &str) -> String {
    if value.chars().count() <= LOG_VALUE_LIMIT {
        return value.to_string();
    }
    value.chars().take(LOG_VALUE_LIMIT).collect::<String>() + "..."
}

fn ws_error(error: tokio_tungstenite::tungstenite::Error) -> WorkflowError {
    WorkflowError::Other(format!("websocket error: {error}"))
}

fn json_error(error: serde_json::Error) -> WorkflowError {
    WorkflowError::Other(format!("invalid websocket payload: {error}"))
}
