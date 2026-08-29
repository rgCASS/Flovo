#[cfg(feature = "context-sync")]
use crate::context_sync::ContextSyncManager;
/// 工作流核心模块
///
/// 实现 WorkFlow 结构体和相关功能
use crate::error::{Result, WorkflowError};
use crate::node::BaseNode;
use crate::parameter_bus::ParameterBus;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tokio::task::AbortHandle;
use tracing::Instrument;

/// 工作流状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowStatus {
    /// 初始化状态
    Init,
    /// 准备就绪
    Ready,
    /// 运行中
    Running,
    /// 已完成
    Finished,
}

/// 节点信息
struct NodeInfo {
    /// 节点实例
    node: Arc<dyn BaseNode>,
    /// 节点激活通知器
    event: Arc<Notify>,
    /// 节点是否已被激活
    activated: Arc<ParkingMutex<bool>>,
    /// 节点被激活的时间（用于耗时统计）
    activated_at: Arc<ParkingMutex<Option<Instant>>>,
    /// 节点任务中止句柄
    abort_handle: ParkingMutex<Option<AbortHandle>>,
    /// 节点是否正在执行
    running: Arc<ParkingMutex<bool>>,
    /// 节点完成次数
    completed_count: Arc<ParkingMutex<u64>>,
    /// 节点运行中被忽略的激活次数
    activation_ignored_while_running: Arc<ParkingMutex<u64>>,
}

/// 工作流
///
/// 管理节点、参数总线和工作流执行
pub struct WorkFlow {
    /// 工作流名称
    pub name: String,

    /// 参数总线
    pub parameter_bus: Arc<ParameterBus>,

    /// 节点信息映射
    nodes: RwLock<HashMap<String, NodeInfo>>,

    /// 起始节点名称
    start_node_name: RwLock<Option<String>>,

    /// 工作流状态
    status: RwLock<WorkflowStatus>,

    /// 上下文
    context: RwLock<HashMap<String, Value>>,

    /// 类型化上下文（用于存放不可序列化的宿主对象）
    context_objects: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,

    /// 记忆机制
    memory: RwLock<HashMap<String, Value>>,

    /// 外部消息队列
    msg_queue_tx: mpsc::UnboundedSender<Value>,
    msg_queue_rx: AsyncMutex<Option<mpsc::UnboundedReceiver<Value>>>,

    /// 完成节点名称
    finish_node: RwLock<Option<String>>,

    /// 上下文同步管理器。
    #[cfg(feature = "context-sync")]
    context_sync: RwLock<Option<ContextSyncManager>>,
}

impl WorkFlow {
    /// 创建新的工作流
    pub fn new(name: String) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();

        Arc::new(Self {
            name,
            parameter_bus: Arc::new(ParameterBus::new()),
            nodes: RwLock::new(HashMap::new()),
            start_node_name: RwLock::new(None),
            status: RwLock::new(WorkflowStatus::Init),
            context: RwLock::new(HashMap::new()),
            context_objects: RwLock::new(HashMap::new()),
            memory: RwLock::new(HashMap::new()),
            msg_queue_tx: tx,
            msg_queue_rx: AsyncMutex::new(Some(rx)),
            finish_node: RwLock::new(None),
            #[cfg(feature = "context-sync")]
            context_sync: RwLock::new(None),
        })
    }

    /// 注入上下文同步管理器。
    #[cfg(feature = "context-sync")]
    pub fn set_context_sync(&self, manager: ContextSyncManager) {
        *self.context_sync.write() = Some(manager);
    }

    /// 设置节点列表
    ///
    /// # 参数
    /// - nodes: 节点列表
    pub fn set_nodes(self: &Arc<Self>, nodes: Vec<Arc<dyn BaseNode>>) -> Result<()> {
        let mut node_map = self.nodes.write();

        for node in nodes {
            // 初始化输出参数
            self.parameter_bus
                .init_output_parameters(node.name(), node.output_parameters());

            // 存储节点信息
            node_map.insert(
                node.name().to_string(),
                NodeInfo {
                    node,
                    event: Arc::new(Notify::new()),
                    activated: Arc::new(ParkingMutex::new(false)),
                    activated_at: Arc::new(ParkingMutex::new(None)),
                    abort_handle: ParkingMutex::new(None),
                    running: Arc::new(ParkingMutex::new(false)),
                    completed_count: Arc::new(ParkingMutex::new(0)),
                    activation_ignored_while_running: Arc::new(ParkingMutex::new(0)),
                },
            );
        }

        // 设置状态为 Ready
        *self.status.write() = WorkflowStatus::Ready;

        Ok(())
    }

    /// 设置起始节点
    ///
    /// # 参数
    /// - node_name: 起始节点名称
    pub fn set_start_node(&self, node_name: &str) {
        *self.start_node_name.write() = Some(node_name.to_string());
    }

    /// 运行所有节点
    ///
    /// 启动所有可运行节点的异步任务，并激活起始节点
    pub async fn run_all(self: &Arc<Self>) -> Result<()> {
        let workflow_started_at = Instant::now();
        #[cfg(feature = "context-sync")]
        let (fetch_user_id, fetch_session_id) = self.context_sync_identifiers();

        // 检查状态
        {
            let status = self.status.read();
            if *status != WorkflowStatus::Ready {
                return Err(WorkflowError::InvalidState(format!(
                    "Workflow not ready, current status: {:?}",
                    *status
                )));
            }
        }

        // 工作流启动前先尝试拉取外部上下文；失败仅记录告警，不阻塞执行。
        #[cfg(feature = "context-sync")]
        {
            let context_sync_manager = { self.context_sync.read().clone() };
            if let Some(manager) = context_sync_manager {
                manager
                    .fetch_on_start(self, &fetch_user_id, &fetch_session_id)
                    .await;
            }
        }

        // 设置状态为 Running
        *self.status.write() = WorkflowStatus::Running;
        tracing::info!("workflow start: id={}", self.name);

        let mut tasks = Vec::new();

        // 为所有可运行节点启动任务
        {
            let nodes = self.nodes.read();
            for (node_name, node_info) in nodes.iter() {
                if node_info.node.runable() {
                    let workflow = Arc::clone(self);
                    let node = Arc::clone(&node_info.node);
                    let node_name = node_name.clone();

                    let task = tokio::spawn(async move { workflow.run_node(&node).await });

                    // 存储中止句柄
                    *node_info.abort_handle.lock() = Some(task.abort_handle());
                    tasks.push((node_name, task));
                }
            }
        }

        // 激活起始节点
        let start_node = self.start_node_name.read().clone();
        if let Some(start_node_name) = start_node {
            self.activate_node(&start_node_name)?;
        } else {
            return Err(WorkflowError::ConfigError("Start node not set".to_string()));
        }

        // 等待所有任务完成
        for (node_name, task) in tasks {
            match task.await {
                Ok(Ok(())) => {
                    let activation_elapsed_ms =
                        self.nodes.read().get(&node_name).and_then(|info| {
                            info.activated_at
                                .lock()
                                .as_ref()
                                .map(|started_at| started_at.elapsed().as_millis())
                        });
                    tracing::info!(
                        "node completed: workflow_id={}, node={}, activation_elapsed_ms={}",
                        self.name,
                        node_name,
                        activation_elapsed_ms.unwrap_or_default()
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!(
                        "workflow node failed: workflow_id={}, node={}, error={}",
                        self.name,
                        node_name,
                        e
                    );
                }
                Err(e) => {
                    if e.is_cancelled() {
                        tracing::debug!(
                            "node cancelled: workflow_id={}, node={}",
                            self.name,
                            node_name
                        );
                    } else {
                        tracing::error!(
                            "workflow node panicked: workflow_id={}, node={}, error={}",
                            self.name,
                            node_name,
                            e
                        );
                    }
                }
            }
        }

        // 工作流执行完成后回写外部上下文；失败需要返回错误。
        #[cfg(feature = "context-sync")]
        {
            let context_sync_manager = { self.context_sync.read().clone() };
            if let Some(manager) = context_sync_manager {
                let (push_user_id, push_session_id) = self.context_sync_identifiers();
                manager
                    .push_on_complete(self, &push_user_id, &push_session_id)
                    .await?;
            }
        }

        tracing::info!(
            "workflow finished: id={}, total_elapsed_ms={}",
            self.name,
            workflow_started_at.elapsed().as_millis()
        );
        Ok(())
    }

    /// 运行单个节点
    async fn run_node(self: &Arc<Self>, node: &Arc<dyn BaseNode>) -> Result<()> {
        let node_name = node.name().to_string();
        let workflow_id = self.name.clone();
        async {
            let ctx = crate::node::NodeContext::new(Arc::downgrade(self));
            if !node.is_reentrant() {
                let result = node.run(&ctx).await;
                self.mark_node_running(node.name(), false)?;
                if result.is_ok() {
                    self.mark_node_completed(node.name())?;
                }
                return result;
            }

            loop {
                if self.status() == WorkflowStatus::Finished {
                    return Ok(());
                }

                let result = node.run(&ctx).await;
                self.mark_node_running(node.name(), false)?;

                result?;
                self.mark_node_completed(node.name())?;

                if self.status() == WorkflowStatus::Finished {
                    return Ok(());
                }
            }
        }
        .instrument(tracing::info_span!(
            "workflow_node",
            workflow_id = %workflow_id,
            node = %node_name
        ))
        .await
    }

    /// 激活节点
    fn activate_node(&self, node_name: &str) -> Result<()> {
        let nodes = self.nodes.read();
        let node_info = nodes
            .get(node_name)
            .ok_or_else(|| WorkflowError::NodeNotFound(node_name.to_string()))?;

        let running = *node_info.running.lock();
        if running {
            let mut ignored = node_info.activation_ignored_while_running.lock();
            *ignored += 1;
            tracing::info!(
                "node activation ignored while running: workflow_id={}, node={}, ignored_count={}",
                self.name,
                node_name,
                *ignored
            );
            return Ok(());
        }

        // 设置激活标志
        *node_info.activated.lock() = true;
        *node_info.activated_at.lock() = Some(Instant::now());
        let completed_count = *node_info.completed_count.lock();
        tracing::info!(
            "node activated: workflow_id={}, node={}, running={}, completed_count={}",
            self.name,
            node_name,
            running,
            completed_count
        );
        // 通知等待者
        node_info.event.notify_waiters();
        Ok(())
    }

    fn mark_node_running(&self, node_name: &str, running: bool) -> Result<()> {
        let nodes = self.nodes.read();
        let node_info = nodes
            .get(node_name)
            .ok_or_else(|| WorkflowError::NodeNotFound(node_name.to_string()))?;
        *node_info.running.lock() = running;
        Ok(())
    }

    fn mark_node_completed(&self, node_name: &str) -> Result<()> {
        let nodes = self.nodes.read();
        let node_info = nodes
            .get(node_name)
            .ok_or_else(|| WorkflowError::NodeNotFound(node_name.to_string()))?;
        *node_info.completed_count.lock() += 1;
        Ok(())
    }

    /// 等待节点被激活
    ///
    /// # 参数
    /// - node_name: 节点名称
    pub async fn wait_for_event(&self, node_name: &str) -> Result<()> {
        let (event, activated, running) = {
            let nodes = self.nodes.read();
            let node_info = nodes
                .get(node_name)
                .ok_or_else(|| WorkflowError::NodeNotFound(node_name.to_string()))?;
            (
                Arc::clone(&node_info.event),
                Arc::clone(&node_info.activated),
                Arc::clone(&node_info.running),
            )
        };

        loop {
            let consumed = {
                let mut flag = activated.lock();
                if *flag {
                    // 消费本次激活信号，避免重复执行时丢失触发。
                    *flag = false;
                    *running.lock() = true;
                    true
                } else {
                    false
                }
            };
            if consumed {
                return Ok(());
            }

            event.notified().await;
        }
    }

    /// 选择下一个节点
    ///
    /// # 参数
    /// - current_node: 当前节点名称
    /// - target_node: 目标节点名称
    pub async fn choose_node(&self, current_node: &str, target_node: &str) -> Result<()> {
        if target_node == "finish" {
            // 设置完成状态
            *self.finish_node.write() = Some(current_node.to_string());
            *self.status.write() = WorkflowStatus::Finished;

            // 取消所有未激活的节点
            self.cancel_all_inactive_nodes().await?;
        } else {
            // 激活目标节点
            self.activate_node(target_node)?;
        }

        Ok(())
    }

    /// 取消节点
    ///
    /// # 参数
    /// - node_name: 要取消的节点名称
    pub async fn cancel_node(&self, node_name: &str) -> Result<()> {
        if node_name == "finish" {
            return Ok(());
        }

        let nodes = self.nodes.read();
        if let Some(node_info) = nodes.get(node_name) {
            // 可重入节点采用常驻任务模型，运行期不做 abort。
            if node_info.node.is_reentrant() {
                tracing::debug!(
                    "skip cancel reentrant node during runtime: workflow_id={}, node={}",
                    self.name,
                    node_name
                );
                return Ok(());
            }
            // 只取消非关键节点且未被激活的节点
            if !node_info.node.is_key_node() && !*node_info.activated.lock() {
                if let Some(handle) = node_info.abort_handle.lock().as_ref() {
                    handle.abort();
                }
            }
        }

        Ok(())
    }

    /// 取消所有未激活的非关键节点（用于清理）
    ///
    /// 当工作流完成或遇到错误时，取消所有还在等待的节点
    pub async fn cancel_all_inactive_nodes(&self) -> Result<()> {
        let nodes = self.nodes.read();
        for node_info in nodes.values() {
            if !node_info.node.is_key_node() && !*node_info.activated.lock() {
                if let Some(handle) = node_info.abort_handle.lock().as_ref() {
                    handle.abort();
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn ignored_activation_count(&self, node_name: &str) -> Option<u64> {
        self.nodes
            .read()
            .get(node_name)
            .map(|info| *info.activation_ignored_while_running.lock())
    }

    /// 获取外部消息
    ///
    /// # 返回
    /// 返回从外部接收的消息
    pub async fn get_message(&self) -> Result<Value> {
        let mut rx = self.msg_queue_rx.lock().await;
        if let Some(receiver) = rx.as_mut() {
            receiver
                .recv()
                .await
                .ok_or_else(|| WorkflowError::Other("Message queue closed".to_string()))
        } else {
            Err(WorkflowError::Other(
                "Message queue receiver not available".to_string(),
            ))
        }
    }

    /// 添加外部消息
    ///
    /// # 参数
    /// - message: 要添加的消息
    pub async fn add_message(&self, message: Value) -> Result<()> {
        self.msg_queue_tx
            .send(message)
            .map_err(|_| WorkflowError::Other("Failed to send message".to_string()))
    }

    /// 获取工作流结果
    ///
    /// # 返回
    /// 返回最终节点的输出结果
    pub fn get_result(&self) -> Result<HashMap<String, Value>> {
        let status = self.status.read();
        if *status != WorkflowStatus::Finished {
            return Err(WorkflowError::InvalidState(
                "Workflow not finished".to_string(),
            ));
        }

        let finish_node = self.finish_node.read();
        if let Some(node_name) = finish_node.as_ref() {
            Ok(self.parameter_bus.get_node_results(node_name))
        } else {
            Err(WorkflowError::Other("Finish node not set".to_string()))
        }
    }

    /// 设置上下文
    pub fn set_context(&self, key: &str, value: Value) {
        self.context.write().insert(key.to_string(), value);
    }

    /// 获取上下文
    pub fn get_context(&self, key: &str) -> Option<Value> {
        self.context.read().get(key).cloned()
    }

    /// 返回当前工作流上下文中的全部字段名。
    pub fn context_keys(&self) -> Vec<String> {
        self.context.read().keys().cloned().collect()
    }

    /// 设置类型化上下文
    pub fn set_context_object<T>(&self, key: &str, value: T)
    where
        T: Any + Send + Sync + 'static,
    {
        self.context_objects
            .write()
            .insert(key.to_string(), Arc::new(value));
    }

    /// 获取类型化上下文
    pub fn get_context_object<T>(&self, key: &str) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.context_objects
            .read()
            .get(key)
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
    }

    /// 设置记忆
    pub fn set_memory(&self, key: &str, value: Value) {
        self.memory.write().insert(key.to_string(), value);
    }

    /// 获取记忆
    pub fn get_memory(&self, key: &str) -> Option<Value> {
        self.memory.read().get(key).cloned()
    }

    /// 获取工作流状态
    pub fn status(&self) -> WorkflowStatus {
        *self.status.read()
    }

    #[cfg(feature = "context-sync")]
    fn context_sync_identifiers(&self) -> (String, String) {
        let user_id = self
            .get_context("user_id")
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_default();

        let session_id = self
            .get_context("session_id")
            .or_else(|| self.get_context("workflow_id"))
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_default();

        (user_id, session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "context-sync")]
    use crate::context_sync::{ContextOps, ContextSyncConfig, ContextSyncManager, PushConfig};
    use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
    use crate::parameter_bus::ParameterType;
    use async_trait::async_trait;
    use parking_lot::Mutex as ParkingMutex;
    use std::time::Duration;

    // 简单测试节点
    struct SimpleNode {
        name: String,
        config: NodeConfig,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
    }

    impl SimpleNode {
        fn new(name: &str, next_node: &str) -> Self {
            let mut output_params = HashMap::new();
            output_params.insert("result".to_string(), ParameterType::Step);

            let mut choice_map = HashMap::new();
            choice_map.insert("next".to_string(), next_node.to_string());

            Self {
                name: name.to_string(),
                config: NodeConfig {
                    id: 1,
                    node_name: name.to_string(),
                    input_map: HashMap::new(),
                    choice_map,
                    attrs: HashMap::new(),
                    key_node: false,
                },
                input_params: HashMap::new(),
                output_params,
                choices: vec!["next".to_string()],
            }
        }
    }

    #[async_trait]
    impl BaseNode for SimpleNode {
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

        async fn run(&self, ctx: &NodeContext) -> Result<()> {
            // 等待激活
            NodeHelper::wait_for_event(&self.name, ctx).await?;

            // 设置输出
            let value = serde_json::json!({"node": self.name, "status": "completed"});
            NodeHelper::set_output(&self.name, "result", value, ctx).await?;

            // 选择下一个节点
            NodeHelper::set_choice(&self.name, "next", &self.config.choice_map, ctx).await?;

            Ok(())
        }
    }

    struct ReentrantSelfNode {
        name: String,
        config: NodeConfig,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
        run_count: Arc<ParkingMutex<u32>>,
    }

    struct ReentrantPressureNode {
        name: String,
        config: NodeConfig,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
        run_count: Arc<ParkingMutex<u32>>,
        target_runs: u32,
    }

    struct DelayedFinishNode {
        name: String,
        config: NodeConfig,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
    }

    impl DelayedFinishNode {
        fn new(name: &str) -> Self {
            let mut choice_map = HashMap::new();
            choice_map.insert("next".to_string(), "finish".to_string());
            let mut output_params = HashMap::new();
            output_params.insert("result".to_string(), ParameterType::Step);
            Self {
                name: name.to_string(),
                config: NodeConfig {
                    id: 1,
                    node_name: name.to_string(),
                    input_map: HashMap::new(),
                    choice_map,
                    attrs: HashMap::new(),
                    key_node: false,
                },
                input_params: HashMap::new(),
                output_params,
                choices: vec!["next".to_string()],
            }
        }
    }

    #[async_trait]
    impl BaseNode for DelayedFinishNode {
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

        async fn run(&self, ctx: &NodeContext) -> Result<()> {
            NodeHelper::wait_for_event(&self.name, ctx).await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
            NodeHelper::set_output(&self.name, "result", Value::from("ok"), ctx).await?;
            NodeHelper::set_choice(&self.name, "next", &self.config.choice_map, ctx).await?;
            Ok(())
        }
    }

    #[cfg(feature = "context-sync")]
    #[derive(Default)]
    struct CaptureContextClient {
        writes: ParkingMutex<Vec<(String, String, String)>>,
    }

    #[cfg(feature = "context-sync")]
    #[async_trait]
    impl ContextOps for CaptureContextClient {
        async fn get_context(&self, _namespace: &str, _field: &str) -> Result<String> {
            Ok(String::new())
        }

        async fn set_context(&self, namespace: &str, field: &str, value: &str) -> Result<()> {
            self.writes
                .lock()
                .push((namespace.to_string(), field.to_string(), value.to_string()));
            Ok(())
        }
    }

    impl ReentrantSelfNode {
        fn new(name: &str) -> Self {
            let mut output_params = HashMap::new();
            output_params.insert("runs".to_string(), ParameterType::Step);

            let mut choice_map = HashMap::new();
            choice_map.insert("exit".to_string(), "finish".to_string());

            Self {
                name: name.to_string(),
                config: NodeConfig {
                    id: 1,
                    node_name: name.to_string(),
                    input_map: HashMap::new(),
                    choice_map,
                    attrs: HashMap::new(),
                    key_node: false,
                },
                input_params: HashMap::new(),
                output_params,
                choices: vec!["exit".to_string()],
                run_count: Arc::new(ParkingMutex::new(0)),
            }
        }
    }

    impl ReentrantPressureNode {
        fn new(name: &str, target_runs: u32) -> Self {
            let mut output_params = HashMap::new();
            output_params.insert("runs".to_string(), ParameterType::Step);

            let mut choice_map = HashMap::new();
            choice_map.insert("exit".to_string(), "finish".to_string());

            Self {
                name: name.to_string(),
                config: NodeConfig {
                    id: 2,
                    node_name: name.to_string(),
                    input_map: HashMap::new(),
                    choice_map,
                    attrs: HashMap::new(),
                    key_node: false,
                },
                input_params: HashMap::new(),
                output_params,
                choices: vec!["exit".to_string()],
                run_count: Arc::new(ParkingMutex::new(0)),
                target_runs,
            }
        }
    }

    #[async_trait]
    impl BaseNode for ReentrantSelfNode {
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
            let runs = {
                let mut runs = self.run_count.lock();
                *runs += 1;
                *runs
            };

            NodeHelper::set_output(&self.name, "runs", Value::from(runs), ctx).await?;

            if runs >= 2 {
                NodeHelper::set_choice(&self.name, "exit", &self.config.choice_map, ctx).await?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl BaseNode for ReentrantPressureNode {
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
            tokio::time::sleep(Duration::from_millis(10)).await;

            let runs = {
                let mut runs = self.run_count.lock();
                *runs += 1;
                *runs
            };
            NodeHelper::set_output(&self.name, "runs", Value::from(runs), ctx).await?;

            if runs >= self.target_runs {
                NodeHelper::set_choice(&self.name, "exit", &self.config.choice_map, ctx).await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_workflow_execution() {
        // 创建工作流
        let workflow = WorkFlow::new("test_workflow".to_string());

        // 创建节点
        let node1 = Arc::new(SimpleNode::new("node1", "node2")) as Arc<dyn BaseNode>;
        let node2 = Arc::new(SimpleNode::new("node2", "finish")) as Arc<dyn BaseNode>;

        // 设置节点
        workflow.set_nodes(vec![node1, node2]).unwrap();
        workflow.set_start_node("node1");

        // 运行工作流
        workflow.run_all().await.unwrap();

        // 验证状态
        assert_eq!(workflow.status(), WorkflowStatus::Finished);

        // 验证结果
        let result = workflow.get_result().unwrap();
        assert!(result.contains_key("result"));
    }

    #[tokio::test]
    async fn test_reentrant_node_can_run_multiple_times() {
        let workflow = WorkFlow::new("reentrant_workflow".to_string());
        let loop_node = Arc::new(ReentrantSelfNode::new("loop")) as Arc<dyn BaseNode>;
        workflow.set_nodes(vec![loop_node]).unwrap();
        workflow.set_start_node("loop");

        let workflow_for_task = Arc::clone(&workflow);
        let handle = tokio::spawn(async move { workflow_for_task.run_all().await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        workflow
            .activate_node("loop")
            .expect("activate loop second time");

        handle
            .await
            .expect("join run_all")
            .expect("run_all success");

        assert_eq!(workflow.status(), WorkflowStatus::Finished);
        let result = workflow.get_result().unwrap();
        assert_eq!(result.get("runs"), Some(&Value::from(2u32)));
    }

    #[tokio::test]
    async fn test_activate_node_ignores_when_node_is_running() {
        let workflow = WorkFlow::new("activate_ignore_running".to_string());
        let node = Arc::new(DelayedFinishNode::new("node1")) as Arc<dyn BaseNode>;
        workflow.set_nodes(vec![node]).unwrap();
        workflow.set_start_node("node1");

        let workflow_for_task = Arc::clone(&workflow);
        let handle = tokio::spawn(async move { workflow_for_task.run_all().await });

        tokio::time::sleep(Duration::from_millis(5)).await;
        workflow
            .activate_node("node1")
            .expect("activate should not fail");

        handle
            .await
            .expect("join run_all")
            .expect("run_all success");

        assert_eq!(workflow.ignored_activation_count("node1"), Some(1));
    }

    #[tokio::test]
    async fn test_cancel_node_does_not_abort_reentrant_node() {
        let workflow = WorkFlow::new("cancel_reentrant_skip".to_string());
        let loop_node = Arc::new(ReentrantSelfNode::new("loop")) as Arc<dyn BaseNode>;
        workflow.set_nodes(vec![loop_node]).unwrap();
        workflow.set_start_node("loop");

        let workflow_for_task = Arc::clone(&workflow);
        let handle = tokio::spawn(async move { workflow_for_task.run_all().await });

        tokio::time::sleep(Duration::from_millis(5)).await;
        workflow
            .cancel_node("loop")
            .await
            .expect("cancel node should not fail");
        workflow
            .activate_node("loop")
            .expect("activate loop second time");

        tokio::time::timeout(Duration::from_millis(500), handle)
            .await
            .expect("run_all timeout")
            .expect("join run_all")
            .expect("run_all success");

        assert_eq!(workflow.status(), WorkflowStatus::Finished);
        let result = workflow.get_result().expect("workflow result");
        assert_eq!(result.get("runs"), Some(&Value::from(2u32)));
    }

    #[tokio::test]
    async fn test_reentrant_node_high_frequency_activation_no_deadlock() {
        let target_runs = 20_u32;
        let workflow = WorkFlow::new("reentrant_pressure".to_string());
        let node =
            Arc::new(ReentrantPressureNode::new("pressure", target_runs)) as Arc<dyn BaseNode>;
        workflow.set_nodes(vec![node]).unwrap();
        workflow.set_start_node("pressure");

        let workflow_for_run = Arc::clone(&workflow);
        let run_handle = tokio::spawn(async move { workflow_for_run.run_all().await });

        let workflow_for_activate = Arc::clone(&workflow);
        let activate_handle = tokio::spawn(async move {
            while workflow_for_activate.status() != WorkflowStatus::Finished {
                let _ = workflow_for_activate.activate_node("pressure");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        tokio::time::timeout(Duration::from_secs(3), run_handle)
            .await
            .expect("run_all timeout")
            .expect("join run_all")
            .expect("run_all success");
        activate_handle.await.expect("join activate task");

        assert_eq!(workflow.status(), WorkflowStatus::Finished);
        assert!(
            workflow
                .ignored_activation_count("pressure")
                .expect("ignored count exists")
                > 0
        );
        assert_eq!(
            workflow
                .get_result()
                .expect("workflow result")
                .get("runs")
                .cloned(),
            Some(Value::from(target_runs))
        );
    }

    #[tokio::test]
    async fn test_workflow_context_and_memory() {
        let workflow = WorkFlow::new("test_workflow".to_string());

        // 测试上下文
        workflow.set_context("key1", serde_json::json!("value1"));
        assert_eq!(
            workflow.get_context("key1"),
            Some(serde_json::json!("value1"))
        );

        // 测试记忆
        workflow.set_memory("mem1", serde_json::json!({"data": 42}));
        assert_eq!(
            workflow.get_memory("mem1"),
            Some(serde_json::json!({"data": 42}))
        );
    }

    #[tokio::test]
    async fn test_run_all_without_start_node_returns_error() {
        // 验证：未设置 start_node 时运行工作流会返回配置错误。
        let workflow = WorkFlow::new("missing_start".to_string());
        let node = Arc::new(SimpleNode::new("node1", "finish")) as Arc<dyn BaseNode>;
        workflow.set_nodes(vec![node]).unwrap();

        let err = workflow.run_all().await.unwrap_err();
        assert!(err.to_string().contains("Start node not set"));
    }

    #[test]
    fn test_get_result_before_finished_returns_error() {
        // 验证：工作流未完成时调用 get_result 返回状态错误。
        let workflow = WorkFlow::new("not_finished".to_string());
        let err = workflow.get_result().unwrap_err();
        assert!(err.to_string().contains("Workflow not finished"));
    }

    #[tokio::test]
    async fn test_message_queue_round_trip() {
        // 验证：外部消息入队和读取流程正常。
        let workflow = WorkFlow::new("message_queue".to_string());
        let expected = serde_json::json!({"event": "input", "text": "hello"});
        workflow.add_message(expected.clone()).await.unwrap();
        let actual = workflow.get_message().await.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_context_object_typed_access() {
        // 验证：类型化上下文支持正确类型读取，不匹配类型返回 None。
        let workflow = WorkFlow::new("context_object".to_string());
        workflow.set_context_object("counter", 7usize);

        let matched = workflow.get_context_object::<usize>("counter").unwrap();
        assert_eq!(*matched, 7usize);
        assert!(workflow.get_context_object::<String>("counter").is_none());
    }

    #[cfg(feature = "context-sync")]
    #[tokio::test]
    async fn test_context_sync_push_uses_latest_runtime_identifiers() {
        let workflow = WorkFlow::new("ctx_runtime_identifiers".to_string());
        let node = Arc::new(DelayedFinishNode::new("node1")) as Arc<dyn BaseNode>;
        workflow.set_nodes(vec![node]).unwrap();
        workflow.set_start_node("node1");

        let client = Arc::new(CaptureContextClient::default());
        let manager = ContextSyncManager::new_with_client(
            client.clone(),
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: Vec::new(),
                push_on_complete: vec![PushConfig {
                    from_context: "workflow_result".to_string(),
                    to_namespace: "user:{user_id}:session:{session_id}".to_string(),
                    to_field: "last_workflow_result".to_string(),
                    alias: None,
                    structured: false,
                }],
            },
        );
        workflow.set_context_sync(manager);

        let workflow_for_task = Arc::clone(&workflow);
        let handle = tokio::spawn(async move { workflow_for_task.run_all().await });

        tokio::time::sleep(Duration::from_millis(5)).await;
        workflow.set_context("user_id", Value::from("u1"));
        workflow.set_context("session_id", Value::from("s1"));
        workflow.set_context("workflow_result", Value::from("done"));

        handle
            .await
            .expect("join run_all")
            .expect("run_all success");
        let writes = client.writes.lock().clone();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, "user:u1:session:s1");
        assert_eq!(writes[0].1, "last_workflow_result");
    }
}
