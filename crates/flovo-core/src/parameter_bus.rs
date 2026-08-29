/// 参数总线模块
///
/// 实现工作流节点之间的参数传递，支持两种参数类型：
/// - step: 单次传递，使用 Notify 进行同步
/// - stream: 流式传递，使用 mpsc channel 支持多消费者
use crate::error::{Result, WorkflowError};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

/// 参数类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParameterType {
    Step,
    Stream,
}

/// Step 类型参数
struct StepParameter {
    value: Option<Value>,
    notify: Arc<Notify>,
}

impl StepParameter {
    fn new() -> Self {
        Self {
            value: None,
            notify: Arc::new(Notify::new()),
        }
    }
}

/// Stream 类型参数的订阅者信息
type StreamSubscribers = HashMap<String, mpsc::UnboundedSender<Value>>;

/// 参数总线
///
/// 管理工作流中所有节点之间的参数传递
pub struct ParameterBus {
    /// Step 类型参数存储
    step_params: Arc<RwLock<HashMap<String, StepParameter>>>,

    /// Stream 类型参数的订阅者
    stream_subscribers: Arc<RwLock<HashMap<String, StreamSubscribers>>>,

    /// Stream 参数历史缓存（用于晚订阅者回放，保证顺序与不丢包）
    stream_history: Arc<RwLock<HashMap<String, Vec<Value>>>>,

    /// 参数类型映射
    param_types: Arc<RwLock<HashMap<String, ParameterType>>>,
}

impl ParameterBus {
    /// 创建新的参数总线
    pub fn new() -> Self {
        Self {
            step_params: Arc::new(RwLock::new(HashMap::new())),
            stream_subscribers: Arc::new(RwLock::new(HashMap::new())),
            stream_history: Arc::new(RwLock::new(HashMap::new())),
            param_types: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 初始化节点的输出参数
    ///
    /// # 参数
    /// - node_name: 节点名称
    /// - output_params: 输出参数映射 (参数名 -> 参数类型)
    pub fn init_output_parameters(
        &self,
        node_name: &str,
        output_params: &HashMap<String, ParameterType>,
    ) {
        let mut step_params = self.step_params.write();
        let mut param_types = self.param_types.write();

        for (param_name, param_type) in output_params {
            let key = format!("{}.{}", node_name, param_name);

            match param_type {
                ParameterType::Step => {
                    step_params.insert(key.clone(), StepParameter::new());
                }
                ParameterType::Stream => {
                    // 提前初始化 stream 历史缓存，便于后续晚订阅回放。
                    self.stream_history.write().entry(key.clone()).or_default();
                }
            }

            param_types.insert(key, param_type.clone());
        }
    }

    /// 为 Stream 类型参数注册订阅者
    ///
    /// # 参数
    /// - param_key: 参数键 (格式: "节点名.参数名")
    /// - consumer_node: 消费者节点名称
    ///
    /// # 返回
    /// 返回接收端 (Receiver)，用于接收流式数据
    pub fn register_stream_subscriber(
        &self,
        param_key: &str,
        consumer_node: &str,
    ) -> Result<mpsc::UnboundedReceiver<Value>> {
        // 检查参数是否存在
        let param_types = self.param_types.read();
        let param_type = param_types
            .get(param_key)
            .ok_or_else(|| WorkflowError::ParameterNotFound(param_key.to_string()))?;

        // 检查参数类型是否为 Stream
        if param_type != &ParameterType::Stream {
            return Err(WorkflowError::ConfigError(format!(
                "Parameter {} must be stream type, but found {:?}",
                param_key, param_type
            )));
        }
        drop(param_types);

        // 创建 channel 并注册订阅者
        let (tx, rx) = mpsc::unbounded_channel();
        let mut subscribers = self.stream_subscribers.write();

        subscribers
            .entry(param_key.to_string())
            .or_default()
            .insert(consumer_node.to_string(), tx);
        drop(subscribers);

        // 回放该 stream 参数已有历史，保证晚订阅也能按顺序消费到先前 chunk。
        if let Some(history) = self.stream_history.read().get(param_key) {
            if let Some(subscriber) = self
                .stream_subscribers
                .read()
                .get(param_key)
                .and_then(|subs| subs.get(consumer_node))
            {
                for item in history {
                    if let Err(err) = subscriber.send(item.clone()) {
                        tracing::warn!(
                            "Failed to replay stream value to consumer {}: {}",
                            consumer_node,
                            err
                        );
                        break;
                    }
                }
            }
        }

        Ok(rx)
    }

    /// 设置参数值
    ///
    /// # 参数
    /// - node_name: 节点名称
    /// - param_name: 参数名称
    /// - value: 参数值
    pub async fn set_value(&self, node_name: &str, param_name: &str, value: Value) -> Result<()> {
        let key = format!("{}.{}", node_name, param_name);

        // 获取参数类型
        let param_types = self.param_types.read();
        let param_type = param_types
            .get(&key)
            .ok_or_else(|| WorkflowError::ParameterNotFound(key.clone()))?
            .clone();
        drop(param_types);

        match param_type {
            ParameterType::Step => {
                // Step 类型：设置值并通知
                let mut step_params = self.step_params.write();
                if let Some(param) = step_params.get_mut(&key) {
                    param.value = Some(value);
                    param.notify.notify_waiters();
                } else {
                    return Err(WorkflowError::ParameterNotFound(key));
                }
            }
            ParameterType::Stream => {
                // 先持久化历史，确保订阅者稍后注册也可回放完整流。
                self.stream_history
                    .write()
                    .entry(key.clone())
                    .or_default()
                    .push(value.clone());

                // Stream 类型：向所有订阅者发送数据
                let subscribers = self.stream_subscribers.read();
                if let Some(subs) = subscribers.get(&key) {
                    for (consumer, tx) in subs {
                        if let Err(e) = tx.send(value.clone()) {
                            tracing::warn!(
                                "Failed to send stream value to consumer {}: {}",
                                consumer,
                                e
                            );
                        }
                    }
                }
                // 如果没有订阅者，也不报错（允许没有消费者的情况）
            }
        }

        Ok(())
    }

    /// 获取参数值
    ///
    /// # 参数
    /// - node_name: 当前节点名称（仅用于 stream 类型）
    /// - param_key: 参数键 (格式: "节点名.参数名")
    ///
    /// 注意：对于 stream 类型，需要先通过 register_stream_subscriber 注册订阅者，
    /// 然后使用返回的 Receiver 来接收数据。这个方法主要用于 step 类型。
    pub async fn get_value(&self, param_key: &str) -> Result<Value> {
        // 获取参数类型
        let param_type = {
            let param_types = self.param_types.read();
            param_types
                .get(param_key)
                .ok_or_else(|| WorkflowError::ParameterNotFound(param_key.to_string()))?
                .clone()
        };

        match param_type {
            ParameterType::Step => {
                // 先检查值是否已经存在
                let existing_value = {
                    let step_params = self.step_params.read();
                    if let Some(param) = step_params.get(param_key) {
                        param.value.clone()
                    } else {
                        return Err(WorkflowError::ParameterNotFound(param_key.to_string()));
                    }
                };

                // 如果值已经存在，直接返回
                if let Some(value) = existing_value {
                    return Ok(value);
                }

                // 值还未设置，需要等待通知
                let notify = {
                    let step_params = self.step_params.read();
                    let param = step_params
                        .get(param_key)
                        .ok_or_else(|| WorkflowError::ParameterNotFound(param_key.to_string()))?;
                    param.notify.clone()
                };

                // 等待通知
                notify.notified().await;

                // 获取值
                let value = {
                    let step_params = self.step_params.read();
                    let param = step_params
                        .get(param_key)
                        .ok_or_else(|| WorkflowError::ParameterNotFound(param_key.to_string()))?;

                    param.value.clone().ok_or_else(|| {
                        WorkflowError::Other(format!("Parameter {} value is None", param_key))
                    })?
                };

                Ok(value)
            }
            ParameterType::Stream => Err(WorkflowError::ConfigError(
                "Stream parameters should be accessed via registered receiver".to_string(),
            )),
        }
    }

    /// 获取节点的所有输出结果（仅 step 类型参数）
    ///
    /// # 参数
    /// - node_name: 节点名称
    ///
    /// # 返回
    /// 返回参数名到值的映射
    pub fn get_node_results(&self, node_name: &str) -> HashMap<String, Value> {
        let step_params = self.step_params.read();
        let param_types = self.param_types.read();
        let mut results = HashMap::new();

        let prefix = format!("{}.", node_name);

        for (key, param) in step_params.iter() {
            if key.starts_with(&prefix) {
                // 确认是 step 类型
                if let Some(ParameterType::Step) = param_types.get(key) {
                    if let Some(value) = &param.value {
                        // 提取参数名（去掉节点名前缀）
                        let param_name = &key[prefix.len()..];
                        results.insert(param_name.to_string(), value.clone());
                    }
                }
            }
        }

        results
    }

    /// 检查参数是否存在
    pub fn check_parameter(&self, param_key: &str) -> bool {
        self.param_types.read().contains_key(param_key)
    }

    /// 获取参数类型
    pub fn get_parameter_type(&self, param_key: &str) -> Option<ParameterType> {
        self.param_types.read().get(param_key).cloned()
    }
}

impl Default for ParameterBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_step_parameter() {
        let bus = ParameterBus::new();

        // 初始化输出参数
        let mut outputs = HashMap::new();
        outputs.insert("result".to_string(), ParameterType::Step);
        bus.init_output_parameters("node1", &outputs);

        // 设置值
        let value = serde_json::json!({"data": 42});
        bus.set_value("node1", "result", value.clone())
            .await
            .unwrap();

        // 获取值
        let retrieved = bus.get_value("node1.result").await.unwrap();
        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_stream_parameter() {
        let bus = ParameterBus::new();

        // 初始化输出参数
        let mut outputs = HashMap::new();
        outputs.insert("stream".to_string(), ParameterType::Stream);
        bus.init_output_parameters("node1", &outputs);

        // 注册订阅者
        let mut rx1 = bus
            .register_stream_subscriber("node1.stream", "consumer1")
            .unwrap();
        let mut rx2 = bus
            .register_stream_subscriber("node1.stream", "consumer2")
            .unwrap();

        // 发送值
        let value1 = serde_json::json!({"msg": "hello"});
        bus.set_value("node1", "stream", value1.clone())
            .await
            .unwrap();

        // 两个订阅者都应该收到
        assert_eq!(rx1.recv().await.unwrap(), value1);
        assert_eq!(rx2.recv().await.unwrap(), value1);
    }

    #[tokio::test]
    async fn test_stream_late_subscriber_replays_history_in_order() {
        // 验证：订阅者晚于生产者注册时，仍能按顺序收到历史流数据，不丢包。
        let bus = ParameterBus::new();

        let mut outputs = HashMap::new();
        outputs.insert("stream".to_string(), ParameterType::Stream);
        bus.init_output_parameters("node1", &outputs);

        let first = serde_json::json!({"status": 1, "text": "A"});
        let second = serde_json::json!({"status": 1, "text": "B"});
        let finish = serde_json::json!({"status": 2, "text": ""});

        bus.set_value("node1", "stream", first.clone())
            .await
            .unwrap();
        bus.set_value("node1", "stream", second.clone())
            .await
            .unwrap();
        bus.set_value("node1", "stream", finish.clone())
            .await
            .unwrap();

        let mut rx = bus
            .register_stream_subscriber("node1.stream", "late_consumer")
            .unwrap();

        assert_eq!(rx.recv().await.unwrap(), first);
        assert_eq!(rx.recv().await.unwrap(), second);
        assert_eq!(rx.recv().await.unwrap(), finish);
    }

    #[tokio::test]
    async fn test_register_stream_subscriber_parameter_not_found() {
        // 验证：订阅未注册参数时返回 ParameterNotFound。
        let bus = ParameterBus::new();
        let err = bus
            .register_stream_subscriber("missing.stream", "consumer")
            .unwrap_err();
        assert!(err.to_string().contains("Parameter not found"));
    }

    #[tokio::test]
    async fn test_register_stream_subscriber_requires_stream_type() {
        // 验证：step 参数不能作为 stream 订阅源。
        let bus = ParameterBus::new();
        let mut outputs = HashMap::new();
        outputs.insert("result".to_string(), ParameterType::Step);
        bus.init_output_parameters("node1", &outputs);

        let err = bus
            .register_stream_subscriber("node1.result", "consumer")
            .unwrap_err();
        assert!(err.to_string().contains("must be stream type"));
    }

    #[tokio::test]
    async fn test_get_value_on_stream_returns_error() {
        // 验证：stream 参数不能通过 get_value 直接读取。
        let bus = ParameterBus::new();
        let mut outputs = HashMap::new();
        outputs.insert("stream".to_string(), ParameterType::Stream);
        bus.init_output_parameters("node1", &outputs);

        let err = bus.get_value("node1.stream").await.unwrap_err();
        assert!(err
            .to_string()
            .contains("Stream parameters should be accessed via registered receiver"));
    }

    #[tokio::test]
    async fn test_parameter_query_and_node_results() {
        // 验证：参数存在性/类型查询和节点结果聚合仅返回 step 已赋值参数。
        let bus = ParameterBus::new();
        let mut outputs = HashMap::new();
        outputs.insert("a".to_string(), ParameterType::Step);
        outputs.insert("b".to_string(), ParameterType::Step);
        outputs.insert("s".to_string(), ParameterType::Stream);
        bus.init_output_parameters("node1", &outputs);

        bus.set_value("node1", "a", serde_json::json!(123))
            .await
            .unwrap();

        assert!(bus.check_parameter("node1.a"));
        assert_eq!(
            bus.get_parameter_type("node1.s"),
            Some(ParameterType::Stream)
        );
        assert!(!bus.check_parameter("node1.missing"));

        let results = bus.get_node_results("node1");
        assert_eq!(results.get("a"), Some(&serde_json::json!(123)));
        assert!(!results.contains_key("b"));
        assert!(!results.contains_key("s"));
    }
}
