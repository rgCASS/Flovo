//! 通用 LLM 调用节点。
//!
//! 节点只依赖核心层的 [`LlmApi`] 抽象。宿主未注入实现时，输出可配置的
//! mock 文本，保证示例工作流无需密钥即可运行。

use crate::error::{Result, WorkflowError};
use crate::llm::{ChunkStatus, LlmApi, StreamChunk};
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::nodes::OutboundSender;
use crate::parameter_bus::ParameterType;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// 调用宿主注入 LLM 实现并将响应写回参数总线的节点。
pub struct LlmCallNode {
    name: String,
    config: NodeConfig,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
    stream: bool,
    system_prompt: Option<String>,
    mock_output: String,
}

impl LlmCallNode {
    /// 创建节点并缓存 `attrs` 配置，避免每次运行重复解析。
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        let mut input_params = HashMap::new();
        input_params.insert("prompt".to_string(), ParameterType::Step);
        let mut output_params = HashMap::new();
        output_params.insert("result".to_string(), ParameterType::Step);

        let stream = config
            .attrs
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let system_prompt = config
            .attrs
            .get("system_prompt")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|value| !value.is_empty());
        let mock_output = config
            .attrs
            .get("mock_output")
            .and_then(Value::as_str)
            .unwrap_or("[mock] {prompt}")
            .to_string();

        let node = Self {
            name: config.node_name.clone(),
            config,
            input_params,
            output_params,
            choices: vec!["default".to_string()],
            stream,
            system_prompt,
            mock_output,
        };
        tracing::info!(
            "llm_call node created: node={}, stream={}",
            node.name,
            node.stream
        );
        Ok(node)
    }

    fn request_text(&self, prompt: String) -> String {
        match self.system_prompt.as_deref() {
            Some(system) => format!("{system}\n\n{prompt}"),
            None => prompt,
        }
    }

    fn mock_text(&self, prompt: &str) -> String {
        self.mock_output.replace("{prompt}", prompt)
    }
}

#[async_trait]
impl BaseNode for LlmCallNode {
    fn name(&self) -> &str {
        &self.name
    }

    fn node_type(&self) -> NodeType {
        if self.stream {
            NodeType::Stream
        } else {
            NodeType::Step
        }
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
        let prompt = NodeHelper::get_input("prompt", &self.config.input_map, ctx)
            .await?
            .map(|value| match value {
                Value::String(text) => text,
                other => other.to_string(),
            })
            .unwrap_or_default();
        let request_text = self.request_text(prompt);
        let workflow = ctx.get_workflow()?;

        // set_context_object 会再包一层 Arc，故这里读取 Arc<dyn LlmApi> 对象。
        let llm = workflow
            .get_context_object::<Arc<dyn LlmApi>>("llm")
            .map(|value| value.as_ref().clone());

        let result = if let Some(llm) = llm {
            if !self.stream {
                llm.chat(request_text).await?
            } else {
                let sender = workflow.get_context_object::<OutboundSender>("outbound_sender");
                let accumulated = Arc::new(Mutex::new(String::new()));
                let callback_error: Arc<Mutex<Option<WorkflowError>>> = Arc::new(Mutex::new(None));
                let accumulated_ref = Arc::clone(&accumulated);
                let callback_error_ref = Arc::clone(&callback_error);
                let sender_ref = sender.clone();
                let callback = Box::new(move |chunk: StreamChunk| {
                    if chunk.status != ChunkStatus::Data {
                        return;
                    }
                    if let Ok(mut result) = accumulated_ref.lock() {
                        result.push_str(&chunk.content);
                    }
                    if let Some(sender) = sender_ref.as_ref() {
                        if let Err(error) = sender.send(Value::String(chunk.content.clone())) {
                            if let Ok(mut slot) = callback_error_ref.lock() {
                                *slot = Some(WorkflowError::Other(format!(
                                    "outbound channel closed: {error}"
                                )));
                            }
                        }
                    }
                });
                llm.chat_stream(request_text, callback).await?;
                if let Ok(mut slot) = callback_error.lock() {
                    if let Some(error) = slot.take() {
                        return Err(error);
                    }
                }
                accumulated.lock().map(|value| value.clone()).map_err(|_| {
                    WorkflowError::Other("failed to lock accumulated LLM result".into())
                })?
            }
        } else {
            tracing::warn!(
                "llm_call node has no injected LLM implementation: node={}",
                self.name
            );
            self.mock_text(&request_text)
        };

        NodeHelper::set_output(&self.name, "result", Value::String(result), ctx).await?;
        NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeContext;
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    struct MockLlm;

    #[async_trait]
    impl LlmApi for MockLlm {
        async fn chat(&self, prompt: String) -> Result<String> {
            Ok(format!("reply:{prompt}"))
        }

        async fn chat_stream(
            &self,
            _prompt: String,
            callback: Box<dyn Fn(StreamChunk) + Send + Sync>,
        ) -> Result<()> {
            callback(StreamChunk::data("a"));
            callback(StreamChunk::data("b"));
            callback(StreamChunk::finish(""));
            Ok(())
        }
    }

    fn config(stream: bool) -> NodeConfig {
        let mut input_map = HashMap::new();
        input_map.insert("prompt".to_string(), Some("source.result".to_string()));
        let mut choice_map = HashMap::new();
        choice_map.insert("default".to_string(), "finish".to_string());
        let mut attrs = HashMap::new();
        attrs.insert("stream".to_string(), Value::Bool(stream));
        NodeConfig {
            id: 1,
            node_name: "llm_call".to_string(),
            input_map,
            choice_map,
            attrs,
            key_node: false,
        }
    }

    async fn run_node(node: LlmCallNode, workflow: Arc<crate::workflow::WorkFlow>) {
        let source = HashMap::from([(String::from("result"), ParameterType::Step)]);
        workflow
            .parameter_bus
            .init_output_parameters("source", &source);
        workflow
            .parameter_bus
            .set_value("source", "result", Value::String("ping".into()))
            .await
            .unwrap();
        workflow.set_nodes(vec![Arc::new(node)]).unwrap();
        workflow.set_start_node("llm_call");
        workflow.run_all().await.unwrap();
    }

    #[tokio::test]
    async fn uses_mock_output_without_injection() {
        let workflow = crate::workflow::WorkFlow::new("test".into());
        run_node(
            LlmCallNode::new(config(false), NodeContext::new(Arc::downgrade(&workflow))).unwrap(),
            Arc::clone(&workflow),
        )
        .await;
        assert_eq!(
            workflow
                .parameter_bus
                .get_value("llm_call.result")
                .await
                .unwrap(),
            Value::String("[mock] ping".into())
        );
    }

    #[tokio::test]
    async fn uses_injected_sync_llm() {
        let workflow = crate::workflow::WorkFlow::new("test".into());
        workflow.set_context_object("llm", Arc::new(MockLlm) as Arc<dyn LlmApi>);
        run_node(
            LlmCallNode::new(config(false), NodeContext::new(Arc::downgrade(&workflow))).unwrap(),
            Arc::clone(&workflow),
        )
        .await;
        assert_eq!(
            workflow
                .parameter_bus
                .get_value("llm_call.result")
                .await
                .unwrap(),
            Value::String("reply:ping".into())
        );
    }

    #[tokio::test]
    async fn streams_data_and_accumulates_result() {
        let workflow = crate::workflow::WorkFlow::new("test".into());
        workflow.set_context_object("llm", Arc::new(MockLlm) as Arc<dyn LlmApi>);
        let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
        workflow.set_context_object("outbound_sender", tx);
        run_node(
            LlmCallNode::new(config(true), NodeContext::new(Arc::downgrade(&workflow))).unwrap(),
            Arc::clone(&workflow),
        )
        .await;
        assert_eq!(rx.recv().await.unwrap(), Value::String("a".into()));
        assert_eq!(rx.recv().await.unwrap(), Value::String("b".into()));
        assert!(rx.try_recv().is_err());
        assert_eq!(
            workflow
                .parameter_bus
                .get_value("llm_call.result")
                .await
                .unwrap(),
            Value::String("ab".into())
        );
    }

    #[test]
    fn stream_flag_controls_node_type() {
        let step =
            LlmCallNode::new(config(false), NodeContext::new(std::sync::Weak::new())).unwrap();
        let stream =
            LlmCallNode::new(config(true), NodeContext::new(std::sync::Weak::new())).unwrap();
        assert_eq!(step.node_type(), NodeType::Step);
        assert_eq!(stream.node_type(), NodeType::Stream);
    }
}
