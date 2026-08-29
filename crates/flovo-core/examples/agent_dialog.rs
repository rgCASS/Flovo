//! Agent 问答流：接收、转换、判断、模型流式抽象和结果回写。

use async_trait::async_trait;
use flovo_core::config::{NodeConfigJson, WorkflowConfig};
#[cfg(feature = "context-sync")]
use flovo_core::context_sync::{ContextOps, ContextSyncConfig, ContextSyncManager, PushConfig};
use flovo_core::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeRegistry, NodeType};
use flovo_core::nodes::{register_builtin_nodes, OutboundSender};
use flovo_core::{ChunkStatus, LlmApi, Result, StreamChunk, WorkflowBuilder};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct MockModel;

#[async_trait]
impl LlmApi for MockModel {
    async fn chat(&self, prompt: String) -> Result<String> {
        Ok(format!("Mock answer for: {prompt}"))
    }

    async fn chat_stream(
        &self,
        prompt: String,
        callback: Box<dyn Fn(StreamChunk) + Send + Sync>,
    ) -> Result<()> {
        callback(StreamChunk::data("Mock "));
        callback(StreamChunk::data("streaming answer for: "));
        callback(StreamChunk::finish(prompt));
        Ok(())
    }
}

struct ModelNode {
    name: String,
    config: NodeConfig,
    model: Arc<dyn LlmApi>,
    input_params: HashMap<String, flovo_core::ParameterType>,
    output_params: HashMap<String, flovo_core::ParameterType>,
    choices: Vec<String>,
}

impl ModelNode {
    fn new(config: NodeConfig, model: Arc<dyn LlmApi>) -> Self {
        Self {
            name: config.node_name.clone(),
            config,
            model,
            input_params: HashMap::from([("input".to_string(), flovo_core::ParameterType::Step)]),
            output_params: HashMap::from([(
                "response".to_string(),
                flovo_core::ParameterType::Step,
            )]),
            choices: vec!["default".to_string()],
        }
    }
}

#[async_trait]
impl BaseNode for ModelNode {
    fn name(&self) -> &str {
        &self.name
    }
    fn node_type(&self) -> NodeType {
        NodeType::Stream
    }
    fn input_parameters(&self) -> &HashMap<String, flovo_core::ParameterType> {
        &self.input_params
    }
    fn output_parameters(&self) -> &HashMap<String, flovo_core::ParameterType> {
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
        let prompt = NodeHelper::get_input("input", &self.config.input_map, ctx)
            .await?
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_default();
        let response = Arc::new(Mutex::new(String::new()));
        let sink = Arc::clone(&response);
        self.model
            .chat_stream(
                prompt,
                Box::new(move |chunk| {
                    let mut value = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    value.push_str(&chunk.content);
                    if chunk.status == ChunkStatus::Data {
                        print!("{}", chunk.content);
                    } else {
                        println!("{}", chunk.content);
                    }
                }),
            )
            .await?;
        let response = response
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        ctx.get_workflow()?
            .set_context("answer", Value::String(response.clone()));
        NodeHelper::set_output(&self.name, "response", Value::String(response), ctx).await?;
        NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;
        Ok(())
    }
}

#[cfg(feature = "context-sync")]
#[derive(Default)]
struct MockContextStore {
    values: Mutex<HashMap<(String, String), String>>,
}

#[cfg(feature = "context-sync")]
#[async_trait]
impl ContextOps for MockContextStore {
    async fn get_context(&self, namespace: &str, field: &str) -> Result<String> {
        Ok(self
            .values
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(namespace.to_string(), field.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    async fn set_context(&self, namespace: &str, field: &str, value: &str) -> Result<()> {
        self.values
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                (namespace.to_string(), field.to_string()),
                value.to_string(),
            );
        Ok(())
    }
}

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
    let model: Arc<dyn LlmApi> = Arc::new(MockModel);
    registry.register("mock_model", move |config, _ctx| {
        Ok(Arc::new(ModelNode::new(config, Arc::clone(&model))))
    });

    #[cfg(feature = "context-sync")]
    let sync_config = ContextSyncConfig {
        enabled: true,
        fetch_on_start: Vec::new(),
        push_on_complete: vec![PushConfig {
            from_context: "answer".to_string(),
            to_namespace: "user:{user_id}:session:{session_id}".to_string(),
            to_field: "last_answer".to_string(),
            alias: None,
            structured: false,
        }],
    };

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
                &[("default", "extract_question")],
                json!({"mode": "recv"}),
            ),
            node(
                2,
                "transform_node",
                "extract_question",
                &[("input", Some("receive.output"))],
                &[("default", "question_check")],
                json!({"transform_type": "extract_field", "field_path": "question"}),
            ),
            node(
                3,
                "condition_node",
                "question_check",
                &[("input", Some("extract_question.result"))],
                &[("true_choice", "model"), ("false_choice", "fallback")],
                json!({"condition_type": "contains", "compare_value": "?"}),
            ),
            node(
                4,
                "mock_model",
                "model",
                &[("input", Some("question_check.input"))],
                &[("default", "reply")],
                json!({}),
            ),
            node(
                5,
                "send_recv_node",
                "reply",
                &[("input", Some("model.response"))],
                &[("default", "finish")],
                json!({"mode": "send"}),
            ),
            node(
                6,
                "print_node",
                "fallback",
                &[("input", Some("question_check.input"))],
                &[("default", "finish")],
                json!({"prefix": "[fallback]"}),
            ),
        ],
        #[cfg(feature = "context-sync")]
        context_sync: Some(sync_config.clone()),
    };

    let builder = WorkflowBuilder::new(
        registry,
        HashMap::from([("agent_dialog".to_string(), config)]),
    );
    #[cfg(feature = "context-sync")]
    let context_store = Arc::new(MockContextStore::default());
    #[cfg(feature = "context-sync")]
    {
        let builder = builder.with_context_sync(ContextSyncManager::new_with_client(
            context_store.clone(),
            sync_config,
        ));
        return run_workflow(builder, Some(context_store)).await;
    }

    #[cfg(not(feature = "context-sync"))]
    run_workflow(builder, None).await
}

async fn run_workflow(
    builder: WorkflowBuilder,
    #[cfg(feature = "context-sync")] context_store: Option<Arc<MockContextStore>>,
    #[cfg(not(feature = "context-sync"))] _context_store: Option<()>,
) -> Result<()> {
    let workflow = builder.build("agent_dialog")?;
    workflow.set_context("user_id", Value::String("user-demo".to_string()));
    workflow.set_context("session_id", Value::String("session-demo".to_string()));
    let (outbound_tx, mut outbound_rx): (OutboundSender, _) =
        tokio::sync::mpsc::unbounded_channel();
    workflow.set_context_object("outbound_sender", outbound_tx);
    workflow
        .add_message(json!({"question": "What can this workflow do?"}))
        .await?;
    workflow.run_all().await?;
    println!(
        "outbound={}",
        outbound_rx.recv().await.unwrap_or(Value::Null)
    );

    #[cfg(feature = "context-sync")]
    if let Some(context_store) = context_store {
        println!(
            "context={:?}",
            context_store
                .values
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        );
    }
    Ok(())
}
