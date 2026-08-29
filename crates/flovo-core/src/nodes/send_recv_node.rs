//! 通用消息收发节点。
//!
//! 节点从工作流消息队列接收 JSON 值，并可将输入写入宿主注入的输出通道。

use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::error::{Result, WorkflowError};
use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
use crate::parameter_bus::ParameterType;

/// 宿主侧输出通道。网络层可将收到的 JSON 值编码为自己的传输协议。
pub type OutboundSender = mpsc::UnboundedSender<Value>;

/// 在工作流消息队列和参数总线之间转发 JSON 数据。
pub struct SendRecvNode {
    name: String,
    config: NodeConfig,
    input_params: HashMap<String, ParameterType>,
    output_params: HashMap<String, ParameterType>,
    choices: Vec<String>,
}

impl SendRecvNode {
    /// 创建节点。`mode` 支持 `recv`、`send` 和 `send_recv`，默认 `recv`。
    pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
        let mut input_params = HashMap::new();
        input_params.insert("input".to_string(), ParameterType::Step);
        let mut output_params = HashMap::new();
        output_params.insert("output".to_string(), ParameterType::Step);
        Ok(Self {
            name: config.node_name.clone(),
            config,
            input_params,
            output_params,
            choices: vec!["default".to_string()],
        })
    }

    fn mode(&self) -> &str {
        self.config
            .attrs
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("recv")
    }
}

#[async_trait]
impl BaseNode for SendRecvNode {
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
        let workflow = ctx.get_workflow()?;
        let value = match self.mode() {
            "recv" => workflow.get_message().await?,
            "send" | "send_recv" => {
                let value = NodeHelper::get_input("input", &self.config.input_map, ctx)
                    .await?
                    .unwrap_or(Value::Null);
                if let Some(sender) =
                    workflow.get_context_object::<OutboundSender>("outbound_sender")
                {
                    sender.send(value.clone()).map_err(|error| {
                        WorkflowError::Other(format!("outbound channel closed: {error}"))
                    })?;
                }
                if self.mode() == "send_recv" {
                    workflow.get_message().await?
                } else {
                    value
                }
            }
            mode => {
                return Err(WorkflowError::ConfigError(format!(
                    "unsupported send_recv mode: {mode}"
                )))
            }
        };

        NodeHelper::set_output(&self.name, "output", value, ctx).await?;
        NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;
        Ok(())
    }
}
