//! 可选上下文读取节点。

#[cfg(feature = "context-sync")]
mod enabled {
    use async_trait::async_trait;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::context_sync::ContextOps;
    use crate::error::{Result, WorkflowError};
    use crate::node::{BaseNode, NodeConfig, NodeContext, NodeHelper, NodeType};
    use crate::parameter_bus::ParameterType;

    /// 通过宿主注入的 `ContextOps` 读取外部上下文。
    pub struct ContextFetchNode {
        name: String,
        config: NodeConfig,
        input_params: HashMap<String, ParameterType>,
        output_params: HashMap<String, ParameterType>,
        choices: Vec<String>,
    }

    impl ContextFetchNode {
        pub fn new(config: NodeConfig, _ctx: NodeContext) -> Result<Self> {
            let mut output_params = HashMap::new();
            output_params.insert("output".to_string(), ParameterType::Step);
            Ok(Self {
                name: config.node_name.clone(),
                config,
                input_params: HashMap::new(),
                output_params,
                choices: vec!["default".to_string()],
            })
        }
    }

    #[async_trait]
    impl BaseNode for ContextFetchNode {
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
            let workflow = ctx.get_workflow()?;
            let client = workflow
                .get_context_object::<Arc<dyn ContextOps>>("context_client")
                .ok_or_else(|| {
                    WorkflowError::ConfigError("context_client was not injected".to_string())
                })?;
            let namespace = self
                .config
                .attrs
                .get("namespace")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    WorkflowError::ConfigError("context_fetch requires namespace".to_string())
                })?;
            let field = self
                .config
                .attrs
                .get("field")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    WorkflowError::ConfigError("context_fetch requires field".to_string())
                })?;
            let structured = self
                .config
                .attrs
                .get("structured")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let value = if structured {
                client.get_context_structured(namespace, field).await?
            } else {
                Value::String(client.get_context(namespace, field).await?)
            };
            NodeHelper::set_output(&self.name, "output", value, ctx).await?;
            NodeHelper::set_choice(&self.name, "default", &self.config.choice_map, ctx).await?;
            Ok(())
        }
    }
}

#[cfg(feature = "context-sync")]
pub use enabled::ContextFetchNode;
