//! 通用内置节点。

pub mod condition_node;
#[cfg(feature = "context-sync")]
pub mod context_fetch_node;
pub mod print_node;
pub mod schema_validator;
pub mod send_recv_node;
pub mod transform_node;
pub mod while_node;

pub use condition_node::ConditionNode;
#[cfg(feature = "context-sync")]
pub use context_fetch_node::ContextFetchNode;
pub use print_node::PrintNode;
pub use schema_validator::SchemaValidatorNode;
pub use send_recv_node::{OutboundSender, SendRecvNode};
pub use transform_node::TransformNode;
pub use while_node::WhileNode;

use crate::node::NodeRegistry;
use std::sync::Arc;

/// 注册当前 feature 下可用的通用节点。
pub fn register_builtin_nodes(registry: &Arc<NodeRegistry>) {
    registry.register("print_node", |config, ctx| {
        Ok(Arc::new(PrintNode::new(config, ctx)?))
    });
    registry.register("transform_node", |config, ctx| {
        Ok(Arc::new(TransformNode::new(config, ctx)?))
    });
    registry.register("condition_node", |config, ctx| {
        Ok(Arc::new(ConditionNode::new(config, ctx)?))
    });
    registry.register("while_node", |config, ctx| {
        Ok(Arc::new(WhileNode::new(config, ctx)?))
    });
    registry.register("send_recv_node", |config, ctx| {
        Ok(Arc::new(SendRecvNode::new(config, ctx)?))
    });
    registry.register("send_cmd_recv", |config, ctx| {
        Ok(Arc::new(SendRecvNode::new(config, ctx)?))
    });
    registry.register("schema_validator", |config, ctx| {
        Ok(Arc::new(SchemaValidatorNode::new(config, ctx)?))
    });
    #[cfg(feature = "context-sync")]
    registry.register("context_fetch_node", |config, ctx| {
        Ok(Arc::new(ContextFetchNode::new(config, ctx)?))
    });
}
