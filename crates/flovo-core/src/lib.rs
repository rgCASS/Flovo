#![allow(clippy::result_large_err)]
#![allow(clippy::should_implement_trait)]

//! Flovo 工作流引擎核心。
//!
//! 该 crate 只包含工作流编排、参数总线、节点抽象和配置解析，
//! 不包含网络服务或具体外部服务客户端。

pub mod builder;
pub mod config;
#[cfg(feature = "context-sync")]
pub mod context_sync;
pub mod error;
pub mod llm;
pub mod node;
pub mod nodes;
pub mod parameter_bus;
pub mod prompt;
pub mod workflow;

pub use builder::WorkflowBuilder;
#[cfg(feature = "context-sync")]
pub use context_sync::{
    ContextOps, ContextSyncConfig, ContextSyncManager, FetchConfig, PushConfig,
};
pub use error::{Result, WorkflowError};
pub use llm::{ChunkStatus, LlmApi, StreamChunk};
pub use parameter_bus::{ParameterBus, ParameterType};
pub use prompt::{PromptBase, PromptFactory, PromptKind};
pub use workflow::{WorkFlow, WorkflowStatus};
