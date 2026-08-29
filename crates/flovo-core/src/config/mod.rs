//! JSON 工作流配置加载与校验。

pub mod loader;
pub mod schema;
pub mod validator;
pub mod workflow;

pub use loader::ConfigLoader;
pub use schema::{load_workflow_configs, NodeConfigJson, WorkflowConfig};
pub use validator::{validate_config, ValidationError, Validator};
pub use workflow::{load_workflow_config, WorkflowConfig as UnifiedWorkflowConfig, WorkflowMeta};
