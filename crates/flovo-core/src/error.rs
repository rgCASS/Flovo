use thiserror::Error;

/// 工作流引擎的错误类型
#[derive(Error, Debug)]
pub enum WorkflowError {
    /// 节点不存在
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    /// 参数不存在
    #[error("Parameter not found: {0}")]
    ParameterNotFound(String),

    /// 配置错误
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// 节点类型未注册
    #[error("Node type not registered: {0}")]
    NodeTypeNotRegistered(String),

    /// 工作流状态错误
    #[error("Invalid workflow state: {0}")]
    InvalidState(String),

    /// 节点执行错误
    #[error("Node execution error in '{node}': {message}")]
    NodeExecutionError { node: String, message: String },

    /// JSON 解析错误
    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// IO 错误
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// 通用错误
    #[error("{0}")]
    Other(String),
}

/// 工作流引擎的 Result 类型
pub type Result<T> = std::result::Result<T, WorkflowError>;
