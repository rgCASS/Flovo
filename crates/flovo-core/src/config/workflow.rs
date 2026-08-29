#[cfg(feature = "context-sync")]
use crate::context_sync::ContextSyncConfig;
use crate::error::{Result, WorkflowError};
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// 统一工作流元数据 + context_sync 配置。
#[derive(Debug, Deserialize, Clone)]
pub struct WorkflowConfig {
    pub workflow: WorkflowMeta,
    #[cfg(feature = "context-sync")]
    #[serde(default)]
    pub context_sync: Option<ContextSyncConfig>,
}

/// 工作流元数据。
#[derive(Debug, Deserialize, Clone)]
pub struct WorkflowMeta {
    pub name: String,
    pub version: String,
}

/// 从统一 JSON 文件加载工作流元数据和 context_sync 配置。
pub fn load_workflow_config(path: &Path) -> Result<WorkflowConfig> {
    let content = fs::read_to_string(path).map_err(|e| {
        WorkflowError::ConfigError(format!(
            "failed to read workflow config '{}': {}",
            path.display(),
            e
        ))
    })?;

    serde_json::from_str::<WorkflowConfig>(&content).map_err(|e| {
        WorkflowError::ConfigError(format!(
            "failed to parse workflow config json '{}': {}",
            path.display(),
            e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "context-sync")]
    #[test]
    fn test_parse_workflow_config_with_context_sync() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("workflow.json");

        let payload = r#"
{
  "workflow": {
    "name": "my_workflow",
    "version": "1.0"
  },
  "context_sync": {
    "enabled": true,
    "fetch_on_start": [
      {
        "namespace": "user:{user_id}:profile",
        "fields": ["language", "timezone"]
      }
    ],
    "push_on_complete": [
      {
        "from_context": "result_summary",
        "to_namespace": "user:{user_id}:session:{session_id}",
        "to_field": "last_result"
      }
    ]
  }
}
"#;
        std::fs::write(&path, payload).expect("write json");

        let config = load_workflow_config(&path).expect("load workflow config");
        assert_eq!(config.workflow.name, "my_workflow");
        assert_eq!(config.workflow.version, "1.0");
        assert!(config.context_sync.as_ref().expect("context_sync").enabled);
    }

    #[test]
    fn test_parse_workflow_config_without_context_sync() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("workflow.json");
        let payload = r#"
{
  "workflow": {
    "name": "my_workflow",
    "version": "1.0"
  }
}
"#;
        std::fs::write(&path, payload).expect("write json");

        let config = load_workflow_config(&path).expect("load workflow config");
        assert_eq!(config.workflow.name, "my_workflow");
        assert_eq!(config.workflow.version, "1.0");
        #[cfg(feature = "context-sync")]
        assert!(config.context_sync.is_none());
    }

    #[test]
    fn test_load_workflow_config_error_when_file_missing() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("missing.json");
        let err = load_workflow_config(&path).expect_err("should fail");
        assert!(err.to_string().contains("failed to read workflow config"));
    }

    #[test]
    fn test_load_workflow_config_error_when_json_invalid() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("invalid.json");
        std::fs::write(&path, "{ invalid json ").expect("write invalid json");

        let err = load_workflow_config(&path).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("failed to parse workflow config json"));
    }
}
