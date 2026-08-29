#[cfg(feature = "context-sync")]
use crate::context_sync::ContextSyncConfig;
/// JSON 配置结构体
///
/// 实现与 Python 版本兼容的 JSON 配置结构和加载功能
use crate::error::{Result, WorkflowError};
use crate::node::NodeConfig;
use crate::parameter_bus::ParameterType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// 工作流配置
///
/// 对应 Python 版本的 JSON 配置格式
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WorkflowConfig {
    /// 起始节点名称
    pub start_node: String,

    /// 是否在启动时监听
    #[serde(default)]
    pub listen_at_start: Option<bool>,

    /// 输入参数定义 (参数名 -> 参数类型)
    pub input_parameters: HashMap<String, String>,

    /// 节点配置列表
    pub nodes: Vec<NodeConfigJson>,

    /// 可选上下文同步配置（用于工作流执行前后与外部上下文服务同步）。
    #[cfg(feature = "context-sync")]
    #[serde(default)]
    pub context_sync: Option<ContextSyncConfig>,
}

/// 节点配置 (JSON 格式)
///
/// 对应 Python 版本的节点配置
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeConfigJson {
    /// 节点 ID
    pub id: u32,

    /// 节点类型
    pub node_type: String,

    /// 节点名称
    pub node_name: String,

    /// 输入参数映射 (参数名 -> 来源参数键)
    pub input_map: HashMap<String, Option<String>>,

    /// 选择映射 (选择名 -> 目标节点名)
    pub choice_map: HashMap<String, String>,

    /// 节点属性
    pub attrs: HashMap<String, serde_json::Value>,

    /// 是否为关键节点
    #[serde(default)]
    pub key_node: bool,
}

impl NodeConfigJson {
    /// 转换为 NodeConfig
    pub fn to_node_config(&self) -> NodeConfig {
        NodeConfig {
            id: self.id,
            node_name: self.node_name.clone(),
            input_map: self.input_map.clone(),
            choice_map: self.choice_map.clone(),
            attrs: self.attrs.clone(),
            key_node: self.key_node,
        }
    }
}

impl ParameterType {
    /// 从字符串解析参数类型
    ///
    /// # 参数
    /// - s: 参数类型字符串 ("step" 或 "stream")
    ///
    /// # 返回
    /// 解析成功返回 ParameterType，失败返回错误
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "step" => Ok(ParameterType::Step),
            "stream" => Ok(ParameterType::Stream),
            _ => Err(WorkflowError::ConfigError(format!(
                "Invalid parameter type: {}. Expected 'step' or 'stream'",
                s
            ))),
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            ParameterType::Step => "step",
            ParameterType::Stream => "stream",
        }
    }
}

/// 从目录加载所有工作流配置（不进行验证）
///
/// # 参数
/// - dir_path: 配置文件目录路径
///
/// # 返回
/// 返回工作流名称到配置的映射
///
/// # 错误
/// - 目录不存在
/// - JSON 解析错误
/// - 工作流名称重复
///
/// # 注意
/// 此函数不进行配置验证。如需验证，请使用 `validate_config` 或在构建工作流时自动验证。
pub fn load_workflow_configs(dir_path: &str) -> Result<HashMap<String, WorkflowConfig>> {
    let path = Path::new(dir_path);

    // 检查目录是否存在
    if !path.exists() {
        return Err(WorkflowError::ConfigError(format!(
            "Configuration directory does not exist: {}",
            dir_path
        )));
    }

    if !path.is_dir() {
        return Err(WorkflowError::ConfigError(format!(
            "Path is not a directory: {}",
            dir_path
        )));
    }

    let mut all_configs = HashMap::new();

    // 遍历目录中的所有 JSON 文件
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();

        // 只处理 .json 文件
        if file_path.extension().and_then(|s| s.to_str()) == Some("json") {
            let file_name = file_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            // 读取文件内容
            let content = fs::read_to_string(&file_path).map_err(|e| {
                WorkflowError::ConfigError(format!("Failed to read file {}: {}", file_name, e))
            })?;

            // 解析 JSON
            let configs: HashMap<String, WorkflowConfig> =
                serde_json::from_str(&content).map_err(|e| {
                    WorkflowError::ConfigError(format!(
                        "Failed to parse JSON in file {}: {}",
                        file_name, e
                    ))
                })?;

            // 检查名称重复
            for (name, config) in configs {
                if all_configs.contains_key(&name) {
                    return Err(WorkflowError::ConfigError(format!(
                        "Duplicate workflow name '{}' found in file {}",
                        name, file_name
                    )));
                }
                all_configs.insert(name, config);
            }
        }
    }

    Ok(all_configs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parameter_type_conversion() {
        assert_eq!(
            ParameterType::from_str("step").unwrap(),
            ParameterType::Step
        );
        assert_eq!(
            ParameterType::from_str("stream").unwrap(),
            ParameterType::Stream
        );
        assert_eq!(
            ParameterType::from_str("STEP").unwrap(),
            ParameterType::Step
        );

        assert!(ParameterType::from_str("invalid").is_err());
    }

    #[test]
    fn test_parameter_type_as_str() {
        assert_eq!(ParameterType::Step.as_str(), "step");
        assert_eq!(ParameterType::Stream.as_str(), "stream");
    }

    #[test]
    fn test_node_config_conversion() {
        let json_config = NodeConfigJson {
            id: 1,
            node_type: "test_node".to_string(),
            node_name: "node1".to_string(),
            input_map: HashMap::new(),
            choice_map: HashMap::new(),
            attrs: HashMap::new(),
            key_node: true,
        };

        let node_config = json_config.to_node_config();
        assert_eq!(node_config.id, 1);
        assert_eq!(node_config.node_name, "node1");
        assert!(node_config.key_node);
    }

    #[cfg(feature = "context-sync")]
    #[test]
    fn test_load_workflow_configs_with_context_sync() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("ctx.json");
        let payload = r#"
{
  "ctx_demo": {
    "context_sync": {
      "enabled": true,
      "fetch_on_start": [
        {
          "namespace": "user:{user_id}:profile",
          "fields": ["username"]
        }
      ],
      "push_on_complete": [
        {
          "from_context": "workflow_result",
          "to_namespace": "user:{user_id}:session:{session_id}",
          "to_field": "last_workflow_result"
        }
      ]
    },
    "start_node": "print_ctx",
    "listen_at_start": false,
    "input_parameters": {},
    "nodes": [
      {
        "id": 1,
        "node_type": "print_node",
        "node_name": "print_ctx",
        "input_map": {"input": null},
        "choice_map": {"default": "finish"},
        "attrs": {},
        "key_node": false
      }
    ]
  }
}
"#;
        std::fs::write(&path, payload).expect("write json");

        let configs = load_workflow_configs(dir.path().to_str().expect("dir path to str"))
            .expect("load workflow configs");
        let config = configs.get("ctx_demo").expect("ctx_demo config");
        let context_sync = config
            .context_sync
            .as_ref()
            .expect("context_sync should exist");
        assert!(context_sync.enabled);
        assert_eq!(context_sync.fetch_on_start.len(), 1);
        assert_eq!(context_sync.push_on_complete.len(), 1);
    }
}
