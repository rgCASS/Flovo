//! 可选的外部上下文同步抽象。

use crate::error::{Result, WorkflowError};
use crate::workflow::WorkFlow;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// 上下文同步总开关与规则。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContextSyncConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub fetch_on_start: Vec<FetchConfig>,
    #[serde(default)]
    pub push_on_complete: Vec<PushConfig>,
}

/// 工作流启动前的读取规则。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchConfig {
    pub namespace: String,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub structured: bool,
}

/// 工作流完成后的写回规则。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushConfig {
    pub from_context: String,
    pub to_namespace: String,
    pub to_field: String,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub structured: bool,
}

/// 外部上下文操作抽象，由宿主应用提供具体实现。
#[async_trait]
pub trait ContextOps: Send + Sync {
    async fn get_context(&self, namespace: &str, field: &str) -> Result<String>;
    async fn set_context(&self, namespace: &str, field: &str, value: &str) -> Result<()>;

    async fn get_context_structured(&self, namespace: &str, field: &str) -> Result<Value> {
        let value = self.get_context(namespace, field).await?;
        serde_json::from_str(&value).map_err(|error| {
            WorkflowError::Other(format!("structured context parse failed: {error}"))
        })
    }

    async fn set_context_with_alias(
        &self,
        namespace: &str,
        field: &str,
        value: &str,
        alias: Option<&str>,
    ) -> Result<()> {
        let _ = alias;
        self.set_context(namespace, field, value).await
    }

    async fn set_context_structured(
        &self,
        namespace: &str,
        field: &str,
        value: &Value,
        alias: Option<&str>,
    ) -> Result<()> {
        self.set_context_with_alias(namespace, field, &value.to_string(), alias)
            .await
    }
}

/// 工作流上下文与外部实现之间的同步管理器。
#[derive(Clone)]
pub struct ContextSyncManager {
    client: Arc<dyn ContextOps>,
    config: ContextSyncConfig,
}

impl ContextSyncManager {
    /// 使用调用方提供的客户端创建同步管理器。
    pub fn new(client: impl ContextOps + 'static, config: ContextSyncConfig) -> Self {
        Self {
            client: Arc::new(client),
            config,
        }
    }

    /// 使用已构造好的客户端对象创建同步管理器（常用于测试）。
    pub fn new_with_client(client: Arc<dyn ContextOps>, config: ContextSyncConfig) -> Self {
        Self { client, config }
    }

    /// 在工作流执行前拉取外部上下文；失败仅告警，不阻塞流程。
    pub async fn fetch_on_start(&self, workflow: &WorkFlow, user_id: &str, session_id: &str) {
        if !self.config.enabled {
            return;
        }

        for fetch in &self.config.fetch_on_start {
            let namespace = Self::interpolate(&fetch.namespace, user_id, session_id);
            for field in &fetch.fields {
                if fetch.structured {
                    match self.client.get_context_structured(&namespace, field).await {
                        Ok(value) => workflow.set_context(field, value),
                        Err(err) => {
                            tracing::warn!(
                                "fetch structured context failed: namespace={}, field={}, error={}",
                                namespace,
                                field,
                                err
                            );
                        }
                    }
                } else {
                    match self.client.get_context(&namespace, field).await {
                        Ok(value) => workflow.set_context(field, Value::String(value)),
                        Err(err) => {
                            tracing::warn!(
                                "fetch context failed: namespace={}, field={}, error={}",
                                namespace,
                                field,
                                err
                            );
                        }
                    }
                }
            }
        }
    }

    /// 在工作流执行完成后回写上下文；失败返回错误。
    pub async fn push_on_complete(
        &self,
        workflow: &WorkFlow,
        user_id: &str,
        session_id: &str,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        for push in &self.config.push_on_complete {
            let namespace = Self::interpolate(&push.to_namespace, user_id, session_id);
            // 工作流未产出该字段（如 receive_schema 校验失败路径）时跳过回写，
            // 避免以 Null 空值调用上下文服务导致回写失败。
            let Some(value) = workflow.get_context(&push.from_context) else {
                continue;
            };

            if push.structured {
                self.client
                    .set_context_structured(
                        &namespace,
                        &push.to_field,
                        &value,
                        push.alias.as_deref(),
                    )
                    .await?;
            } else {
                let value = value.to_string();
                self.client
                    .set_context_with_alias(
                        &namespace,
                        &push.to_field,
                        &value,
                        push.alias.as_deref(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// 替换命名空间模板中的 `{user_id}` 和 `{session_id}` 占位符。
    pub fn interpolate(raw: &str, user_id: &str, session_id: &str) -> String {
        raw.replace("{user_id}", user_id)
            .replace("{session_id}", session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{FetchConfig, PushConfig};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    struct MockContextClient {
        data: Mutex<HashMap<(String, String), String>>,
        aliases: Mutex<HashMap<(String, String), String>>,
        set_structured_calls: Mutex<Vec<(String, String, String)>>,
        fail_get: bool,
        fail_set: bool,
    }

    impl MockContextClient {
        fn new(fail_get: bool, fail_set: bool) -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
                aliases: Mutex::new(HashMap::new()),
                set_structured_calls: Mutex::new(Vec::new()),
                fail_get,
                fail_set,
            }
        }
    }

    #[async_trait]
    impl ContextOps for MockContextClient {
        async fn get_context(&self, namespace: &str, field: &str) -> Result<String> {
            if self.fail_get {
                return Err(WorkflowError::Other("mock get error".to_string()));
            }
            Ok(self
                .data
                .lock()
                .get(&(namespace.to_string(), field.to_string()))
                .cloned()
                .unwrap_or_default())
        }

        async fn set_context(&self, namespace: &str, field: &str, value: &str) -> Result<()> {
            if self.fail_set {
                return Err(WorkflowError::Other("mock set error".to_string()));
            }
            self.data.lock().insert(
                (namespace.to_string(), field.to_string()),
                value.to_string(),
            );
            Ok(())
        }

        async fn get_context_structured(&self, namespace: &str, field: &str) -> Result<Value> {
            if self.fail_get {
                return Err(WorkflowError::Other("mock get error".to_string()));
            }

            let value = self
                .data
                .lock()
                .get(&(namespace.to_string(), field.to_string()))
                .cloned()
                .unwrap_or_default();

            Ok(serde_json::from_str(&value).unwrap_or(Value::String(value)))
        }

        async fn set_context_with_alias(
            &self,
            namespace: &str,
            field: &str,
            value: &str,
            alias: Option<&str>,
        ) -> Result<()> {
            self.set_context(namespace, field, value).await?;
            if let Some(alias) = alias {
                self.aliases.lock().insert(
                    (namespace.to_string(), field.to_string()),
                    alias.to_string(),
                );
            }
            Ok(())
        }

        async fn set_context_structured(
            &self,
            namespace: &str,
            field: &str,
            value: &Value,
            alias: Option<&str>,
        ) -> Result<()> {
            let value = value.to_string();
            self.set_structured_calls.lock().push((
                namespace.to_string(),
                field.to_string(),
                value.clone(),
            ));
            self.set_context_with_alias(namespace, field, &value, alias)
                .await
        }
    }

    #[tokio::test]
    async fn test_fetch_on_start() {
        let workflow = WorkFlow::new("ctx_fetch_test".to_string());
        let client = Arc::new(MockContextClient::new(false, false));
        client.data.lock().insert(
            ("user:u1:session:s1".to_string(), "profile".to_string()),
            "runner".to_string(),
        );

        let manager = ContextSyncManager::new_with_client(
            client,
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: vec![FetchConfig {
                    namespace: "user:{user_id}:session:{session_id}".to_string(),
                    fields: vec!["profile".to_string()],
                    structured: false,
                }],
                push_on_complete: Vec::new(),
            },
        );

        manager.fetch_on_start(&workflow, "u1", "s1").await;
        assert_eq!(workflow.get_context("profile"), Some(Value::from("runner")));
    }

    #[tokio::test]
    async fn test_fetch_structured() {
        let workflow = WorkFlow::new("ctx_fetch_structured_test".to_string());
        let client = Arc::new(MockContextClient::new(false, false));
        client.data.lock().insert(
            ("user:u1:session:s1".to_string(), "profile".to_string()),
            r#"{"name":"runner","score":7}"#.to_string(),
        );

        let manager = ContextSyncManager::new_with_client(
            client,
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: vec![FetchConfig {
                    namespace: "user:{user_id}:session:{session_id}".to_string(),
                    fields: vec!["profile".to_string()],
                    structured: true,
                }],
                push_on_complete: Vec::new(),
            },
        );

        manager.fetch_on_start(&workflow, "u1", "s1").await;

        assert_eq!(workflow.get_context("profile.name"), None);
        assert_eq!(
            workflow.get_context("profile"),
            Some(serde_json::json!({"name":"runner","score":7}))
        );
    }

    #[tokio::test]
    async fn test_push_on_complete() {
        let workflow = WorkFlow::new("ctx_push_test".to_string());
        workflow.set_context("summary", Value::from("done"));

        let client = Arc::new(MockContextClient::new(false, false));
        let manager = ContextSyncManager::new_with_client(
            client.clone(),
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: Vec::new(),
                push_on_complete: vec![PushConfig {
                    from_context: "summary".to_string(),
                    to_namespace: "user:{user_id}:session:{session_id}".to_string(),
                    to_field: "summary".to_string(),
                    alias: None,
                    structured: false,
                }],
            },
        );

        manager
            .push_on_complete(&workflow, "u1", "s1")
            .await
            .expect("push success");

        assert_eq!(
            client
                .data
                .lock()
                .get(&("user:u1:session:s1".to_string(), "summary".to_string()))
                .cloned(),
            Some("\"done\"".to_string())
        );
    }

    #[tokio::test]
    async fn test_push_with_alias() {
        let workflow = WorkFlow::new("ctx_push_alias_test".to_string());
        workflow.set_context("summary", Value::from("done"));

        let client = Arc::new(MockContextClient::new(false, false));
        let manager = ContextSyncManager::new_with_client(
            client.clone(),
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: Vec::new(),
                push_on_complete: vec![PushConfig {
                    from_context: "summary".to_string(),
                    to_namespace: "user:{user_id}:session:{session_id}".to_string(),
                    to_field: "summary".to_string(),
                    alias: Some("session_summary".to_string()),
                    structured: false,
                }],
            },
        );

        manager
            .push_on_complete(&workflow, "u1", "s1")
            .await
            .expect("push success");

        assert_eq!(
            client
                .aliases
                .lock()
                .get(&("user:u1:session:s1".to_string(), "summary".to_string()))
                .cloned(),
            Some("session_summary".to_string())
        );
    }

    #[tokio::test]
    async fn test_push_without_alias() {
        let workflow = WorkFlow::new("ctx_push_without_alias_test".to_string());
        workflow.set_context("summary", Value::from("done"));

        let client = Arc::new(MockContextClient::new(false, false));
        let manager = ContextSyncManager::new_with_client(
            client.clone(),
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: Vec::new(),
                push_on_complete: vec![PushConfig {
                    from_context: "summary".to_string(),
                    to_namespace: "user:{user_id}:session:{session_id}".to_string(),
                    to_field: "summary".to_string(),
                    alias: None,
                    structured: false,
                }],
            },
        );

        manager
            .push_on_complete(&workflow, "u1", "s1")
            .await
            .expect("push success");

        assert_eq!(
            client
                .data
                .lock()
                .get(&("user:u1:session:s1".to_string(), "summary".to_string()))
                .cloned(),
            Some("\"done\"".to_string())
        );
        assert!(client.aliases.lock().is_empty());
    }

    #[tokio::test]
    async fn test_push_structured() {
        let workflow = WorkFlow::new("ctx_push_structured_test".to_string());
        workflow.set_context(
            "profile",
            serde_json::json!({"score": 7, "tags": ["beginner"]}),
        );

        let client = Arc::new(MockContextClient::new(false, false));
        let manager = ContextSyncManager::new_with_client(
            client.clone(),
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: Vec::new(),
                push_on_complete: vec![PushConfig {
                    from_context: "profile".to_string(),
                    to_namespace: "user:{user_id}:session:{session_id}".to_string(),
                    to_field: "profile".to_string(),
                    alias: None,
                    structured: true,
                }],
            },
        );

        manager
            .push_on_complete(&workflow, "u1", "s1")
            .await
            .expect("push structured success");

        assert_eq!(
            client.set_structured_calls.lock().as_slice(),
            [(
                "user:u1:session:s1".to_string(),
                "profile".to_string(),
                r#"{"score":7,"tags":["beginner"]}"#.to_string()
            )]
        );
        assert_eq!(
            client
                .data
                .lock()
                .get(&("user:u1:session:s1".to_string(), "profile".to_string()))
                .cloned(),
            Some(r#"{"score":7,"tags":["beginner"]}"#.to_string())
        );
    }

    #[tokio::test]
    async fn test_push_on_complete_error() {
        let workflow = WorkFlow::new("ctx_push_error".to_string());
        workflow.set_context("summary", Value::from("done"));

        let client = Arc::new(MockContextClient::new(false, true));
        let manager = ContextSyncManager::new_with_client(
            client,
            ContextSyncConfig {
                enabled: true,
                fetch_on_start: Vec::new(),
                push_on_complete: vec![PushConfig {
                    from_context: "summary".to_string(),
                    to_namespace: "user:{user_id}:session:{session_id}".to_string(),
                    to_field: "summary".to_string(),
                    alias: None,
                    structured: false,
                }],
            },
        );

        let result = manager.push_on_complete(&workflow, "u1", "s1").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_interpolate() {
        let result =
            ContextSyncManager::interpolate("user:{user_id}:session:{session_id}", "u123", "s456");
        assert_eq!(result, "user:u123:session:s456");
    }
}
