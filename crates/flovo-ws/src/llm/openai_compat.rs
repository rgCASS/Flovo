//! OpenAI 兼容 Chat Completions 客户端。

use flovo_core::{async_trait, LlmApi, Result, StreamChunk, WorkflowError};
use futures_util::StreamExt;
use reqwest::header::AUTHORIZATION;
use serde_json::{json, Value};
use std::time::Duration;

/// 使用 OpenAI Chat Completions 协议的 LLM 实现。
pub struct OpenaiCompatLlm {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl OpenaiCompatLlm {
    /// 从环境变量构造客户端；未配置 API key 时返回 None。
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("FLOVO_LLM_API_KEY").ok()?;
        if api_key.trim().is_empty() {
            return None;
        }
        Some(Self::new(
            std::env::var("FLOVO_LLM_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            api_key,
            std::env::var("FLOVO_LLM_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string()),
        ))
    }

    /// 使用显式配置构造客户端，便于测试和自托管兼容服务接入。
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(60))
            .timeout(Duration::from_secs(300))
            .build()
            .unwrap_or_else(|error| {
                tracing::warn!("failed to configure LLM HTTP client, using defaults: {error}");
                reqwest::Client::new()
            });
        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.api_key)
    }

    fn error_message(&self, message: impl Into<String>) -> WorkflowError {
        let message = message.into();
        let sanitized = if self.api_key.is_empty() {
            message
        } else {
            message.replace(&self.api_key, "[redacted]")
        };
        WorkflowError::Other(sanitized)
    }

    async fn response_error(&self, response: reqwest::Response) -> WorkflowError {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|error| error.to_string());
        let snippet = body.chars().take(512).collect::<String>();
        self.error_message(format!("LLM API returned HTTP {status}: {snippet}"))
    }

    fn parse_chat_content(&self, body: &Value) -> Result<String> {
        body.get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                self.error_message("LLM API response missing choices[0].message.content")
            })
    }
}

#[async_trait]
impl LlmApi for OpenaiCompatLlm {
    async fn chat(&self, prompt: String) -> Result<String> {
        let response = self
            .client
            .post(self.endpoint())
            .header(AUTHORIZATION, self.auth_header())
            .json(&json!({
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": false,
            }))
            .send()
            .await
            .map_err(|error| self.error_message(format!("LLM request failed: {error}")))?;
        if !response.status().is_success() {
            return Err(self.response_error(response).await);
        }
        let status = response.status();
        let raw_body = response.text().await.map_err(|error| {
            self.error_message(format!(
                "failed to read HTTP {status} LLM response: {error}"
            ))
        })?;
        let body = serde_json::from_str::<Value>(&raw_body).map_err(|error| {
            let snippet = raw_body.chars().take(512).collect::<String>();
            self.error_message(format!(
                "failed to parse HTTP {status} LLM response: {error}; body: {snippet}"
            ))
        })?;
        self.parse_chat_content(&body)
    }

    async fn chat_stream(
        &self,
        prompt: String,
        callback: Box<dyn Fn(StreamChunk) + Send + Sync>,
    ) -> Result<()> {
        let response = self
            .client
            .post(self.endpoint())
            .header(AUTHORIZATION, self.auth_header())
            .json(&json!({
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": true,
            }))
            .send()
            .await
            .map_err(|error| self.error_message(format!("LLM stream request failed: {error}")))?;
        if !response.status().is_success() {
            return Err(self.response_error(response).await);
        }

        let mut bytes = response.bytes_stream();
        let mut pending = Vec::new();
        let mut last_delta = String::new();
        let mut finished = false;
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk
                .map_err(|error| self.error_message(format!("LLM stream read failed: {error}")))?;
            pending.extend_from_slice(&chunk);
            while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                let line = pending.drain(..=newline).collect::<Vec<_>>();
                let line = String::from_utf8_lossy(&line);
                let line = line.trim_end_matches(['\r', '\n']);
                if process_sse_line(line, &callback, &mut last_delta) {
                    finished = true;
                    break;
                }
            }
            if finished {
                break;
            }
        }
        if !finished && !pending.is_empty() {
            let line = String::from_utf8_lossy(&pending);
            process_sse_line(
                line.trim_end_matches(['\r', '\n']),
                &callback,
                &mut last_delta,
            );
        }
        callback(StreamChunk::finish(last_delta));
        Ok(())
    }
}

/// 解析一行 SSE；返回 true 表示流应结束。
fn process_sse_line(line: &str, callback: &dyn Fn(StreamChunk), last_delta: &mut String) -> bool {
    let Some(payload) = line.strip_prefix("data:").map(str::trim) else {
        return false;
    };
    if payload.is_empty() {
        return false;
    }
    if payload == "[DONE]" {
        return true;
    }
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        tracing::debug!("skipping malformed LLM SSE line");
        return false;
    };
    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|v| v.first());
    if let Some(content) = choice
        .and_then(|choice| choice.get("delta"))
        .and_then(|delta| delta.get("content"))
        .and_then(Value::as_str)
        .filter(|content| !content.is_empty())
    {
        *last_delta = content.to_string();
        callback(StreamChunk::data(content));
    }
    choice
        .and_then(|choice| choice.get("finish_reason"))
        .map(|reason| !reason.is_null())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flovo_core::ChunkStatus;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    async fn mock_server(body: String, content_type: &str) -> (String, Arc<Mutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request = Arc::new(Mutex::new(String::new()));
        let request_ref = Arc::clone(&request);
        let content_type = content_type.to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let count = stream.read(&mut chunk).await.unwrap();
                if count == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..count]);
                if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers_end = position + 4;
                    let headers = String::from_utf8_lossy(&buffer[..headers_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::to_string)
                        })
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    while buffer.len() < headers_end + content_length {
                        let count = stream.read(&mut chunk).await.unwrap();
                        if count == 0 {
                            break;
                        }
                        buffer.extend_from_slice(&chunk[..count]);
                    }
                    break;
                }
            }
            *request_ref.lock().unwrap() = String::from_utf8_lossy(&buffer).into_owned();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        (format!("http://{address}/v1"), request)
    }

    #[tokio::test]
    async fn chat_posts_openai_compatible_request() {
        let (base_url, request) = mock_server(
            r#"{"choices":[{"message":{"content":"hello"}}]}"#.to_string(),
            "application/json",
        )
        .await;
        let llm = OpenaiCompatLlm::new(base_url, "test-key", "test-model");
        assert_eq!(llm.chat("ping".into()).await.unwrap(), "hello");
        let request = request.lock().unwrap().clone();
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer test-key"));
        assert!(request.contains("\"stream\":false"));
    }

    #[tokio::test]
    async fn chat_stream_emits_data_then_finish() {
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"b\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let (base_url, _) = mock_server(body.to_string(), "text/event-stream").await;
        let llm = OpenaiCompatLlm::new(base_url, "test-key", "test-model");
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let chunks_ref = Arc::clone(&chunks);
        llm.chat_stream(
            "ping".into(),
            Box::new(move |chunk| {
                chunks_ref
                    .lock()
                    .unwrap()
                    .push((chunk.status, chunk.content))
            }),
        )
        .await
        .unwrap();
        let chunks = chunks.lock().unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].1, "a");
        assert_eq!(chunks[1].1, "b");
        assert_eq!(chunks[2].0, ChunkStatus::Finish);
    }

    #[test]
    fn from_env_requires_api_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("FLOVO_LLM_API_KEY");
        assert!(OpenaiCompatLlm::from_env().is_none());
        std::env::set_var("FLOVO_LLM_API_KEY", " ");
        assert!(OpenaiCompatLlm::from_env().is_none());
        std::env::remove_var("FLOVO_LLM_API_KEY");
    }
}
