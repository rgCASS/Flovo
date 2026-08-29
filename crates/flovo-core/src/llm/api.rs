//! LLM API 抽象接口
//!
//! 定义与大语言模型交互的通用接口，支持同步聊天和流式聊天两种模式。
//! 对应 Python 版本的 `python_example/llm/llm_interface.py`。

use async_trait::async_trait;

use crate::error::Result;
use crate::prompt::PromptBase;

// ─── 流式输出类型 ─────────────────────────────────────────────────────────────

/// 流式输出状态
///
/// 对应 Python 版本 recall 回调中的 `status` 字段
#[derive(Debug, Clone, PartialEq)]
pub enum ChunkStatus {
    /// 数据片段（中间块，尚未完成）
    Data,
    /// 最后一个片段（流式输出结束）
    Finish,
}

/// 流式聊天的单个数据块
///
/// 对应 Python 版本中传给 recall 的字典：`{'status': '...', 'content': '...'}`
#[derive(Debug, Clone)]
pub struct StreamChunk {
    /// 该 chunk 的状态（Data 或 Finish）
    pub status: ChunkStatus,
    /// 该 chunk 的文本内容
    pub content: String,
}

impl StreamChunk {
    /// 创建一个数据块
    pub fn data(content: impl Into<String>) -> Self {
        Self {
            status: ChunkStatus::Data,
            content: content.into(),
        }
    }

    /// 创建一个结束块
    pub fn finish(content: impl Into<String>) -> Self {
        Self {
            status: ChunkStatus::Finish,
            content: content.into(),
        }
    }

    /// 判断是否为结束块
    pub fn is_finish(&self) -> bool {
        self.status == ChunkStatus::Finish
    }
}

// ─── LlmApi trait ─────────────────────────────────────────────────────────────

/// LLM API 抽象接口
///
/// 所有大语言模型接入层均需实现此 trait，以确保可互换性。
/// 对应 Python 版本的 `LLM_INTERFACE` 基类。
///
/// # 示例
///
/// 具体实现由宿主应用注入，核心 crate 不绑定任何模型供应商。
#[async_trait]
pub trait LlmApi: Send + Sync {
    /// 同步聊天：发送 prompt，等待完整响应后返回。
    ///
    /// # 参数
    /// - `prompt`: 渲染好的 Prompt 文本（来自 `PromptBase::content`）
    ///
    /// # 返回
    /// - `Ok(String)`: LLM 返回的完整响应文本
    /// - `Err(WorkflowError)`: 网络错误或 API 返回错误
    async fn chat(&self, prompt: String) -> Result<String>;

    /// 流式聊天：发送 prompt，通过回调逐块输出响应内容。
    ///
    /// # 参数
    /// - `prompt`: 渲染好的 Prompt 文本
    /// - `callback`: 每当有新的 chunk 到达时调用，参数为 `StreamChunk`；
    ///               当 `chunk.is_finish()` 为 true 时，表示流式输出结束
    ///
    /// # 返回
    /// - `Ok(())`: 流式输出完成
    /// - `Err(WorkflowError)`: 网络错误或 API 返回错误
    async fn chat_stream(
        &self,
        prompt: String,
        callback: Box<dyn Fn(StreamChunk) + Send + Sync>,
    ) -> Result<()>;

    /// 便捷方法：直接接受 `PromptBase` 进行同步聊天
    ///
    /// # 参数
    /// - `prompt`: 已渲染的 Prompt 对象（来自 `PromptFactory`）
    async fn chat_prompt(&self, prompt: &PromptBase) -> Result<String> {
        self.chat(prompt.content.clone()).await
    }

    /// 便捷方法：直接接受 `PromptBase` 进行流式聊天
    ///
    /// # 参数
    /// - `prompt`: 已渲染的 Prompt 对象
    /// - `callback`: 流式输出回调
    async fn chat_stream_prompt(
        &self,
        prompt: &PromptBase,
        callback: Box<dyn Fn(StreamChunk) + Send + Sync>,
    ) -> Result<()> {
        self.chat_stream(prompt.content.clone(), callback).await
    }
}
