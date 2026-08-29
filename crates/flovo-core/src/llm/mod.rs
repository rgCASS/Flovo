//! 与具体供应商无关的语言模型接口。

mod api;

pub use api::{ChunkStatus, LlmApi, StreamChunk};
