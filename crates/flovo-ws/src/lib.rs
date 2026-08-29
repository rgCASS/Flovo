#![allow(clippy::result_large_err)]

//! Flovo WebSocket 传输层。

pub mod batch;
pub mod batch_ack;
mod server;

pub use batch::{pack_batch, unpack_batch, DEFAULT_MAX_BATCH_SIZE};
pub use batch_ack::BatchAckResult;
pub use server::{
    WsBinaryResource, WsEnvelope, WsImageResource, WsOutboundMessage, WsServer, WsServerConfig,
    WsTextBlock, WsVideoResource, WsWriterSender,
};
