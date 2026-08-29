//! WebSocket 批量消息封包与解包工具。

use serde_json::Value;
use uuid::Uuid;

use crate::WsEnvelope;

/// 默认单批最大项目数量。
pub const DEFAULT_MAX_BATCH_SIZE: usize = 5;

/// 将 JSON 项按指定大小拆分为批量信封。
pub fn pack_batch(workflow: &str, items: Vec<Value>, max_batch_size: usize) -> Vec<WsEnvelope> {
    assert!(max_batch_size > 0, "max_batch_size must be greater than 0");
    items
        .chunks(max_batch_size)
        .map(|chunk| {
            WsEnvelope::service(
                workflow,
                "batch",
                None,
                serde_json::json!({
                    "batch_id": Uuid::new_v4().to_string(),
                    "items": chunk,
                }),
            )
        })
        .collect()
}

/// 从批量信封中读取 JSON 项。
pub fn unpack_batch(envelope: &WsEnvelope) -> Result<Vec<Value>, String> {
    if envelope.cmd != "batch" {
        return Err("not a batch message".to_string());
    }
    let items = envelope
        .info
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| "batch items is missing".to_string())?;
    if items.is_empty() {
        return Err("batch items is empty".to_string());
    }
    Ok(items.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_and_unpack_round_trip() {
        let items = vec![Value::from(1), Value::from(2), Value::from(3)];
        let batches = pack_batch("pipeline", items.clone(), 2);
        assert_eq!(batches.len(), 2);
        let unpacked = batches
            .iter()
            .map(unpack_batch)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(unpacked, items);
    }
}
