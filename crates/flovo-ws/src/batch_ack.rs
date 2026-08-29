//! WebSocket `batch_ack` 确认结果类型。
//!
//! 该模块只定义批次确认结果的数据结构，等待与路由由传输层调用方管理。

/// 客户端对批量消息的确认结果。
///
/// # 字段
/// - `status`：确认状态，当前约定为 `"ok"` / `"error"` / `"timeout"`。
/// - `batch_id`：被确认的批次 ID。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAckResult {
    pub status: String,
    pub batch_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_ack_result_fields() {
        let result = BatchAckResult {
            status: "ok".to_string(),
            batch_id: "batch-001".to_string(),
        };

        assert_eq!(result.status, "ok");
        assert_eq!(result.batch_id, "batch-001");
    }
}
