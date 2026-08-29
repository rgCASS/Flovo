# Flovo 项目上下文

项目的上下文主要 CLAUDE.md 中。

## 当前状态

- FL-01：仓库骨架初始化，已完成。
- FL-02：核心 crate 提取与业务剥离，已完成开发和本地验收。
- 当前功能分支：`feature/flovo-01-core-extract`，基于 `dev` 的 `3889ce3`。
- Rust edition：2021；workspace resolver：2；许可证：Apache-2.0。

## Workspace 结构

```text
.
├── Cargo.toml
├── crates
│   ├── flovo-core
│   │   ├── examples
│   │   │   ├── agent_dialog.rs
│   │   │   └── data_pipeline.rs
│   │   ├── src
│   │   │   ├── config
│   │   │   ├── context_sync
│   │   │   ├── llm
│   │   │   ├── node
│   │   │   └── nodes
│   │   └── tests
│   └── flovo-ws
│       ├── src
│       └── tests
└── CLAUDE.md
```

- `flovo-core`：工作流调度、`WorkflowBuilder`、`WorkFlow`、Step/Stream 参数总线、节点抽象、配置加载与校验、Prompt 抽象、通用模型接口和内置通用节点。
- `flovo-ws`：WebSocket 握手、端点路由、连接限流、每连接独立工作流、消息读写、批量 JSON 封包和通用资源协议。
- 依赖方向固定为 `flovo-ws -> flovo-core`；核心 crate 不包含 WebSocket 服务、外部服务客户端、proto 或云厂商实现。

## 上下文同步

`context-sync` feature 默认关闭。关闭时相关模块不参与编译，工作流无需外部客户端即可正常运行：

```bash
cargo build --workspace --no-default-features
```

开启时，宿主实现 `ContextOps` 并向 `ContextSyncManager` 注入客户端：

```bash
cargo build --workspace --features flovo-ws/context-sync
```

同步行为：

- `fetch_on_start` 读取失败只记录警告，不阻断工作流。
- `push_on_complete` 失败向调用方返回错误。
- 未生成的回写字段会跳过，不向外部实现写入空值。
- 命名空间支持 `{user_id}` 和 `{session_id}` 占位符。

## 依赖安装

项目只需要 Rust stable 工具链。WebSocket 使用纯 Rust 实现，无 OpenSSL、gRPC、proto 编译器或外部服务 SDK 要求。

```bash
rustup toolchain install stable
rustup default stable
cargo build --workspace
```

Linux 上运行测试需要可用的系统 C linker，例如 GCC 提供的 `cc`。

## 示例运行

数据处理流水线：接收 JSON，执行字段校验、转换、条件分支并打印结果。

```bash
cargo run -p flovo-core --example data_pipeline
```

预期输出包含：

```text
[priority] {
  "priority": "high",
  "processed": true,
  "record_id": "record-001",
  "value": 42
}
```

Agent 问答流：接收问题、提取字段、条件判断、调用 `LlmApi` mock 流式实现、通过输出通道回写，并演示 mock 上下文同步。

```bash
cargo run -p flovo-core --example agent_dialog --features context-sync
```

预期输出包含流式回答、`outbound` 消息和写入 `user:user-demo:session:session-demo` 的 mock 上下文。

## 验证结果

FL-02 本地验收结果（2026-08-29）：

- `cargo build --workspace --no-default-features`：通过。
- `cargo build --workspace --features flovo-ws/context-sync`：通过。
- 默认 feature 测试：98 项通过。
- 开启 `context-sync` 测试：111 项通过。
- 两个示例均已实际运行通过。
- WS 双连接并发端到端测试通过。
- 核心参数总线覆盖 Step、Stream 和晚订阅顺序回放。
- 未迁入业务节点、云服务客户端、业务 proto、密钥、内部域名或本机项目路径。

常用验收命令：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --no-default-features
cargo build --workspace --features flovo-ws/context-sync
cargo test --workspace --no-default-features
cargo test --workspace --features flovo-ws/context-sync
```

## 异常处理

- `context sync is enabled but no client was injected`：配置启用了同步，但宿主未注入 `ContextOps`；关闭配置或通过 `ContextSyncManager` 注入实现。
- `workflow endpoint not found`：WebSocket URL 路径未对应任何工作流配置键。
- `connection limit reached`：连接数达到 `WsServerConfig.max_concurrent_connections`，服务器完成握手后返回拒绝信封并关闭连接。
- `outbound channel closed`：宿主输出接收端已释放；节点返回错误，避免静默丢失结果。
- `pthread_atfork` 链接失败：检查 `which cc` 是否指向自定义 wrapper。可临时使用标准 linker：

```bash
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/cc cargo test --workspace
```
