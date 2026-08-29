# Changelog

This changelog follows the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
format. Versions follow [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-08-29

### Added

- Core workflow engine: `WorkFlow`, `WorkflowBuilder`, `ParameterBus`,
  `BaseNode`, and `NodeRegistry`.
- Eight general-purpose nodes: print, transform, condition, while,
  send/receive, command send/receive, schema validation, and context fetch.
- `flovo-ws` WebSocket service layer with endpoint routing and batch protocol.
- Optional `context-sync` integration through the `ContextOps` trait.
- `data_pipeline` and `agent_dialog` example workflows.
