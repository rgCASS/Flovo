# Flovo

Flovo — a JSON-driven async streaming workflow engine in Rust

Flovo is a Rust workflow engine for composing AI-agent actions. Workflows are
JSON-configured, asynchronous, event-driven, streaming-first, and extensible
through a node registry. The repository is deliberately split into a small
execution core and an optional WebSocket transport.

## Architecture

```text
                         events / values
                              |   ^
                              v   |
+------------------------------------------------------------------+
| flovo-core                                                      |
|  WorkFlow + WorkflowBuilder        ParameterBus (step/stream)   |
|  BaseNode + NodeRegistry            config JSON load/validate    |
|  context_sync (optional feature)    LlmApi / Prompt abstractions |
+------------------------------------------------------------------+
                              ^
                              | workflow messages
                              v
+------------------------------------------------------------------+
| flovo-ws                                                        |
|  WebSocket server, endpoint routing, connection limits, batch    |
|  protocol and JSON envelopes                                    |
+------------------------------------------------------------------+
                              ^
                              |
                       WebSocket clients
```

`flovo-core` owns scheduling, node execution, and the parameter bus. `flovo-ws`
depends on the core and exposes each configured workflow over WebSocket; it
does not add business-service clients or protocol-specific domain nodes.

## Quick Start

1. Clone and build the workspace:

   ```bash
   git clone https://github.com/rgCASS/Flovo.git
   cd Flovo
   cargo build --workspace
   ```

2. A minimal workflow configuration has a start node, typed inputs, and a list
   of nodes connected by `choice_map` and `input_map`:

   ```json
   {
     "start_node": "receive",
     "input_parameters": {},
     "nodes": [
       {
         "id": 1,
         "node_type": "send_recv_node",
         "node_name": "receive",
         "input_map": {},
         "choice_map": {"default": "print"},
         "attrs": {"mode": "recv"}
       },
       {
         "id": 2,
         "node_type": "print_node",
         "node_name": "print",
         "input_map": {"input": "receive.output"},
         "choice_map": {},
         "attrs": {"prefix": "[result]"}
       }
     ]
   }
   ```

   The runnable `data_pipeline` example builds an equivalent configuration in
   Rust and exercises validation, transformation, branching, and output:

   ```bash
   cargo run -p flovo-core --example data_pipeline
   ```

3. Expected output includes a processed record similar to:

   ```text
   [priority] {
     "priority": "high",
     "processed": true,
     "record_id": "record-001",
     "value": 42
   }
   ```

## Built-in Nodes

| Node | Purpose | Key configuration |
| --- | --- | --- |
| `print_node` | Print a value and pass it on | `prefix` |
| `transform_node` | Apply a JSON transformation | `transform_type`, transformation fields |
| `condition_node` | Route by a JSON condition | `condition_type`, `field_path`, comparison value |
| `while_node` | Repeat while a condition remains true | loop condition and iteration limits |
| `send_recv_node` | Receive or send workflow messages | `mode`, message mapping |
| `send_cmd_recv` | Command-oriented send/receive alias | command and message mapping |
| `llm_call` | Call an injected LLM or use mock fallback | `stream`, `system_prompt`, `mock_output` |
| `schema_validator` | Validate JSON against a schema | `schema` |
| `context_fetch_node` | Fetch a field from injected context storage | `context_client`, field mapping; requires `context-sync` |

Register application-specific nodes with `NodeRegistry`; the engine only knows
about a node type after its factory (or custom logic implementation) is
registered.

## A Custom Node in 10 Lines

`BaseNode` keeps custom execution small. The production trait also requires
metadata methods so the builder can validate parameter and branch wiring:

```rust
use async_trait::async_trait;
use flovo_core::node::{BaseNode, NodeContext, NodeType, NodeRegistry};
use flovo_core::{Result, ParameterType};
use std::{collections::HashMap, sync::Arc};

struct AuditNode { inputs: HashMap<String, ParameterType>, outputs: HashMap<String, ParameterType> }
#[async_trait]
impl BaseNode for AuditNode {
    fn name(&self) -> &str { "audit" }
    fn node_type(&self) -> NodeType { NodeType::Step }
    fn input_parameters(&self) -> &HashMap<String, ParameterType> { &self.inputs }
    fn output_parameters(&self) -> &HashMap<String, ParameterType> { &self.outputs }
    fn choices(&self) -> &[String] { &[] }
    fn is_key_node(&self) -> bool { false }
    async fn run(&self, _ctx: &NodeContext) -> Result<()> { Ok(()) }
}

let registry = Arc::new(NodeRegistry::new());
registry.register("audit_node", |config, _ctx| Ok(Arc::new(AuditNode {
    inputs: HashMap::new(), outputs: HashMap::new(),
})));
```

In a real node, `NodeHelper::get_input` and `NodeHelper::set_output` connect
the implementation to the workflow's `ParameterBus`. The registered string
(`audit_node`) is the `node_type` used in JSON.

## Performance

In upstream `Workflow_standalone` measurements, streaming delivered the first
audio in **48 ms** versus **2367 ms** for unary processing, approximately **2.3 s
earlier**. Data source: upstream `Workflow_standalone` measured results; this
repository does not claim an independent benchmark.

## Positioning

| Dimension | Flovo | n8n | Temporal | Dify |
| --- | --- | --- | --- | --- |
| Runtime | Lightweight Rust library | General automation platform | Distributed durable workflows | AI application platform |
| Interaction model | Event-driven, streaming-first | Trigger/task oriented | Task/workflow orchestration | Application pipelines and agents |
| Primary fit | AI-agent action composition | SaaS integrations | Long-running reliable jobs | Prompt-centric AI apps |
| Deployment surface | `flovo-core` plus optional `flovo-ws` | Web UI and services | Server/worker cluster | Web UI and services |

These are positioning differences, not compatibility claims; choose the tool
that matches your durability, UI, and deployment requirements.

## Optional Context Sync

The `context-sync` feature is disabled by default. Enable it only when the host
application supplies a `ContextOps` implementation through
`ContextSyncManager`. Fetch failures are warnings, while push failures are
returned to the caller. Without an injected implementation, the core remains a
normal local workflow engine.

## Real LLM Configuration

`flovo-ws` provides an OpenAI-compatible `LlmApi` implementation. Configure it
through environment variables before starting the example server:

| Variable | Required | Default |
| --- | --- | --- |
| `FLOVO_LLM_BASE_URL` | No | `https://api.openai.com/v1` |
| `FLOVO_LLM_API_KEY` | Yes | — |
| `FLOVO_LLM_MODEL` | No | `gpt-4o-mini` |

The `agent_dialog` workflow uses the built-in `llm_call` node. Start it with:

```bash
export FLOVO_LLM_API_KEY=your-api-key
cargo run -p flovo-ws --example server -- \
  --config crates/flovo-ws/examples/dialog_workflow.json --port 8090
```

With a key configured, non-empty questions are sent to the configured
`/chat/completions` endpoint. If the key is missing or empty, the server skips
LLM injection and the workflow remains runnable using the fallback output
`[mock] {prompt}`.

The mock integration is runnable with:

```bash
cargo run -p flovo-core --example agent_dialog --features context-sync
```

## Contributing and License

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch, commit, and verification
conventions. Flovo is licensed under [Apache-2.0](LICENSE).

Copyright rgCASS.

[![GitHub Pages](https://img.shields.io/badge/docs-GitHub%20Pages-dea584)](https://rgcass.github.io/Flovo/)
