//! WebSocket 工作流服务器示例，供外部客户端联调。

use flovo_core::config::WorkflowConfig;
use flovo_ws::{WsServer, WsServerConfig};
use std::collections::HashMap;
use std::error::Error;
use std::io;

const DEFAULT_CONFIG_PATH: &str = "crates/flovo-ws/examples/dialog_workflow.json";
const DEFAULT_PORT: u16 = 8090;

struct Options {
    config_path: String,
    port: u16,
}

fn parse_options() -> Result<Options, String> {
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut port = DEFAULT_PORT;
    let mut args = std::env::args().skip(1);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--config" => {
                config_path = args
                    .next()
                    .ok_or_else(|| "--config requires a file path".to_string())?;
            }
            "--port" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--port requires a port number".to_string())?;
                port = value
                    .parse::<u16>()
                    .map_err(|_| format!("invalid port: {value}"))?;
            }
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }

    Ok(Options { config_path, port })
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options =
        parse_options().map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    let content = std::fs::read_to_string(&options.config_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read {}: {error}", options.config_path),
        )
    })?;
    let configs: HashMap<String, WorkflowConfig> =
        serde_json::from_str(&content).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse {}: {error}", options.config_path),
            )
        })?;

    let mut workflow_names = configs.keys().cloned().collect::<Vec<_>>();
    workflow_names.sort();
    let address = format!("127.0.0.1:{}", options.port);

    // WsServer 只注册内置节点；配置通过转换和发送节点组合模拟 LLM 分块输出。
    let server = WsServer::new_with_config(address.clone(), configs, WsServerConfig::default())?;
    println!("Listening on ws://{address}/<workflow>");
    println!("Loaded workflows: {}", workflow_names.join(", "));
    server.start().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
