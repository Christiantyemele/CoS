mod state;
mod utils;
mod nodes;
mod domain;
mod runtime;
mod app_state;
mod rag;
mod neo4j;
mod api;
mod service;

use anyhow::Result;
use std::env;
use pocketflow_rs::{build_flow, Context};
use state::MyState;
use nodes::{EmployeeAgentNode, EndNode, GetInputNode, OrgBrainNode};
use app_state::APP_STATE;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    {
        let mut state = APP_STATE.lock().await;
        state.init_neo4j().await?;
        state.init_rag().await?;
    }

    let http_enabled = env::var("COS_HTTP")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    if http_enabled {
        let addr = env::var("COS_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
        let addr: std::net::SocketAddr = addr.parse()?;
        api::write_spec_json("spec.json").await?;
        return api::run_server(addr).await;
    }

    let get_input = GetInputNode;
    let employee = EmployeeAgentNode;
    let brain = OrgBrainNode;
    let end = EndNode;

    let flow = build_flow!(
        start: ("get_input", get_input),
        nodes: [("employee", employee), ("brain", brain), ("end", end)],
        edges: [
            ("get_input", "employee", MyState::Success),
            ("get_input", "get_input", MyState::Failure),
            ("get_input", "end", MyState::Exit),
            ("employee", "brain", MyState::Success),
            ("employee", "get_input", MyState::Failure),
            ("brain", "get_input", MyState::Success)
            ,("brain", "get_input", MyState::Failure)
        ]
    );

    // Shared context
    let context = Context::new();

    let _result_context = flow.run(context).await?;

    Ok(())
}
