use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{sse::Event, IntoResponse, Sse},
    routing::{get, post},
    Json, Router,
};
use futures::{stream, Stream, StreamExt};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, time::Duration};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::app_state::APP_STATE;
use crate::domain::{EmployeeRole, ReasoningTrace};

fn normalize_employee_name(s: &str) -> String {
    s.trim().to_lowercase()
}

fn resolve_employee_agent_id(
    headers: &HeaderMap,
    employee_name_body: Option<&str>,
    agent_id_body: Option<&str>,
) -> Option<String> {
    if let Some(v) = headers
        .get("x-employee-name")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let n = normalize_employee_name(v);
        return Some(format!("employee_{}", n));
    }
    if let Some(v) = employee_name_body.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let n = normalize_employee_name(v);
        return Some(format!("employee_{}", n));
    }
    agent_id_body
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn employee_role_from_agent_id(agent_id: &str) -> EmployeeRole {
    match agent_id {
        "employee_john" => EmployeeRole::Ceo,
        "employee_sarah" => EmployeeRole::Hr,
        "employee_bob" => EmployeeRole::Engineer,
        _ => EmployeeRole::Engineer,
    }
}

fn role_default_visibility(role: &EmployeeRole, topic: &str) -> &'static str {
    let t = topic.trim().to_lowercase();
    match role {
        EmployeeRole::Ceo => "full",
        EmployeeRole::Hr => {
            if t.contains("hr")
                || t.contains("people")
                || t.contains("hiring")
                || t.contains("policy")
                || t.contains("compensation")
                || t.contains("performance")
            {
                "summary"
            } else {
                "none"
            }
        }
        EmployeeRole::Engineer => {
            if t.contains("engineer")
                || t.contains("eng")
                || t.contains("tech")
                || t.contains("product")
                || t.contains("reliab")
                || t.contains("infra")
            {
                "summary"
            } else {
                "none"
            }
        }
    }
}

fn visibility_for_agent(trace: &ReasoningTrace, agent_id: &str) -> String {
    if let Some(level) = trace.routing.get(agent_id) {
        return level.clone();
    }
    let role = employee_role_from_agent_id(agent_id);
    role_default_visibility(&role, &trace.topic).to_string()
}

fn build_cors_layer() -> CorsLayer {
    let origins_raw = std::env::var("COS_CORS_ORIGINS").ok();
    let origins_raw_for_split = origins_raw.clone().unwrap_or_else(|| "*".to_string());
    let origins: Vec<String> = origins_raw_for_split
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let allow_origin = if origins.iter().any(|o| o == "*") {
        AllowOrigin::any()
    } else {
        let values = origins
            .into_iter()
            .filter_map(|o| o.parse::<axum::http::HeaderValue>().ok())
            .collect::<Vec<_>>();
        AllowOrigin::list(values)
    };

    let mut cors = CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers(Any);

    if origins_raw.is_some() && !origins_raw.as_deref().unwrap_or("").contains('*') {
        cors = cors.allow_credentials(true);
    }
    cors
}

#[derive(Clone)]
pub struct ApiState {
    pub events_tx: broadcast::Sender<ServerEvent>,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ServerEvent {
    Trace(ReasoningTrace),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AskRequest {
    pub text: Option<String>,
    pub audio_base64: Option<String>,
    pub audio_mime: Option<String>,
    pub agent_id: Option<String>,
    pub employee_name: Option<String>,
    pub response_audio: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AskResponse {
    pub response_text: String,
    pub trace: ReasoningTrace,
    pub audio_base64: Option<String>,
    pub audio_mime: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KnowledgeIngestRequest {
    pub truth_id: String,
    pub kind: String,
    pub content: String,
    pub agent_id: Option<String>,
    pub routing: serde_json::Value,
    pub add_to_rag: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct KnowledgeIngestResponse {
    pub trace: ReasoningTrace,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TraceListResponse {
    pub traces: Vec<ReasoningTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AgentTraceListResponse {
    pub agent_id: String,
    pub traces: Vec<ReasoningTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphEdge {
    pub id: String,
    pub edge_type: String,
    pub from: String,
    pub to: String,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphSnapshotResponse {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CurrentDecisionsResponse {
    pub decisions: Vec<GraphNode>,
    pub decision_versions: Vec<GraphNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CurrentTruthResponse {
    pub truth_objects: Vec<GraphNode>,
    pub truth_versions: Vec<GraphNode>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
#[derive(IntoParams)]
pub struct Pagination {
    pub limit: Option<usize>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health,
        ask,
        ingest_knowledge,
        list_traces,
        agent_traces,
        graph_snapshot,
        agent_graph_snapshot,
        current_decisions,
        current_truth,
        sse_stream,
        openapi_json
    ),
    components(
        schemas(
            AskRequest,
            AskResponse,
            KnowledgeIngestRequest,
            KnowledgeIngestResponse,
            HealthResponse,
            TraceListResponse,
            AgentTraceListResponse,
            ReasoningTrace,
            ServerEvent,
            GraphSnapshotResponse,
            GraphNode,
            GraphEdge,
            CurrentDecisionsResponse,
            CurrentTruthResponse,
            Pagination
        )
    ),
    tags(
        (name = "cos", description = "AI Chief of Staff backend")
    )
)]
pub struct ApiDoc;

pub fn app(state: ApiState) -> Router {
    let cors = build_cors_layer();

    Router::new()
        .route("/health", get(health))
        .route("/v1/ask", post(ask))
        .route("/v1/knowledge", post(ingest_knowledge))
        .route("/v1/traces", get(list_traces))
        .route("/v1/agents/:agent_id/traces", get(agent_traces))
        .route("/v1/graph/snapshot", get(graph_snapshot))
        .route("/v1/agents/:agent_id/graph/snapshot", get(agent_graph_snapshot))
        .route("/v1/decisions/current", get(current_decisions))
        .route("/v1/truth/current", get(current_truth))
        .route("/v1/stream", get(sse_stream))
        .route("/openapi.json", get(openapi_json))
        .with_state(state)
        .layer(cors)
}

fn unauthorized() -> axum::response::Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "unauthorized"})),
    )
        .into_response()
}

fn auth_ok(headers: &HeaderMap, state: &ApiState) -> bool {
    let Some(expected) = &state.api_key else {
        return true;
    };

    let provided = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    provided == expected
}

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, body = HealthResponse))
)]
async fn health() -> impl IntoResponse {
    Json(HealthResponse { ok: true })
}

#[utoipa::path(
    post,
    path = "/v1/ask",
    request_body = AskRequest,
    responses(
        (status = 200, body = AskResponse),
        (status = 500, body = serde_json::Value)
    )
)]
async fn ask(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<AskRequest>,
) -> impl IntoResponse {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }

    // Identity is required (either header or request body field for audio clients).
    let Some(_caller_agent_id) = resolve_employee_agent_id(
        &headers,
        req.employee_name.as_deref(),
        req.agent_id.as_deref(),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing x-employee-name"})),
        )
            .into_response();
    };

    let text = if let Some(t) = req.text.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        t.to_string()
    } else if let Some(b64) = req.audio_base64.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let bytes = match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(b) => b,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "audio_base64 must be valid base64"})),
                )
                    .into_response();
            }
        };

        match crate::utils::elevenlabs_stt_from_bytes(bytes, req.audio_mime.as_deref()).await {
            Ok(t) => t,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "provide either non-empty text or audio_base64"})),
        )
            .into_response();
    };

    let resolved_agent_id = resolve_employee_agent_id(
        &headers,
        req.employee_name.as_deref(),
        req.agent_id.as_deref(),
    );
    match crate::service::ask_and_persist(text, resolved_agent_id).await {
        Ok((response_text, trace)) => {
            let _ = api_state.events_tx.send(ServerEvent::Trace(trace.clone()));
            let want_audio = req.response_audio.unwrap_or(false);
            if want_audio {
                match crate::utils::elevenlabs_tts_to_mp3_bytes(&response_text).await {
                    Ok(bytes) => {
                        let audio_base64 = Some(base64::engine::general_purpose::STANDARD.encode(bytes));
                        let audio_mime = Some("audio/mpeg".to_string());
                        (
                            StatusCode::OK,
                            Json(AskResponse {
                                response_text,
                                trace,
                                audio_base64,
                                audio_mime,
                            }),
                        )
                            .into_response()
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            } else {
                (
                    StatusCode::OK,
                    Json(AskResponse {
                        response_text,
                        trace,
                        audio_base64: None,
                        audio_mime: None,
                    }),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/v1/knowledge",
    request_body = KnowledgeIngestRequest,
    responses(
        (status = 200, body = KnowledgeIngestResponse),
        (status = 400, body = serde_json::Value),
        (status = 500, body = serde_json::Value)
    )
)]
async fn ingest_knowledge(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<KnowledgeIngestRequest>,
) -> axum::response::Response {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }

    if req.truth_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "truth_id must be non-empty"})),
        )
            .into_response();
    }
    if req.kind.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "kind must be non-empty"})),
        )
            .into_response();
    }
    if !req.routing.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "routing must be an object mapping agent_id -> level"})),
        )
            .into_response();
    }

    let add_to_rag = req.add_to_rag.unwrap_or(true);
    match crate::service::ingest_knowledge(
        req.truth_id,
        req.kind,
        req.content,
        req.agent_id,
        req.routing,
        add_to_rag,
    )
    .await
    {
        Ok(trace) => {
            let _ = api_state.events_tx.send(ServerEvent::Trace(trace.clone()));
            (StatusCode::OK, Json(KnowledgeIngestResponse { trace })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/v1/traces",
    params(Pagination),
    responses((status = 200, body = TraceListResponse))
)]
async fn list_traces(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Query(p): Query<Pagination>,
) -> axum::response::Response {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }
    // Only CEO may view all traces.
    let Some(agent_id) = resolve_employee_agent_id(&headers, None, None) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing x-employee-name"})),
        )
            .into_response();
    };
    if employee_role_from_agent_id(&agent_id) != EmployeeRole::Ceo {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "forbidden"})),
        )
            .into_response();
    }

    let limit = p.limit.unwrap_or(50);
    let state = APP_STATE.lock().await;
    let mut traces = state.traces.clone();
    traces.reverse();
    traces.truncate(limit);
    (StatusCode::OK, Json(TraceListResponse { traces })).into_response()
}

#[utoipa::path(
    get,
    path = "/v1/agents/{agent_id}/traces",
    params(
        ("agent_id" = String, Path, description = "Employee/agent id"),
        Pagination
    ),
    responses((status = 200, body = AgentTraceListResponse))
)]
async fn agent_traces(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
    Query(p): Query<Pagination>,
) -> impl IntoResponse {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }

    // Only allow a caller to request their own agent view (or CEO).
    let Some(caller_agent_id) = resolve_employee_agent_id(&headers, None, None) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing x-employee-name"})),
        )
            .into_response();
    };
    let caller_role = employee_role_from_agent_id(&caller_agent_id);
    if caller_role != EmployeeRole::Ceo && caller_agent_id != agent_id {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "forbidden"})),
        )
            .into_response();
    }

    let limit = p.limit.unwrap_or(50);
    let state = APP_STATE.lock().await;
    let mut out = Vec::new();

    for t in state.traces.iter().rev() {
        let level = visibility_for_agent(t, &agent_id);
        if level == "none" {
            continue;
        }

        let mut tt = t.clone();
        if level == "summary" {
            tt.evidence = Vec::new();
            tt.assumptions = Vec::new();
        }

        out.push(tt);
        if out.len() >= limit {
            break;
        }
    }

    Json(AgentTraceListResponse {
        agent_id,
        traces: out,
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/v1/graph/snapshot",
    params(Pagination),
    responses(
        (status = 200, body = GraphSnapshotResponse),
        (status = 500, body = serde_json::Value)
    )
)]
async fn graph_snapshot(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Query(p): Query<Pagination>,
) -> axum::response::Response {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }
    let limit = p.limit.unwrap_or(5000) as i64;

    let state = APP_STATE.lock().await;
    let client = match state.neo4j.clone() {
        Some(c) => c,
        None => {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "neo4j not initialized"})),
        )
            .into_response();
        }
    };

    drop(state);

    let graph = client.graph();

    let node_query = neo4rs::query(
        r#"
MATCH (n)
WITH n,
     properties(n) AS p,
     toString(n.created_at) AS created_at_s,
     coalesce(
       n.name,
       n.label,
       n.summary,
       n.decision,
       n.truth_id,
       n.employee_id,
       n.team_id,
       n.topic,
       n.decision_id,
       n.decision_version_id,
       n.truth_version_id,
       elementId(n)
     ) AS display_label
WITH n, p, created_at_s,
     CASE
       WHEN display_label = elementId(n) THEN coalesce(head(labels(n)), 'Node') + ':' + display_label
       ELSE display_label
     END AS display_label2
RETURN elementId(n) AS id,
       labels(n) AS labels,
       p { .*, label: display_label2, created_at: created_at_s } AS props
LIMIT $limit
"#,
    )
    .param("limit", limit);

    let edge_query = neo4rs::query(
        r#"
MATCH (a)-[r]->(b)
WITH a, r, b,
     properties(r) AS p,
     toString(r.created_at) AS created_at_s,
     coalesce(r.name, r.label, type(r)) AS display_label
RETURN elementId(r) AS id,
       type(r) AS t,
       elementId(a) AS from,
       elementId(b) AS to,
       p { .*, label: display_label, created_at: created_at_s } AS props
LIMIT $limit
"#,
    )
    .param("limit", limit);

    let mut nodes_out = Vec::new();
    let mut stream = match graph.execute(node_query).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    while let Ok(Some(row)) = stream.next().await {
        let id: String = row.get("id").unwrap_or_default();
        let labels: Vec<String> = row.get("labels").unwrap_or_default();
        let properties = match row.get::<neo4rs::BoltType>("props") {
            Ok(v) => bolt_to_json(v),
            Err(_) => serde_json::Value::Null,
        };

        nodes_out.push(GraphNode {
            id,
            labels,
            properties,
        });
    }

    let mut edges_out = Vec::new();
    let mut stream = match graph.execute(edge_query).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    while let Ok(Some(row)) = stream.next().await {
        let id: String = row.get("id").unwrap_or_default();
        let edge_type: String = row.get("t").unwrap_or_default();
        let from: String = row.get("from").unwrap_or_default();
        let to: String = row.get("to").unwrap_or_default();
        let properties = match row.get::<neo4rs::BoltType>("props") {
            Ok(v) => bolt_to_json(v),
            Err(_) => serde_json::Value::Null,
        };

        edges_out.push(GraphEdge {
            id,
            edge_type,
            from,
            to,
            properties,
        });
    }

    Json(GraphSnapshotResponse {
        nodes: nodes_out,
        edges: edges_out,
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/v1/agents/{agent_id}/graph/snapshot",
    params(
        ("agent_id" = String, Path, description = "Employee/agent id"),
        Pagination
    ),
    responses(
        (status = 200, body = GraphSnapshotResponse),
        (status = 500, body = serde_json::Value)
    )
)]
async fn agent_graph_snapshot(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Path(agent_id): Path<String>,
    Query(p): Query<Pagination>,
) -> impl IntoResponse {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }

    let limit = p.limit.unwrap_or(5000) as i64;

    let state = APP_STATE.lock().await;
    let client = match state.neo4j.clone() {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "neo4j not initialized"})),
            )
                .into_response();
        }
    };
    drop(state);

    let graph = client.graph();

    let q = neo4rs::query(
        r#"
MATCH (n)
WHERE (n:DecisionVersion OR n:TruthVersion) AND $agent_id IN coalesce(n.routing_agents, [])
WITH collect(n) AS versions
UNWIND versions AS v
OPTIONAL MATCH (a)-[r]->(b)
WHERE a = v OR b = v
WITH a, r, b,
     properties(a) AS a_p,
     properties(r) AS r_p,
     properties(b) AS b_p,
     toString(a.created_at) AS a_created_at_s,
     toString(r.created_at) AS r_created_at_s,
     toString(b.created_at) AS b_created_at_s,
     coalesce(
       a.name,
       a.label,
       a.summary,
       a.decision,
       a.truth_id,
       a.employee_id,
       a.team_id,
       a.topic,
       a.decision_id,
       a.decision_version_id,
       a.truth_version_id,
       elementId(a)
     ) AS a_display_label,
     coalesce(r.name, r.label, type(r)) AS r_display_label,
     coalesce(
       b.name,
       b.label,
       b.summary,
       b.decision,
       b.truth_id,
       b.employee_id,
       b.team_id,
       b.topic,
       b.decision_id,
       b.decision_version_id,
       b.truth_version_id,
       elementId(b)
     ) AS b_display_label
WITH a, r, b,
     a_p, r_p, b_p,
     a_created_at_s, r_created_at_s, b_created_at_s,
     CASE
       WHEN a_display_label = elementId(a) THEN coalesce(head(labels(a)), 'Node') + ':' + a_display_label
       ELSE a_display_label
     END AS a_display_label2,
     r_display_label,
     CASE
       WHEN b_display_label = elementId(b) THEN coalesce(head(labels(b)), 'Node') + ':' + b_display_label
       ELSE b_display_label
     END AS b_display_label2
RETURN elementId(a) AS a_id,
       labels(a) AS a_labels,
       a_p { .*, label: a_display_label2, created_at: a_created_at_s } AS a_props,
       elementId(r) AS r_id,
       type(r) AS r_type,
       r_p { .*, label: r_display_label, created_at: r_created_at_s } AS r_props,
       elementId(b) AS b_id,
       labels(b) AS b_labels,
       b_p { .*, label: b_display_label2, created_at: b_created_at_s } AS b_props
LIMIT $limit
"#,
    )
    .param("agent_id", agent_id)
    .param("limit", limit);

    let mut nodes: HashMap<String, GraphNode> = HashMap::new();
    let mut edges: HashMap<String, GraphEdge> = HashMap::new();

    let mut stream = match graph.execute(q).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    while let Ok(Some(row)) = stream.next().await {
        let a_id: String = row.get("a_id").unwrap_or_default();
        if !a_id.is_empty() {
            let a_labels: Vec<String> = row.get("a_labels").unwrap_or_default();
            let a_props = match row.get::<neo4rs::BoltType>("a_props") {
                Ok(v) => bolt_to_json(v),
                Err(_) => serde_json::Value::Null,
            };
            nodes.entry(a_id.clone()).or_insert(GraphNode {
                id: a_id,
                labels: a_labels,
                properties: a_props,
            });
        }

        let b_id: String = row.get("b_id").unwrap_or_default();
        if !b_id.is_empty() {
            let b_labels: Vec<String> = row.get("b_labels").unwrap_or_default();
            let b_props = match row.get::<neo4rs::BoltType>("b_props") {
                Ok(v) => bolt_to_json(v),
                Err(_) => serde_json::Value::Null,
            };
            nodes.entry(b_id.clone()).or_insert(GraphNode {
                id: b_id,
                labels: b_labels,
                properties: b_props,
            });
        }

        let r_id: String = row.get("r_id").unwrap_or_default();
        if !r_id.is_empty() {
            let r_type: String = row.get("r_type").unwrap_or_default();
            let r_props = match row.get::<neo4rs::BoltType>("r_props") {
                Ok(v) => bolt_to_json(v),
                Err(_) => serde_json::Value::Null,
            };
            let from: String = row.get("a_id").unwrap_or_default();
            let to: String = row.get("b_id").unwrap_or_default();
            edges.entry(r_id.clone()).or_insert(GraphEdge {
                id: r_id,
                edge_type: r_type,
                from,
                to,
                properties: r_props,
            });
        }
    }

    Json(GraphSnapshotResponse {
        nodes: nodes.into_values().collect(),
        edges: edges.into_values().collect(),
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/v1/decisions/current",
    params(Pagination),
    responses(
        (status = 200, body = CurrentDecisionsResponse),
        (status = 500, body = serde_json::Value)
    )
)]
async fn current_decisions(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Query(p): Query<Pagination>,
) -> impl IntoResponse {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }

    let limit = p.limit.unwrap_or(200) as i64;
    let state = APP_STATE.lock().await;
    let client = match state.neo4j.clone() {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "neo4j not initialized"})),
            )
                .into_response();
        }
    };
    drop(state);

    let graph = client.graph();
    let q = neo4rs::query(
        r#"
MATCH (d:Decision)-[:CURRENT]->(dv:DecisionVersion)
RETURN elementId(d) AS d_id, labels(d) AS d_labels, properties(d) AS d_props,
       elementId(dv) AS dv_id, labels(dv) AS dv_labels, properties(dv) AS dv_props
LIMIT $limit
"#,
    )
    .param("limit", limit);

    let mut decisions: HashMap<String, GraphNode> = HashMap::new();
    let mut versions: HashMap<String, GraphNode> = HashMap::new();
    let mut stream = match graph.execute(q).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    while let Ok(Some(row)) = stream.next().await {
        let d_id: String = row.get("d_id").unwrap_or_default();
        let d_labels: Vec<String> = row.get("d_labels").unwrap_or_default();
        let d_props = match row.get::<neo4rs::BoltType>("d_props") {
            Ok(v) => bolt_to_json(v),
            Err(_) => serde_json::Value::Null,
        };
        decisions.entry(d_id.clone()).or_insert(GraphNode {
            id: d_id,
            labels: d_labels,
            properties: d_props,
        });

        let dv_id: String = row.get("dv_id").unwrap_or_default();
        let dv_labels: Vec<String> = row.get("dv_labels").unwrap_or_default();
        let dv_props = match row.get::<neo4rs::BoltType>("dv_props") {
            Ok(v) => bolt_to_json(v),
            Err(_) => serde_json::Value::Null,
        };
        versions.entry(dv_id.clone()).or_insert(GraphNode {
            id: dv_id,
            labels: dv_labels,
            properties: dv_props,
        });
    }

    Json(CurrentDecisionsResponse {
        decisions: decisions.into_values().collect(),
        decision_versions: versions.into_values().collect(),
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/v1/truth/current",
    params(Pagination),
    responses(
        (status = 200, body = CurrentTruthResponse),
        (status = 500, body = serde_json::Value)
    )
)]
async fn current_truth(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Query(p): Query<Pagination>,
) -> impl IntoResponse {
    if !auth_ok(&headers, &api_state) {
        return unauthorized();
    }

    let limit = p.limit.unwrap_or(200) as i64;
    let state = APP_STATE.lock().await;
    let client = match state.neo4j.clone() {
        Some(c) => c,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "neo4j not initialized"})),
            )
                .into_response();
        }
    };
    drop(state);

    let graph = client.graph();
    let q = neo4rs::query(
        r#"
MATCH (o:TruthObject)-[:CURRENT]->(tv:TruthVersion)
RETURN elementId(o) AS o_id, labels(o) AS o_labels, properties(o) AS o_props,
       elementId(tv) AS tv_id, labels(tv) AS tv_labels, properties(tv) AS tv_props
LIMIT $limit
"#,
    )
    .param("limit", limit);

    let mut objs: HashMap<String, GraphNode> = HashMap::new();
    let mut vers: HashMap<String, GraphNode> = HashMap::new();
    let mut stream = match graph.execute(q).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    while let Ok(Some(row)) = stream.next().await {
        let o_id: String = row.get("o_id").unwrap_or_default();
        let o_labels: Vec<String> = row.get("o_labels").unwrap_or_default();
        let o_props = match row.get::<neo4rs::BoltType>("o_props") {
            Ok(v) => bolt_to_json(v),
            Err(_) => serde_json::Value::Null,
        };
        objs.entry(o_id.clone()).or_insert(GraphNode {
            id: o_id,
            labels: o_labels,
            properties: o_props,
        });

        let tv_id: String = row.get("tv_id").unwrap_or_default();
        let tv_labels: Vec<String> = row.get("tv_labels").unwrap_or_default();
        let tv_props = match row.get::<neo4rs::BoltType>("tv_props") {
            Ok(v) => bolt_to_json(v),
            Err(_) => serde_json::Value::Null,
        };
        vers.entry(tv_id.clone()).or_insert(GraphNode {
            id: tv_id,
            labels: tv_labels,
            properties: tv_props,
        });
    }

    Json(CurrentTruthResponse {
        truth_objects: objs.into_values().collect(),
        truth_versions: vers.into_values().collect(),
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/v1/stream",
    params(
        ("employee_name" = Option<String>, Query, description = "Employee name (for browser EventSource; alternative to x-employee-name header)"),
    ),
    responses((status = 200, body = String, description = "SSE stream"))
)]
async fn sse_stream(
    State(api_state): State<ApiState>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = api_state.events_tx.subscribe();

    let employee_name = q.get("employee_name").map(|s| s.as_str());
    let agent_id = resolve_employee_agent_id(&headers, employee_name, None);

    let initial = stream::once(async {
        Ok(Event::default().event("cos").data("{\"type\":\"connected\"}"))
    });

    let stream = initial.chain(
        BroadcastStream::new(rx)
        .filter_map(|msg| async move { msg.ok() })
        .filter_map(move |evt| {
            let agent_id = agent_id.clone();
            async move {
                match (&evt, agent_id.as_deref()) {
                    (ServerEvent::Trace(t), Some(aid)) => {
                        let level = visibility_for_agent(t, aid);
                        if level == "none" {
                            return None;
                        }
                        let mut tt = t.clone();
                        if level == "summary" {
                            tt.evidence = Vec::new();
                            tt.assumptions = Vec::new();
                        }
                        Some(ServerEvent::Trace(tt))
                    }
                    // If no identity is provided, do not emit any events.
                    _ => None,
                }
            }
        })
        .map(|evt| {
            let data = serde_json::to_string(&evt).unwrap_or_else(|_| "{}".to_string());
            Ok(Event::default().event("cos").data(data))
        }),
    );

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("ping"),
    )
}

#[utoipa::path(
    get,
    path = "/openapi.json",
    responses((status = 200, body = serde_json::Value))
)]
async fn openapi_json() -> impl IntoResponse {
    Json(serde_json::to_value(&ApiDoc::openapi()).unwrap_or_else(|_| json!({})))
}

pub async fn run_server(addr: SocketAddr) -> anyhow::Result<()> {
    let (tx, _rx) = broadcast::channel::<ServerEvent>(256);
    let api_key = std::env::var("COS_API_KEY").ok();
    let app = app(ApiState {
        events_tx: tx,
        api_key,
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn write_spec_json(path: &str) -> anyhow::Result<()> {
    let v = serde_json::to_value(&ApiDoc::openapi()).unwrap_or_else(|_| json!({}));
    let bytes = serde_json::to_vec_pretty(&v)?;
    tokio::fs::write(path, bytes).await?;
    Ok(())
}

fn bolt_to_json(v: neo4rs::BoltType) -> serde_json::Value {
    match v {
        neo4rs::BoltType::Null(_) => serde_json::Value::Null,
        neo4rs::BoltType::Boolean(b) => serde_json::Value::Bool(b.value),
        neo4rs::BoltType::Integer(i) => {
            let v: i64 = i.into();
            serde_json::Value::Number(v.into())
        }
        neo4rs::BoltType::Float(f) => serde_json::Number::from_f64(f.value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        neo4rs::BoltType::String(s) => serde_json::Value::String(s.to_string()),
        neo4rs::BoltType::DateTime(dt) => serde_json::Value::String(format!("{dt:?}")),
        neo4rs::BoltType::LocalDateTime(dt) => serde_json::Value::String(format!("{dt:?}")),
        neo4rs::BoltType::Date(d) => serde_json::Value::String(format!("{d:?}")),
        neo4rs::BoltType::Time(t) => serde_json::Value::String(format!("{t:?}")),
        neo4rs::BoltType::LocalTime(t) => serde_json::Value::String(format!("{t:?}")),
        neo4rs::BoltType::Duration(d) => serde_json::Value::String(format!("{d:?}")),
        neo4rs::BoltType::List(l) => {
            let v: Vec<neo4rs::BoltType> = l.into();
            serde_json::Value::Array(v.into_iter().map(bolt_to_json).collect())
        }
        neo4rs::BoltType::Map(m) => {
            let mut out = serde_json::Map::new();
            for (k, val) in m.value {
                out.insert(k.to_string(), bolt_to_json(val));
            }
            serde_json::Value::Object(out)
        }
        other => serde_json::Value::String(format!("{other:?}")),
    }
}
