# Backend HTTP API

This backend exposes a minimal HTTP API for:

- Asking questions / ingesting tool signals
- Returning responses + structured reasoning traces
- Fetching a graph snapshot for visualization
- Fetching per-agent filtered views (selective routing)
- Streaming real-time trace events via SSE
- Serving an OpenAPI spec for client generation

## Base URL

- Default: `http://127.0.0.1:3000`

## OpenAPI spec

- `GET /openapi.json`
- `spec.json` is also written to the repo root on startup in HTTP mode.

Use `spec.json` or `/openapi.json` to generate frontend client functions.

## Auth (optional)

If you set `COS_API_KEY` in the backend environment:

- Requests to admin/global endpoints must include header `x-api-key: <COS_API_KEY>`.
- Agent-scoped endpoints also require `x-api-key` when `COS_API_KEY` is set.

If `COS_API_KEY` is not set, all endpoints are open.

## Endpoints

### Health

- `GET /health`

Response:
```json
{ "ok": true }
```

### Ask (primary endpoint)

- `POST /v1/ask`

Request:
```json
{
  "text": "We decided to change PTO policy; notify engineering.",
  "agent_id": "employee_1"
}
```

You can also send audio (base64) instead of `text`:
```json
{
  "audio_base64": "<base64 audio bytes>",
  "audio_mime": "audio/webm",
  "agent_id": "employee_1",
  "response_audio": true
}
```

Response:
```json
{
  "response_text": "...",
  "audio_base64": "<optional base64 mp3>",
  "audio_mime": "audio/mpeg",
  "trace": {
    "decision_id": "...",
    "summary": "...",
    "version": 1,
    "rationale": "...",
    "evidence": ["..."],
    "assumptions": ["..."],
    "trigger_events": ["<uuid>"]
  }
}
```

Notes:
- The backend runs the flow: EmployeeAgent -> Event -> OrgBrain -> Neo4j persistence -> Trace.
- `trace.graph_updates.nodes` contains Neo4j `elementId(...)` values for newly written nodes.
- `trace.routing` is the selective disclosure map.

### Knowledge ingest (frontend adds extra knowledge)

- `POST /v1/knowledge`

Use this when the frontend collects additional structured knowledge (policy, decision notes, meeting summary) and you want to immediately:

- Version it into Neo4j as a `TruthObject` + new `TruthVersion`
- Optionally add it to the RAG index (so the OrgBrain can retrieve it later)
- Emit a `ReasoningTrace` via SSE so graph UI updates in real time

Request:
```json
{
  "truth_id": "pto_policy",
  "kind": "policy",
  "content": "PTO policy updated: ...",
  "agent_id": "employee_1",
  "routing": { "employee_1": "full", "employee_2": "summary" },
  "add_to_rag": true
}
```

Response:
```json
{
  "trace": {
    "decision_id": "pto_policy",
    "summary": "PTO policy updated: ...",
    "version": 3,
    "rationale": "knowledge_ingest",
    "trigger_events": ["<uuid>"]
  }
}
```

### List traces

- `GET /v1/traces?limit=50`

Returns latest traces (most recent first).

Auth:
- Requires `x-api-key` if `COS_API_KEY` is set.

### Per-agent traces (routing-enforced)

- `GET /v1/agents/{agent_id}/traces?limit=50`

This endpoint enforces selective information routing:

- If a trace has `routing[agent_id] == "none"` it is excluded.
- If `routing[agent_id] == "summary"`, the backend redacts:
  - `trace.evidence`
  - `trace.assumptions`

This is the recommended endpoint for agent/user-specific UIs.

### Graph snapshot (for visualization)

- `GET /v1/graph/snapshot?limit=500`

Returns:
- `nodes`: `{ id, labels, properties }`
- `edges`: `{ id, edge_type, from, to, properties }`

Notes:
- `id`, `from`, `to` are Neo4j `elementId(...)` strings.

Auth:
- Requires `x-api-key` if `COS_API_KEY` is set.

### Per-agent graph snapshot (routing-enforced)

- `GET /v1/agents/{agent_id}/graph/snapshot?limit=500`

This returns only nodes/edges connected to version nodes where the agent is routed:

- `DecisionVersion` / `TruthVersion` nodes with `agent_id` in `routing_agents`.

This is the recommended endpoint for agent/user-specific graph visualization.

### Current decisions

- `GET /v1/decisions/current?limit=200`

Returns the current `Decision` + `DecisionVersion` pairs (via `CURRENT` relationship).

### Current organizational truth

- `GET /v1/truth/current?limit=200`

Returns the current `TruthObject` + `TruthVersion` pairs (via `CURRENT` relationship).

### Real-time stream (SSE)

- `GET /v1/stream`

This is Server-Sent Events. Each message is an event named `cos` with JSON payload.

Example payload:
```json
{ "type": "trace", "data": { "decision_id": "...", "summary": "..." } }
```

## Frontend usage examples

### Fetch ask

```ts
const r = await fetch("http://127.0.0.1:3000/v1/ask", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ text: "hello", agent_id: "employee_1" })
});
const data = await r.json();
console.log(data.response_text, data.trace);
```

If `COS_API_KEY` is set:

```ts
const r = await fetch("http://127.0.0.1:3000/v1/ask", {
  method: "POST",
  headers: { "Content-Type": "application/json", "x-api-key": process.env.COS_API_KEY! },
  body: JSON.stringify({ text: "hello", agent_id: "employee_1" })
});
```

### Subscribe to SSE

```ts
const es = new EventSource("http://127.0.0.1:3000/v1/stream");
es.addEventListener("cos", (msg) => {
  const evt = JSON.parse((msg as MessageEvent).data);
  if (evt.type === "trace") {
    // update timeline
  }
});
```
