use anyhow::Result;
use async_trait::async_trait;
use pocketflow_rs::{Context, Node, ProcessResult};
use serde_json::json;
use tokio::io::{self, AsyncBufReadExt};

use crate::state::MyState;

use crate::app_state::APP_STATE;
use crate::domain::{EmployeeAgentId, Event, EventType, GraphUpdates, ReasoningTrace};
use crate::neo4j::writer::{next_decision_version, next_truth_version, persist_decision_version, persist_truth_version};
use crate::utils::{elevenlabs_stt_from_file, elevenlabs_tts_to_mp3_bytes, openai_chat, play_mp3_bytes};

pub struct GetInputNode;

pub struct EndNode;

fn extract_first_json_object(s: &str) -> Option<String> {
    let start = s.find('{')?;
    let end = s.rfind('}')?;
    if end <= start {
        return None;
    }
    Some(s[start..=end].to_string())
}

#[async_trait]
impl Node for GetInputNode {
    type State = MyState;

    async fn execute(&self, _context: &Context) -> Result<serde_json::Value> {
        println!("Enter text, or 'stt:/path/to/audio', or 'exit': ");
        let mut reader = io::BufReader::new(tokio::io::stdin());
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(json!({"mode": "exit", "text": ""}));
        }
        let raw = line.trim().to_string();

        if raw == "exit" {
            return Ok(json!({"mode": "exit", "text": ""}));
        }

        if let Some(path) = raw.strip_prefix("stt:") {
            let text = elevenlabs_stt_from_file(path.trim()).await?;
            return Ok(json!({"mode": "stt", "text": text}));
        }

        Ok(json!({"mode": "text", "text": raw}))
    }

    async fn post_process(
        &self,
        context: &mut Context,
        result: &Result<serde_json::Value>,
    ) -> Result<ProcessResult<MyState>> {
        if let Ok(val) = result {
            let mode = val.get("mode").and_then(|v| v.as_str()).unwrap_or("text");
            let text = val.get("text").and_then(|v| v.as_str()).unwrap_or("");

            if mode == "exit" {
                return Ok(ProcessResult::new(MyState::Exit, "exit".to_string()));
            }

            println!("You said: {}", text);
            context.set("input_text", json!(text));
            Ok(ProcessResult::new(MyState::Success, "success".to_string()))
        } else {
            if let Err(e) = result {
                eprintln!("GetInputNode error: {e}");
            }
            Ok(ProcessResult::new(MyState::Failure, "failure".to_string()))
        }
    }
}

pub struct EmployeeAgentNode;

#[async_trait]
impl Node for EmployeeAgentNode {
    type State = MyState;

    async fn execute(&self, context: &Context) -> Result<serde_json::Value> {
        let input_text = context
            .get("input_text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let agent_id = EmployeeAgentId("employee_1".to_string());

        let system = r#"You are an EmployeeAgent.
Given the user's input, emit a single event for the OrgBrain to process.

Return STRICT JSON with keys:
- event_type: one of ["decision_signal","update","concern","clarification"]
- topic: short topic string
- confidence: number in [0,1]
- private_note: a short private note (may include sensitive/rough thoughts)
"#;

        let out = openai_chat(system, &input_text).await?;
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap_or_else(|_| {
            json!({
                "event_type": "update",
                "topic": "general",
                "confidence": 0.5,
                "private_note": out
            })
        });

        let event_type = match parsed
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("update")
        {
            "decision_signal" => EventType::DecisionSignal,
            "concern" => EventType::Concern,
            "clarification" => EventType::Clarification,
            _ => EventType::Update,
        };

        let topic = parsed
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("general")
            .to_string();
        let confidence = parsed
            .get("confidence")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.5) as f32;
        let private_note = parsed
            .get("private_note")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut state = APP_STATE.lock().await;
        let private_key = state.store_private(&agent_id, private_note);
        let event = Event::new(agent_id.clone(), event_type, topic, confidence, vec![private_key]);
        let event_id = event.event_id;

        println!(
            "EmployeeAgent emitted event: id={} type={:?} topic={} confidence={}",
            event_id, event.event_type, event.topic, event.confidence
        );

        state.emit(event);

        Ok(json!({"event_id": event_id.to_string(), "agent_id": agent_id.0}))
    }

    async fn post_process(
        &self,
        context: &mut Context,
        result: &Result<serde_json::Value>,
    ) -> Result<ProcessResult<MyState>> {
        if let Ok(val) = result {
            context.set("last_employee_event", val.clone());
            Ok(ProcessResult::new(MyState::Success, "success".to_string()))
        } else {
            if let Err(e) = result {
                eprintln!("EmployeeAgentNode error: {e}");
            }
            Ok(ProcessResult::new(MyState::Failure, "failure".to_string()))
        }
    }
}

pub struct OrgBrainNode;

#[async_trait]
impl Node for OrgBrainNode {
    type State = MyState;

    async fn execute(&self, _context: &Context) -> Result<serde_json::Value> {
        let mut state = APP_STATE.lock().await;
        let events = state.drain_events();
        let neo4j = state.neo4j.clone();
        drop(state);

        if events.is_empty() {
            return Ok(json!({"response": "No new events.", "decision": "noop"}));
        }

        let events_json = serde_json::to_string(&events)?;

        let rag_snippets = {
            let state = APP_STATE.lock().await;
            state.rag_search(format!("{}", events_json), 3).await?
        };

        let truth_snapshot = {
            let state = APP_STATE.lock().await;
            state.org_truth.clone()
        };

        let system = r#"You are the OrgBrain.
You maintain the Organization Truth (versioned), and produce a reasoning trace.

Use retrieved policy snippets if relevant.

Return STRICT JSON with keys:
- decision_id: stable string identifier for this decision (if new, create a new UUID string)
- decision: short label
- summary: a short summary of the decision/update
- rationale: why this decision/update was made (1-3 sentences)
- evidence: array of short evidence strings (may include relevant RAG snippets)
- assumptions: array of assumptions made
- response_text: what to say to the user
- confidence: number in [0,1]
- routing: object mapping agent_id -> one of ["full","summary","none"]
- org_updates: object mapping truth_id -> update_string (can be empty)
"#;

        let user = json!({
            "events": events,
            "rag": rag_snippets,
            "org_truth": truth_snapshot
        })
        .to_string();

        let out = openai_chat(system, &user).await?;
        let parsed: serde_json::Value = serde_json::from_str(&out)
            .or_else(|_| {
                let extracted = match extract_first_json_object(&out) {
                    Some(v) => v,
                    None => {
                        return Err(serde_json::Error::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "no json object found in orgbrain output",
                        )));
                    }
                };
                serde_json::from_str::<serde_json::Value>(&extracted)
            })
            .unwrap_or_else(|_| {
            json!({
                "rationale": "",
                "evidence": [],
                "assumptions": [],
                "decision": "respond",
                "response_text": out,
                "confidence": 0.5,
                "org_updates": {}
            })
        });

        let decision = parsed
            .get("decision_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let decision_label = parsed
            .get("decision")
            .and_then(|v| v.as_str())
            .unwrap_or("respond")
            .to_string();

        let summary = parsed
            .get("summary")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let rationale = parsed
            .get("rationale")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let evidence: Vec<String> = parsed
            .get("evidence")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let assumptions: Vec<String> = parsed
            .get("assumptions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let response_text = parsed
            .get("response_text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let confidence = parsed
            .get("confidence")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.5) as f32;

        let routing_val = parsed.get("routing").cloned().unwrap_or_else(|| json!({}));
        let routing_map: std::collections::HashMap<String, String> = routing_val
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("none").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut updated_nodes = Vec::new();
        if let Some(obj) = parsed.get("org_updates").and_then(|v| v.as_object()) {
            let mut state = APP_STATE.lock().await;
            for (k, v) in obj {
                let upd = v.as_str().unwrap_or("").to_string();
                if !upd.is_empty() {
                    state.update_org_truth(k, upd);
                    updated_nodes.push(k.clone());
                }
            }
        }

        let mut graph_updates = GraphUpdates {
            nodes: Vec::new(),
            edges: Vec::new(),
        };

        let final_decision_id = if decision.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            decision.clone()
        };

        let mut decision_version: i64 = 1;
        if let Some(client) = neo4j {
            let graph = client.graph();

            decision_version = next_decision_version(graph, &final_decision_id)
                .await
                .unwrap_or(1);

            if let Ok(upd) = persist_decision_version(
                graph,
                final_decision_id.clone(),
                decision_version,
                if summary.is_empty() { decision_label.clone() } else { summary.clone() },
                confidence as f64,
                events.iter().map(|e| e.event_id).collect(),
                events.iter().map(|e| e.emitted_by.0.clone()).collect(),
                routing_val.clone(),
            )
            .await
            {
                graph_updates.nodes.extend(upd.nodes);
                graph_updates.edges.extend(upd.edges);
            }

            for truth_id in &updated_nodes {
                let v = next_truth_version(graph, truth_id).await.unwrap_or(1);
                let content = {
                    let state = APP_STATE.lock().await;
                    state.latest_truth(truth_id).unwrap_or("").to_string()
                };

                if content.is_empty() {
                    continue;
                }

                if let Ok(upd) = persist_truth_version(
                    graph,
                    truth_id.clone(),
                    "org_truth".to_string(),
                    v,
                    content,
                    confidence as f64,
                    events.iter().map(|e| e.event_id).collect(),
                    events.iter().map(|e| e.emitted_by.0.clone()).collect(),
                    routing_val.clone(),
                )
                .await
                {
                    graph_updates.nodes.extend(upd.nodes);
                    graph_updates.edges.extend(upd.edges);
                }
            }
        }

        let trace = ReasoningTrace {
            decision_id: final_decision_id,
            topic: "general".to_string(),
            summary: if summary.is_empty() { decision_label.clone() } else { summary.clone() },
            version: decision_version,
            rationale,
            evidence,
            assumptions,
            trigger_events: events.iter().map(|e| e.event_id).collect(),
            agents_involved: events.iter().map(|e| e.emitted_by.clone()).collect(),
            graph_updates,
            routing: routing_map,
        };

        {
            let mut state = APP_STATE.lock().await;
            state.add_trace(trace);
        }

        if !response_text.is_empty() {
            println!("OrgBrain: {}", response_text);
            if let Ok(mp3) = elevenlabs_tts_to_mp3_bytes(&response_text).await {
                let _ = play_mp3_bytes(&mp3);
            } else {
                eprintln!("(TTS unavailable; set ELEVEN_API_KEY to enable speech)");
            }
        }

        Ok(json!({
            "decision": decision_label,
            "response_text": response_text,
            "confidence": confidence
        }))
    }

    async fn post_process(
        &self,
        context: &mut Context,
        result: &Result<serde_json::Value>,
    ) -> Result<ProcessResult<MyState>> {
        if let Ok(val) = result {
            context.set("brain_response", val.clone());
            Ok(ProcessResult::new(MyState::Success, "success".to_string()))
        } else {
            if let Err(e) = result {
                eprintln!("OrgBrainNode error: {e}");
            }
            Ok(ProcessResult::new(MyState::Failure, "failure".to_string()))
        }
    }
}

#[async_trait]
impl Node for EndNode {
    type State = MyState;

    async fn execute(&self, _context: &Context) -> Result<serde_json::Value> {
        Ok(json!({"status": "exit"}))
    }

    async fn post_process(
        &self,
        _context: &mut Context,
        _result: &Result<serde_json::Value>,
    ) -> Result<ProcessResult<MyState>> {
        Ok(ProcessResult::new(MyState::Exit, "exit".to_string()))
    }
}
