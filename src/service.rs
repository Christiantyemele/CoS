use anyhow::Result;
use serde_json::json;

use crate::app_state::APP_STATE;
use crate::domain::{EmployeeAgentId, Event, EventType, GraphUpdates, ReasoningTrace};
use crate::neo4j::writer::{
    next_decision_version, next_truth_version, persist_decision_version, persist_truth_version,
    load_recent_conversation_turns, persist_conversation_turn,
};
use crate::utils::openai_chat;
use rrag::prelude::Document;
use uuid::Uuid;

fn extract_first_json_object(s: &str) -> Option<String> {
    let start = s.find('{')?;
    let end = s.rfind('}')?;
    if end <= start {
        return None;
    }
    Some(s[start..=end].to_string())
}

pub async fn ingest_knowledge(
    truth_id: String,
    kind: String,
    content: String,
    agent_id: Option<String>,
    routing: serde_json::Value,
    add_to_rag: bool,
) -> Result<ReasoningTrace> {
    let agent_id = EmployeeAgentId(agent_id.unwrap_or_else(|| "employee_1".to_string()));
    let trigger_event = Uuid::new_v4();

    let mut graph_updates = GraphUpdates {
        nodes: Vec::new(),
        edges: Vec::new(),
    };

    let (rag, neo4j) = {
        let mut state = APP_STATE.lock().await;
        state.update_org_truth(&truth_id, content.clone());
        (state.rag.clone(), state.neo4j.clone())
    };

    if add_to_rag {
        if let Some(rag) = rag {
            let rag = rag.lock().await;
            let doc = Document::new(content.clone())
                .with_metadata("source", "frontend".into())
                .with_metadata("truth_id", truth_id.clone().into())
                .with_metadata("kind", kind.clone().into())
                .with_content_hash();
            let _ = rag.process_document(doc).await;
        }
    }

    let version = if let Some(client) = neo4j {
        let graph = client.graph();
        let version = next_truth_version(graph, &truth_id).await.unwrap_or(1);
        if let Ok(upd) = persist_truth_version(
            graph,
            truth_id.clone(),
            kind,
            version,
            content.clone(),
            1.0,
            vec![trigger_event],
            vec![agent_id.0.clone()],
            routing.clone(),
        )
        .await
        {
            graph_updates.nodes.extend(upd.nodes);
            graph_updates.edges.extend(upd.edges);
        }
        version
    } else {
        1
    };

    Ok(ReasoningTrace {
        decision_id: truth_id,
        topic: "knowledge".to_string(),
        summary: content,
        version,
        rationale: "knowledge_ingest".to_string(),
        evidence: Vec::new(),
        assumptions: Vec::new(),
        trigger_events: vec![trigger_event],
        agents_involved: vec![agent_id],
        graph_updates,
        routing: routing
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("none").to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

pub async fn ask_and_persist(text: String, agent_id: Option<String>) -> Result<(String, ReasoningTrace)> {
    let agent_id = EmployeeAgentId(agent_id.unwrap_or_else(|| "employee_1".to_string()));

    // Load recent per-employee conversation context (Neo4j-backed, cached in memory).
    let (neo4j, cached) = {
        let state = APP_STATE.lock().await;
        (state.neo4j.clone(), state.conversation_cache.get(&agent_id).cloned())
    };
    let mut memory_turns = cached.unwrap_or_default();
    if memory_turns.is_empty() {
        if let Some(client) = neo4j.clone() {
            let graph = client.graph();
            if let Ok(turns) = load_recent_conversation_turns(graph, &agent_id.0, 20).await {
                // stored DESC; reverse for chronological.
                memory_turns = turns.into_iter().rev().collect();
            }
        }
    }

    let memory_context = if memory_turns.is_empty() {
        String::new()
    } else {
        let mut s = String::from("Prior conversation (most recent last):\n");
        for (role, content) in memory_turns.iter() {
            s.push_str(&format!("- {}: {}\n", role, content));
        }
        s
    };

    let employee_system = r#"You are an EmployeeAgent.
Given the user's input, emit a single event for the OrgBrain to process.

Return STRICT JSON with keys:
- event_type: one of ["decision_signal","update","concern","clarification"]
- topic: short topic string
- confidence: number in [0,1]
- private_note: a short private note (may include sensitive/rough thoughts)
"#;

    let employee_user = if memory_context.is_empty() {
        text.clone()
    } else {
        format!("{}\n\nUser: {}", memory_context, text)
    };
    let employee_out = openai_chat(employee_system, &employee_user).await?;
    let employee_parsed: serde_json::Value = serde_json::from_str(&employee_out)
        .or_else(|_| {
            let extracted = extract_first_json_object(&employee_out)
                .ok_or_else(|| serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "no json object found in employee output",
                )))?;
            serde_json::from_str(&extracted)
        })
        .unwrap_or_else(|_| {
            json!({
                "event_type": "update",
                "topic": "general",
                "confidence": 0.5,
                "private_note": employee_out
            })
        });

    let event_type = match employee_parsed
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("update")
    {
        "decision_signal" => EventType::DecisionSignal,
        "concern" => EventType::Concern,
        "clarification" => EventType::Clarification,
        _ => EventType::Update,
    };

    let topic = employee_parsed
        .get("topic")
        .and_then(|v| v.as_str())
        .unwrap_or("general")
        .to_string();
    let confidence = employee_parsed
        .get("confidence")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.5) as f32;
    let private_note = employee_parsed
        .get("private_note")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let mut state = APP_STATE.lock().await;
    let private_key = state.store_private(&agent_id, private_note);
    let event = Event::new(
        agent_id.clone(),
        event_type,
        topic.clone(),
        confidence,
        vec![private_key],
    );
    let event_id = event.event_id;
    state.emit(event);

    let events = state.drain_events();
    let neo4j = state.neo4j.clone();
    drop(state);

    let events_json = serde_json::to_string(&events)?;

    let rag_snippets = {
        let state = APP_STATE.lock().await;
        state.rag_search(format!("{}", events_json), 3).await?
    };

    let truth_snapshot = {
        let state = APP_STATE.lock().await;
        state.org_truth.clone()
    };

    let org_system = r#"You are the OrgBrain.
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

    let org_user = json!({
        "events": events,
        "rag": rag_snippets,
        "org_truth": truth_snapshot
    })
    .to_string();

    let org_out = openai_chat(org_system, &org_user).await?;
    let org_parsed: serde_json::Value = serde_json::from_str(&org_out)
        .or_else(|_| {
            let extracted = extract_first_json_object(&org_out)
                .ok_or_else(|| serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "no json object found in orgbrain output",
                )))?;
            serde_json::from_str(&extracted)
        })
        .unwrap_or_else(|_| {
            json!({
                "decision_id": "",
                "decision": "respond",
                "summary": "",
                "rationale": "",
                "evidence": [],
                "assumptions": [],
                "response_text": org_out,
                "confidence": 0.5,
                "routing": {},
                "org_updates": {}
            })
        });

    let decision_id_in = org_parsed
        .get("decision_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let decision_label = org_parsed
        .get("decision")
        .and_then(|v| v.as_str())
        .unwrap_or("respond")
        .to_string();
    let summary = org_parsed
        .get("summary")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let rationale = org_parsed
        .get("rationale")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let evidence: Vec<String> = org_parsed
        .get("evidence")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let assumptions: Vec<String> = org_parsed
        .get("assumptions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let response_text = org_parsed
        .get("response_text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let routing_val = org_parsed.get("routing").cloned().unwrap_or_else(|| json!({}));

    let routing_map: std::collections::HashMap<String, String> = routing_val
        .as_object()
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("none").to_string()))
                .collect()
        })
        .unwrap_or_default();

    let mut updated_truth_ids = Vec::new();
    if let Some(obj) = org_parsed.get("org_updates").and_then(|v| v.as_object()) {
        let mut state = APP_STATE.lock().await;
        for (k, v) in obj {
            let upd = v.as_str().unwrap_or("").to_string();
            if !upd.is_empty() {
                state.update_org_truth(k, upd);
                updated_truth_ids.push(k.clone());
            }
        }
    }

    let final_decision_id = if decision_id_in.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        decision_id_in
    };

    let mut graph_updates = GraphUpdates {
        nodes: Vec::new(),
        edges: Vec::new(),
    };

    let mut decision_version = 1i64;
    if let Some(client) = neo4j.clone() {
        let graph = client.graph();

        decision_version = next_decision_version(graph, &final_decision_id)
            .await
            .unwrap_or(1);

        if let Ok(upd) = persist_decision_version(
            graph,
            final_decision_id.clone(),
            decision_version,
            if summary.is_empty() {
                decision_label.clone()
            } else {
                summary.clone()
            },
            confidence as f64,
            vec![event_id],
            vec![agent_id.0.clone()],
            routing_val.clone(),
        )
        .await
        {
            graph_updates.nodes.extend(upd.nodes);
            graph_updates.edges.extend(upd.edges);
        }

        for truth_id in &updated_truth_ids {
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
                vec![event_id],
                vec![agent_id.0.clone()],
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
        topic: topic.clone(),
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
        state.add_trace(trace.clone());
    }

    // Persist per-employee memory (Neo4j-backed) and update in-memory cache.
    if let Some(client) = neo4j {
        let graph = client.graph();
        let _ = persist_conversation_turn(graph, &agent_id.0, "user", &text).await;
        let _ = persist_conversation_turn(graph, &agent_id.0, "assistant", &response_text).await;
    }
    {
        let mut state = APP_STATE.lock().await;
        let entry = state.conversation_cache.entry(agent_id.clone()).or_default();
        entry.push(("user".to_string(), text));
        entry.push(("assistant".to_string(), response_text.clone()));
        if entry.len() > 40 {
            let keep_from = entry.len() - 40;
            *entry = entry.split_off(keep_from);
        }
    }

    Ok((response_text, trace))
}
