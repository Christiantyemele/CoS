use std::collections::HashMap;

use anyhow::Result;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;

use rrag::prelude::*;
use std::env;
use std::fs::File;
use std::path::Path;
use uuid::Uuid;

use crate::domain::{EmployeeAgentId, Event, PrivateStoreKey, ReasoningTrace};
use crate::neo4j::Neo4jClient;
use crate::neo4j::writer::{
    merge_employee_from_email, persist_email_message, persist_knowledge_cluster, seed_employees,
};
use crate::runtime::event_bus::EventBus;

pub static APP_STATE: Lazy<Mutex<AppState>> = Lazy::new(|| Mutex::new(AppState::new()));

type PrivateMem = HashMap<PrivateStoreKey, String>;

pub struct AppState {
    pub event_bus: EventBus,
    pub private_store: HashMap<EmployeeAgentId, PrivateMem>,
    pub org_truth: HashMap<String, Vec<String>>,
    pub traces: Vec<ReasoningTrace>,
    pub conversation_cache: HashMap<EmployeeAgentId, Vec<(String, String)>>,
    pub rag: Option<Arc<Mutex<RragSystem>>>,
    pub neo4j: Option<Neo4jClient>,
    private_seq: u64,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            event_bus: EventBus::new(),
            private_store: HashMap::new(),
            org_truth: HashMap::new(),
            traces: Vec::new(),
            conversation_cache: HashMap::new(),
            rag: None,
            neo4j: None,
            private_seq: 0,
        }
    }

    pub async fn init_neo4j(&mut self) -> Result<()> {
        let client = Neo4jClient::connect_from_env().await?;
        client.run_migrations().await?;
        seed_employees(client.graph()).await?;
        self.neo4j = Some(client);
        Ok(())
    }

    pub async fn init_rag(&mut self) -> Result<()> {
        let rag = RragSystemBuilder::new()
            .with_name("OrgBrain")
            .with_environment("development")
            .build()
            .await?;

        let max_docs: usize = env::var("RAG_MAX_DOCS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        let path = Path::new("knowledge.csv");
        if path.exists() {
            let file = File::open(path)?;
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .flexible(true)
                .from_reader(file);

            let mut ingested = 0usize;
            let neo4j = self.neo4j.clone();

            let cluster_enabled = env::var("OPENAI_API_KEY")
                .ok()
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);

            let cluster_sim_threshold: f32 = env::var("ORG_EMAIL_CLUSTER_SIM")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.85);

            let mut cluster_centroids: Vec<Vec<f32>> = Vec::new();
            let mut cluster_members: Vec<Vec<String>> = Vec::new();
            let mut cluster_labels: Vec<String> = Vec::new();

            for result in rdr.records() {
                let record = result?;
                let file_name = record.get(0).unwrap_or("").to_string();
                let message = record.get(1).unwrap_or("").to_string();

                if message.trim().is_empty() {
                    continue;
                }

                if let Some(client) = neo4j.clone() {
                    let graph = client.graph();

                    let parsed = parse_email_blob(&message);
                    if let Some(from_email) = parsed.from_email.as_deref() {
                        let _ = merge_employee_from_email(
                            graph,
                            from_email,
                            parsed.from_name.as_deref(),
                        )
                        .await;
                    }

                    for (to_email, to_name) in parsed.to_emails.iter() {
                        let _ = merge_employee_from_email(graph, to_email, to_name.as_deref()).await;
                    }

                    let from_employee_id = parsed
                        .from_email
                        .as_deref()
                        .map(crate::neo4j::writer::canonical_employee_id_from_email)
                        .unwrap_or_else(|| "employee_email_unknown".to_string());

                    let to_employee_ids: Vec<String> = parsed
                        .to_emails
                        .iter()
                        .map(|(e, _)| crate::neo4j::writer::canonical_employee_id_from_email(e))
                        .collect();

                    let topic_ids = derive_topics(&parsed.subject);
                    let msg_id = parsed
                        .message_id
                        .clone()
                        .unwrap_or_else(|| file_name.clone());

                    let _ = persist_email_message(
                        graph,
                        &msg_id,
                        &file_name,
                        parsed.subject.as_deref().unwrap_or(""),
                        parsed.date.as_deref().unwrap_or(""),
                        &from_employee_id,
                        &to_employee_ids,
                        &topic_ids,
                    )
                    .await;

                    if cluster_enabled {
                        let text = build_embedding_text(
                            parsed.subject.as_deref().unwrap_or(""),
                            &parsed.body,
                        );
                        if let Ok(emb) = openai_embedding(&text).await {
                            assign_to_clusters(
                                msg_id.clone(),
                                &topic_ids,
                                emb,
                                cluster_sim_threshold,
                                &mut cluster_centroids,
                                &mut cluster_members,
                                &mut cluster_labels,
                            );
                        }
                    }
                }

                let doc = Document::new(message)
                    .with_metadata("source", "knowledge.csv".into())
                    .with_metadata("file", file_name.into())
                    .with_content_hash();
                rag.process_document(doc).await?;

                ingested += 1;
                if ingested >= max_docs {
                    break;
                }
            }

            if cluster_enabled {
                if let Some(client) = neo4j {
                    let graph = client.graph();
                    for (idx, member_ids) in cluster_members.iter().enumerate() {
                        if member_ids.len() < 2 {
                            continue;
                        }
                        let cluster_id = format!("cluster_{}", Uuid::new_v4());
                        let label = cluster_labels
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| "cluster".to_string());
                        let _ = persist_knowledge_cluster(graph, &cluster_id, &label, member_ids).await;
                    }
                }
            }
        } else {
            let docs = [
                ("org_policy", "Company policy: decisions should be communicated with a short summary, confidence, and references."),
                ("product", "Product roadmap: prioritize reliability, testability, and clear ownership of decisions."),
                ("engineering", "Engineering guidelines: prefer small changes, add logging for debugging, and avoid breaking APIs."),
            ];

            for (source, text) in docs {
                let doc = Document::new(text)
                    .with_metadata("source", source.into())
                    .with_content_hash();
                rag.process_document(doc).await?;
            }
        }

        self.rag = Some(Arc::new(Mutex::new(rag)));
        Ok(())
    }

    pub fn store_private(&mut self, agent: &EmployeeAgentId, content: String) -> PrivateStoreKey {
        self.private_seq += 1;
        let key = PrivateStoreKey(format!("{}:{}", agent.0, self.private_seq));
        self.private_store
            .entry(agent.clone())
            .or_default()
            .insert(key.clone(), content);
        key
    }

    pub fn emit(&mut self, event: Event) {
        self.event_bus.emit(event);
    }

    pub fn drain_events(&mut self) -> Vec<Event> {
        self.event_bus.drain()
    }

    pub fn update_org_truth(&mut self, node: &str, content: String) {
        self.org_truth.entry(node.to_string()).or_default().push(content);
    }

    pub fn latest_truth(&self, node: &str) -> Option<&str> {
        self.org_truth.get(node).and_then(|v| v.last().map(|s| s.as_str()))
    }

    pub fn add_trace(&mut self, trace: ReasoningTrace) {
        self.traces.push(trace);
    }

    pub async fn rag_search(&self, query: String, k: usize) -> Result<Vec<String>> {
        let Some(rag) = &self.rag else {
            return Ok(Vec::new());
        };
        let rag = rag.lock().await;
        let results = rag.search(query, Some(k)).await?;
        let mut out = Vec::new();
        for r in results.results {
            out.push(r.content);
        }
        Ok(out)
    }
}

#[derive(Debug, Default, Clone)]
struct ParsedEmail {
    message_id: Option<String>,
    date: Option<String>,
    subject: Option<String>,
    from_email: Option<String>,
    from_name: Option<String>,
    to_emails: Vec<(String, Option<String>)>,
    body: String,
}

fn parse_email_blob(message: &str) -> ParsedEmail {
    let mut out = ParsedEmail::default();
    let mut headers: HashMap<String, String> = HashMap::new();

    let mut in_headers = true;
    let mut body_lines: Vec<&str> = Vec::new();

    for line in message.lines() {
        if in_headers {
            if line.trim().is_empty() {
                in_headers = false;
                continue;
            }

            if let Some((k, v)) = line.split_once(':') {
                let key = k.trim().to_lowercase();
                let val = v.trim().to_string();
                headers
                    .entry(key)
                    .and_modify(|e| {
                        e.push(' ');
                        e.push_str(&val);
                    })
                    .or_insert(val);
            }
        } else {
            body_lines.push(line);
        }
    }

    out.body = body_lines.join("\n");

    out.message_id = headers
        .get("message-id")
        .cloned()
        .map(|s| s.trim().trim_matches('<').trim_matches('>').to_string());
    out.date = headers.get("date").cloned();
    out.subject = headers.get("subject").cloned();

    let x_from = headers.get("x-from").cloned();
    let from = headers.get("from").cloned().unwrap_or_default();
    let (from_email, from_name) = parse_name_email(&from).unwrap_or((None, None));
    out.from_email = from_email;
    out.from_name = x_from.or(from_name);

    let mut to_pairs = Vec::new();
    for key in ["to", "cc", "bcc"] {
        if let Some(v) = headers.get(key) {
            to_pairs.extend(parse_many_recipients(v));
        }
    }
    out.to_emails = to_pairs;

    out
}

fn parse_many_recipients(s: &str) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((email_opt, name_opt)) = parse_name_email(part) {
            if let Some(email) = email_opt {
                out.push((email, name_opt));
                continue;
            }
        }

        for email in extract_emails(part) {
            out.push((email, None));
        }
    }
    out
}

fn parse_name_email(s: &str) -> Option<(Option<String>, Option<String>)> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((left, right)) = trimmed.split_once('<') {
        let name = left.trim().trim_matches('"').to_string();
        let email = right
            .split_once('>')
            .map(|(e, _)| e.trim())
            .unwrap_or_else(|| right.trim());
        let email = email.to_lowercase();
        return Some((
            Some(email),
            if name.trim().is_empty() {
                None
            } else {
                Some(name)
            },
        ));
    }

    let emails = extract_emails(trimmed);
    if emails.len() == 1 {
        return Some((Some(emails[0].clone()), None));
    }

    Some((None, None))
}

fn extract_emails(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'@' {
            let mut l = i;
            while l > 0 {
                let c = bytes[l - 1] as char;
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    l -= 1;
                } else {
                    break;
                }
            }
            let mut r = i + 1;
            while r < bytes.len() {
                let c = bytes[r] as char;
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    r += 1;
                } else {
                    break;
                }
            }
            if l < i && r > i + 1 {
                let cand = &s[l..r];
                if cand.contains('.') {
                    out.push(cand.trim().to_lowercase());
                }
                i = r;
                continue;
            }
        }
        i += 1;
    }

    out.sort();
    out.dedup();
    out
}

fn derive_topics(subject: &Option<String>) -> Vec<String> {
    let subj = subject.clone().unwrap_or_default();
    let norm = subj.trim().to_lowercase();
    if norm.is_empty() {
        return vec!["(no subject)".to_string()];
    }
    vec![norm]
}

fn build_embedding_text(subject: &str, body: &str) -> String {
    let mut out = String::new();
    if !subject.trim().is_empty() {
        out.push_str("subject: ");
        out.push_str(subject.trim());
        out.push('\n');
    }
    out.push_str("body: ");
    let b = body.trim();
    if b.len() > 1200 {
        out.push_str(&b[..1200]);
    } else {
        out.push_str(b);
    }
    out
}

async fn openai_embedding(text: &str) -> Result<Vec<f32>> {
    let api_key = env::var("OPENAI_API_KEY")?;
    let model = env::var("OPENAI_EMBED_MODEL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "text-embedding-3-small".to_string());

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.openai.com/v1/embeddings")
        .bearer_auth(api_key)
        .json(&serde_json::json!({
            "model": model,
            "input": text
        }))
        .send()
        .await?
        .error_for_status()?;

    let v: serde_json::Value = resp.json().await?;
    let arr = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|x| x.get("embedding"))
        .and_then(|e| e.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing embedding"))?;

    let mut out = Vec::with_capacity(arr.len());
    for n in arr {
        if let Some(f) = n.as_f64() {
            out.push(f as f32);
        }
    }
    Ok(out)
}

fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0f32;
    let mut na = 0f32;
    let mut nb = 0f32;
    let len = a.len().min(b.len());
    for i in 0..len {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na <= 0.0 || nb <= 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

fn assign_to_clusters(
    message_id: String,
    topic_ids: &[String],
    emb: Vec<f32>,
    sim_threshold: f32,
    centroids: &mut Vec<Vec<f32>>,
    members: &mut Vec<Vec<String>>,
    labels: &mut Vec<String>,
) {
    let mut best_idx: Option<usize> = None;
    let mut best_sim = -1f32;
    for (i, c) in centroids.iter().enumerate() {
        let s = cosine_sim(c, &emb);
        if s > best_sim {
            best_sim = s;
            best_idx = Some(i);
        }
    }

    let label = topic_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "cluster".to_string());

    if best_idx.is_none() || best_sim < sim_threshold {
        centroids.push(emb);
        members.push(vec![message_id]);
        labels.push(label);
        return;
    }

    let idx = best_idx.unwrap();
    let k = members.get(idx).map(|m| m.len()).unwrap_or(1) as f32;
    if let Some(c) = centroids.get_mut(idx) {
        let len = c.len().min(emb.len());
        for i in 0..len {
            c[i] = (c[i] * k + emb[i]) / (k + 1.0);
        }
    }
    if let Some(m) = members.get_mut(idx) {
        m.push(message_id);
    }
    if labels.get(idx).map(|l| l.trim().is_empty()).unwrap_or(false) {
        if let Some(l) = labels.get_mut(idx) {
            *l = label;
        }
    }
}
