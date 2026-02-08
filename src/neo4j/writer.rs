use anyhow::{Context as _, Result};
use neo4rs::{query, Graph};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphUpdateResult {
    pub nodes: Vec<String>,
    pub edges: Vec<String>,
}

pub fn canonical_employee_id_from_email(email: &str) -> String {
    let mut out = String::with_capacity(email.len() + 15);
    out.push_str("employee_email_");
    for ch in email.trim().to_lowercase().chars() {
        match ch {
            'a'..='z' | '0'..='9' => out.push(ch),
            _ => out.push('_'),
        }
    }
    out
}

pub async fn merge_employee_from_email(
    graph: &Graph,
    email: &str,
    display_name: Option<&str>,
) -> Result<String> {
    let employee_id = canonical_employee_id_from_email(email);
    let name = display_name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| email.trim().to_string());

    let q = query(
        r#"
MERGE (e:Employee {employee_id: $employee_id})
ON CREATE SET e.created_at = datetime()
SET e.name = coalesce(e.name, $name),
    e.email = coalesce(e.email, $email)
RETURN elementId(e) AS node_id
"#,
    )
    .param("employee_id", employee_id)
    .param("name", name)
    .param("email", email.trim().to_lowercase());

    let mut stream = graph.execute(q).await.context("merge employee")?;
    let row = stream
        .next()
        .await
        .context("read merge employee")?
        .context("merge employee returned no row")?;
    let node_id: String = row.get("node_id").context("missing employee node_id")?;
    Ok(node_id)
}

pub async fn persist_email_message(
    graph: &Graph,
    message_id: &str,
    file: &str,
    subject: &str,
    date: &str,
    from_employee_id: &str,
    to_employee_ids: &[String],
    topic_ids: &[String],
) -> Result<GraphUpdateResult> {
    let mut txn = graph.start_txn().await.context("start email txn")?;

    let q = query(
        r#"
MERGE (m:EmailMessage {message_id: $message_id})
ON CREATE SET m.created_at = datetime()
SET m.file = $file,
    m.subject = $subject,
    m.date = $date
WITH m
MERGE (sender:Employee {employee_id: $from_employee_id})
MERGE (sender)-[:SENT]->(m)
WITH m, sender
UNWIND $to_employee_ids AS to_id
MERGE (r:Employee {employee_id: to_id})
MERGE (m)-[:TO]->(r)
WITH m, sender
UNWIND $to_employee_ids AS to_id
MERGE (r:Employee {employee_id: to_id})
MERGE (sender)-[cw:COMMUNICATES_WITH]->(r)
ON CREATE SET cw.created_at = datetime(), cw.count = 0
SET cw.count = coalesce(cw.count, 0) + 1
WITH m
UNWIND $topic_ids AS tid
MERGE (t:Topic {topic_id: tid})
ON CREATE SET t.created_at = datetime(), t.topic = tid
MERGE (m)-[:ABOUT]->(t)
MERGE (m)-[:DEPENDS_ON]->(t)
RETURN elementId(m) AS message_node_id
"#,
    )
    .param("message_id", message_id.to_string())
    .param("file", file.to_string())
    .param("subject", subject.to_string())
    .param("date", date.to_string())
    .param("from_employee_id", from_employee_id.to_string())
    .param("to_employee_ids", to_employee_ids.to_vec())
    .param("topic_ids", topic_ids.to_vec());

    let mut stream = txn
        .execute(q)
        .await
        .context("persist email message")?;
    let row = stream
        .next(txn.handle())
        .await
        .context("read persist email message")?
        .context("persist email message returned no row")?;
    let message_node_id: String = row
        .get("message_node_id")
        .context("missing message_node_id")?;

    txn.commit().await.context("commit email txn")?;

    Ok(GraphUpdateResult {
        nodes: vec![message_node_id],
        edges: Vec::new(),
    })
}

pub async fn persist_knowledge_cluster(
    graph: &Graph,
    cluster_id: &str,
    label: &str,
    member_message_ids: &[String],
) -> Result<GraphUpdateResult> {
    let mut txn = graph.start_txn().await.context("start cluster txn")?;

    let q = query(
        r#"
MERGE (c:KnowledgeCluster {cluster_id: $cluster_id})
ON CREATE SET c.created_at = datetime()
SET c.name = $label
WITH c
UNWIND $member_message_ids AS mid
MATCH (m:EmailMessage {message_id: mid})
MERGE (m)-[:IN_CLUSTER]->(c)
RETURN elementId(c) AS cluster_node_id
"#,
    )
    .param("cluster_id", cluster_id.to_string())
    .param("label", label.to_string())
    .param("member_message_ids", member_message_ids.to_vec());

    let mut stream = txn
        .execute(q)
        .await
        .context("persist knowledge cluster")?;
    let row = stream
        .next(txn.handle())
        .await
        .context("read persist knowledge cluster")?
        .context("persist knowledge cluster returned no row")?;
    let cluster_node_id: String = row
        .get("cluster_node_id")
        .context("missing cluster_node_id")?;

    txn.commit().await.context("commit cluster txn")?;

    Ok(GraphUpdateResult {
        nodes: vec![cluster_node_id],
        edges: Vec::new(),
    })
}

impl GraphUpdateResult {
    pub fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

pub async fn seed_employees(graph: &Graph) -> Result<()> {
    // Idempotent seed. These employees become the canonical identities for the UI.
    // Note: neo4rs params must be Bolt-compatible (avoid passing serde_json::Value).
    let employees = [
        ("employee_john", "John", "ceo"),
        ("employee_sarah", "Sarah", "hr"),
        ("employee_bob", "Bob", "engineer"),
    ];

    for (employee_id, name, role) in employees {
        let q = query(
            r#"
MERGE (emp:Employee {employee_id: $employee_id})
ON CREATE SET emp.created_at = datetime()
SET emp.name = $name,
    emp.role = $role
"#,
        )
        .param("employee_id", employee_id.to_string())
        .param("name", name.to_string())
        .param("role", role.to_string());

        graph
            .run(q)
            .await
            .with_context(|| format!("seed employee {employee_id}"))?;
    }
    Ok(())
}

pub async fn persist_conversation_turn(
    graph: &Graph,
    employee_id: &str,
    role: &str,
    content: &str,
) -> Result<()> {
    let q = query(
        r#"
MATCH (e:Employee {employee_id: $employee_id})
CREATE (t:ConversationTurn {
  turn_id: $turn_id,
  created_at: datetime(),
  role: $role,
  content: $content
})
MERGE (e)-[:SAID]->(t)
"#,
    )
    .param("employee_id", employee_id.to_string())
    .param("turn_id", Uuid::new_v4().to_string())
    .param("role", role.to_string())
    .param("content", content.to_string());

    graph
        .run(q)
        .await
        .context("persist conversation turn")?;
    Ok(())
}

pub async fn load_recent_conversation_turns(
    graph: &Graph,
    employee_id: &str,
    limit: i64,
) -> Result<Vec<(String, String)>> {
    let q = query(
        r#"
MATCH (:Employee {employee_id: $employee_id})-[:SAID]->(t:ConversationTurn)
RETURN t.role AS role, t.content AS content
ORDER BY t.created_at DESC
LIMIT $limit
"#,
    )
    .param("employee_id", employee_id.to_string())
    .param("limit", limit);

    let mut stream = graph.execute(q).await.context("load recent conversation")?;
    let mut out = Vec::new();
    while let Ok(Some(row)) = stream.next().await {
        let role: String = row.get("role").unwrap_or_else(|_| "user".to_string());
        let content: String = row.get("content").unwrap_or_default();
        out.push((role, content));
    }
    Ok(out)
}

fn routing_to_json(routing: &Value) -> String {
    serde_json::to_string(routing).unwrap_or_else(|_| "{}".to_string())
}

fn routing_agents(routing: &Value) -> Vec<String> {
    routing
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    let level = v.as_str().unwrap_or("none");
                    if level == "none" {
                        None
                    } else {
                        Some(k.clone())
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub async fn next_decision_version(graph: &Graph, decision_id: &str) -> Result<i64> {
    let mut stream = graph
        .execute(
            query(
                r#"
MATCH (d:Decision {decision_id: $decision_id})-[:CURRENT]->(dv:DecisionVersion)
RETURN dv.version AS v
"#,
            )
            .param("decision_id", decision_id.to_string()),
        )
        .await
        .context("query current decision version")?;

    if let Some(row) = stream.next().await.context("read decision version")? {
        let v: i64 = row.get("v").context("missing decision version")?;
        Ok(v + 1)
    } else {
        Ok(1)
    }
}

pub async fn next_truth_version(graph: &Graph, truth_id: &str) -> Result<i64> {
    let mut stream = graph
        .execute(
            query(
                r#"
MATCH (o:TruthObject {truth_id: $truth_id})-[:CURRENT]->(tv:TruthVersion)
RETURN tv.version AS v
"#,
            )
            .param("truth_id", truth_id.to_string()),
        )
        .await
        .context("query current truth version")?;

    if let Some(row) = stream.next().await.context("read truth version")? {
        let v: i64 = row.get("v").context("missing truth version")?;
        Ok(v + 1)
    } else {
        Ok(1)
    }
}

pub async fn persist_decision_version(
    graph: &Graph,
    decision_id: String,
    version: i64,
    summary: String,
    confidence: f64,
    trigger_events: Vec<Uuid>,
    agents_involved: Vec<String>,
    routing: Value,
) -> Result<GraphUpdateResult> {
    let routing_json = routing_to_json(&routing);
    let routing_agents = routing_agents(&routing);
    let decision_version_id = format!("{}:v{}", decision_id.clone(), version);
    let mut txn = graph.start_txn().await.context("start neo4j txn")?;

    // MERGE decision and CREATE version.
    // Note: we set CURRENT pointer transactionally by deleting existing CURRENT and creating new.
    let q = query(
        r#"
MERGE (d:Decision {decision_id: $decision_id})
ON CREATE SET d.created_at = datetime()
CREATE (dv:DecisionVersion {
  decision_version_id: $decision_version_id,
  decision_id: $decision_id,
  version: $version,
  created_at: datetime(),
  summary: $summary,
  confidence: $confidence,
  trigger_events: $trigger_events,
  agents_involved: $agents_involved,
  routing_agents: $routing_agents,
  routing_json: $routing_json
})
WITH d, dv
OPTIONAL MATCH (d)-[c:CURRENT]->(old:DecisionVersion)
FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END | DELETE c)
MERGE (d)-[:CURRENT]->(dv)
WITH d, dv, old
FOREACH (_ IN CASE WHEN old IS NULL THEN [] ELSE [1] END | MERGE (dv)-[:SUPERSEDES]->(old))
WITH d, dv
UNWIND $agents_involved AS aid
MERGE (e:Employee {employee_id: aid})
MERGE (e)-[:PARTICIPATED_IN]->(dv)
RETURN elementId(d) AS decision_node_id, elementId(dv) AS version_node_id
"#,
    )
    .param("decision_id", decision_id)
    .param("decision_version_id", decision_version_id)
    .param("version", version)
    .param("summary", summary)
    .param("confidence", confidence)
    .param(
        "trigger_events",
        trigger_events
            .into_iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>(),
    )
    .param("agents_involved", agents_involved)
    .param("routing_agents", routing_agents)
    .param("routing_json", routing_json);

    let mut stream = txn.execute(q).await.context("execute persist_decision_version")?;
    let row = stream
        .next(txn.handle())
        .await
        .context("read persist_decision_version result")?
        .context("persist_decision_version returned no row")?;

    let decision_node_id: String = row.get("decision_node_id").context("missing decision_node_id")?;
    let version_node_id: String = row.get("version_node_id").context("missing version_node_id")?;

    txn.commit().await.context("commit persist_decision_version")?;

    Ok(GraphUpdateResult {
        nodes: vec![decision_node_id, version_node_id],
        edges: Vec::new(),
    })
}

pub async fn persist_truth_version(
    graph: &Graph,
    truth_id: String,
    kind: String,
    version: i64,
    summary: String,
    confidence: f64,
    trigger_events: Vec<Uuid>,
    agents_involved: Vec<String>,
    routing: Value,
) -> Result<GraphUpdateResult> {
    let routing_json = routing_to_json(&routing);
    let routing_agents = routing_agents(&routing);
    let truth_version_id = format!("{}:v{}", truth_id.clone(), version);
    let mut txn = graph.start_txn().await.context("start neo4j txn")?;

    let q = query(
        r#"
MERGE (o:TruthObject {truth_id: $truth_id})
ON CREATE SET o.created_at = datetime(), o.kind = $kind
ON MATCH SET o.kind = coalesce(o.kind, $kind)
CREATE (tv:TruthVersion {
  truth_version_id: $truth_version_id,
  truth_id: $truth_id,
  version: $version,
  created_at: datetime(),
  summary: $summary,
  confidence: $confidence,
  trigger_events: $trigger_events,
  agents_involved: $agents_involved,
  routing_agents: $routing_agents,
  routing_json: $routing_json
})
WITH o, tv
OPTIONAL MATCH (o)-[c:CURRENT]->(old:TruthVersion)
FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END | DELETE c)
MERGE (o)-[:CURRENT]->(tv)
WITH o, tv, old
FOREACH (_ IN CASE WHEN old IS NULL THEN [] ELSE [1] END | MERGE (tv)-[:SUPERSEDES]->(old))
WITH o, tv
UNWIND $agents_involved AS aid
MERGE (e:Employee {employee_id: aid})
MERGE (e)-[:PARTICIPATED_IN]->(tv)
RETURN elementId(o) AS truth_node_id, elementId(tv) AS version_node_id
"#,
    )
    .param("truth_id", truth_id)
    .param("kind", kind)
    .param("truth_version_id", truth_version_id)
    .param("version", version)
    .param("summary", summary)
    .param("confidence", confidence)
    .param(
        "trigger_events",
        trigger_events
            .into_iter()
            .map(|u| u.to_string())
            .collect::<Vec<_>>(),
    )
    .param("agents_involved", agents_involved)
    .param("routing_agents", routing_agents)
    .param("routing_json", routing_json);

    let mut stream = txn.execute(q).await.context("execute persist_truth_version")?;
    let row = stream
        .next(txn.handle())
        .await
        .context("read persist_truth_version result")?
        .context("persist_truth_version returned no row")?;

    let truth_node_id: String = row.get("truth_node_id").context("missing truth_node_id")?;
    let version_node_id: String = row.get("version_node_id").context("missing version_node_id")?;

    txn.commit().await.context("commit persist_truth_version")?;

    Ok(GraphUpdateResult {
        nodes: vec![truth_node_id, version_node_id],
        edges: Vec::new(),
    })
}
