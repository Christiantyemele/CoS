use anyhow::{Context as _, Result};
use neo4rs::{query, Graph};

pub async fn run_migrations(graph: &Graph) -> Result<()> {
    let mut txn = graph.start_txn().await.context("start neo4j txn")?;

    // Uniqueness constraints
    let statements = [
        // Employee
        "CREATE CONSTRAINT employee_employee_id IF NOT EXISTS FOR (e:Employee) REQUIRE e.employee_id IS UNIQUE",
        // Team
        "CREATE CONSTRAINT team_team_id IF NOT EXISTS FOR (t:Team) REQUIRE t.team_id IS UNIQUE",
        // Topic
        "CREATE CONSTRAINT topic_topic_id IF NOT EXISTS FOR (t:Topic) REQUIRE t.topic_id IS UNIQUE",
        // Decision
        "CREATE CONSTRAINT decision_decision_id IF NOT EXISTS FOR (d:Decision) REQUIRE d.decision_id IS UNIQUE",
        // DecisionVersion (community edition: use synthetic unique id)
        "CREATE CONSTRAINT decision_version_id IF NOT EXISTS FOR (dv:DecisionVersion) REQUIRE dv.decision_version_id IS UNIQUE",
        // TruthObject
        "CREATE CONSTRAINT truth_object_truth_id IF NOT EXISTS FOR (o:TruthObject) REQUIRE o.truth_id IS UNIQUE",
        // TruthVersion (community edition: use synthetic unique id)
        "CREATE CONSTRAINT truth_version_id IF NOT EXISTS FOR (tv:TruthVersion) REQUIRE tv.truth_version_id IS UNIQUE",
        // ConversationTurn
        "CREATE CONSTRAINT conversation_turn_id IF NOT EXISTS FOR (t:ConversationTurn) REQUIRE t.turn_id IS UNIQUE",
        // EmailMessage
        "CREATE CONSTRAINT email_message_id IF NOT EXISTS FOR (m:EmailMessage) REQUIRE m.message_id IS UNIQUE",
        // KnowledgeCluster
        "CREATE CONSTRAINT knowledge_cluster_id IF NOT EXISTS FOR (c:KnowledgeCluster) REQUIRE c.cluster_id IS UNIQUE",
    ];

    for stmt in statements {
        txn.run(query(stmt))
            .await
            .with_context(|| format!("neo4j migration failed: {stmt}"))?;
    }

    txn.commit().await.context("commit neo4j migrations")?;
    Ok(())
}
