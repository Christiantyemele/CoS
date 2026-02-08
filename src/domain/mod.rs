use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct EmployeeAgentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmployeeRole {
    Ceo,
    Hr,
    Engineer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct PrivateStoreKey(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    DecisionSignal,
    Update,
    Concern,
    Clarification,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Event {
    pub event_id: Uuid,
    pub emitted_by: EmployeeAgentId,
    pub event_type: EventType,
    pub topic: String,
    pub timestamp: DateTime<Utc>,
    pub confidence: f32,
    pub references: Vec<PrivateStoreKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RoutingDirective {
    pub agent_id: EmployeeAgentId,
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GraphUpdates {
    pub nodes: Vec<String>,
    pub edges: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReasoningTrace {
    pub decision_id: String,
    pub topic: String,
    pub summary: String,
    pub version: i64,
    pub rationale: String,
    pub evidence: Vec<String>,
    pub assumptions: Vec<String>,
    pub trigger_events: Vec<Uuid>,
    pub agents_involved: Vec<EmployeeAgentId>,
    pub graph_updates: GraphUpdates,
    pub routing: HashMap<String, String>,
}

impl Event {
    pub fn new(
        emitted_by: EmployeeAgentId,
        event_type: EventType,
        topic: String,
        confidence: f32,
        references: Vec<PrivateStoreKey>,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            emitted_by,
            event_type,
            topic,
            timestamp: Utc::now(),
            confidence,
            references,
        }
    }
}
