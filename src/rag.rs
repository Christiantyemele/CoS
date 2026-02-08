use anyhow::Result;

use crate::app_state::APP_STATE;

pub async fn search_brain(query: String, k: usize) -> Result<Vec<String>> {
    let state = APP_STATE.lock().await;
    state.rag_search(query, k).await
}
