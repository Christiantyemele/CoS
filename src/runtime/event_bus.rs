use std::collections::VecDeque;

use crate::domain::Event;

#[derive(Debug, Default)]
pub struct EventBus {
    queue: VecDeque<Event>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn emit(&mut self, event: Event) {
        self.queue.push_back(event);
    }

    pub fn drain(&mut self) -> Vec<Event> {
        self.queue.drain(..).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
