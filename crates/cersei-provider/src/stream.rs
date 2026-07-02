//! Stream accumulator: collects SSE stream events into a complete response.

use cersei_types::*;
use std::collections::HashMap;

/// Accumulates streaming events into content blocks.
pub struct StreamAccumulator {
    content_blocks: Vec<ContentBlock>,
    partial_text: HashMap<usize, String>,
    partial_json: HashMap<usize, String>,
    partial_thinking: HashMap<usize, String>,
    partial_signature: HashMap<usize, String>,
    redacted_thinking_data: HashMap<usize, String>,
    block_types: HashMap<usize, String>,
    tool_use_ids: HashMap<usize, String>,
    tool_use_names: HashMap<usize, String>,
    stop_reason: Option<StopReason>,
    usage: Usage,
    model: Option<String>,
    message_id: Option<String>,
}

impl StreamAccumulator {
    pub fn new() -> Self {
        Self {
            content_blocks: Vec::new(),
            partial_text: HashMap::new(),
            partial_json: HashMap::new(),
            partial_thinking: HashMap::new(),
            partial_signature: HashMap::new(),
            redacted_thinking_data: HashMap::new(),
            block_types: HashMap::new(),
            tool_use_ids: HashMap::new(),
            tool_use_names: HashMap::new(),
            stop_reason: None,
            usage: Usage::default(),
            model: None,
            message_id: None,
        }
    }

    /// Whether the provider has started responding (a `message_start` event
    /// has been accumulated). Before this point a stream error is
    /// indistinguishable from a failed request — nothing has been generated —
    /// so re-issuing the request is safe.
    pub fn has_started(&self) -> bool {
        self.message_id.is_some()
    }

    pub fn process_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::MessageStart { id, model } => {
                self.message_id = Some(id);
                self.model = Some(model);
            }
            StreamEvent::ContentBlockStart {
                index,
                block_type,
                id,
                name,
            } => {
                self.block_types.insert(index, block_type);
                if let Some(id) = id {
                    self.tool_use_ids.insert(index, id);
                }
                if let Some(name) = name {
                    self.tool_use_names.insert(index, name);
                }
            }
            StreamEvent::TextDelta { index, text } => {
                self.partial_text.entry(index).or_default().push_str(&text);
            }
            StreamEvent::InputJsonDelta {
                index,
                partial_json,
            } => {
                self.partial_json
                    .entry(index)
                    .or_default()
                    .push_str(&partial_json);
            }
            StreamEvent::ThinkingDelta { index, thinking } => {
                self.partial_thinking
                    .entry(index)
                    .or_default()
                    .push_str(&thinking);
            }
            StreamEvent::SignatureDelta { index, signature } => {
                self.partial_signature
                    .entry(index)
                    .or_default()
                    .push_str(&signature);
            }
            StreamEvent::RedactedThinking { index, data } => {
                self.redacted_thinking_data.insert(index, data);
                self.block_types.insert(index, "redacted_thinking".to_string());
            }
            StreamEvent::ContentBlockStop { index } => {
                let block_type = self.block_types.get(&index).cloned().unwrap_or_default();
                let block = match block_type.as_str() {
                    "text" => ContentBlock::Text {
                        text: self.partial_text.remove(&index).unwrap_or_default(),
                    },
                    "tool_use" => {
                        let json_str = self.partial_json.remove(&index).unwrap_or_default();
                        let input =
                            serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);
                        ContentBlock::ToolUse {
                            id: self.tool_use_ids.remove(&index).unwrap_or_default(),
                            name: self.tool_use_names.remove(&index).unwrap_or_default(),
                            input,
                        }
                    }
                    "thinking" => ContentBlock::Thinking {
                        thinking: self.partial_thinking.remove(&index).unwrap_or_default(),
                        signature: self.partial_signature.remove(&index).unwrap_or_default(),
                    },
                    "redacted_thinking" => ContentBlock::RedactedThinking {
                        data: self.redacted_thinking_data.remove(&index).unwrap_or_default(),
                    },
                    _ => ContentBlock::Text {
                        text: self.partial_text.remove(&index).unwrap_or_default(),
                    },
                };
                // Ensure we have enough slots
                while self.content_blocks.len() <= index {
                    self.content_blocks.push(ContentBlock::Text {
                        text: String::new(),
                    });
                }
                self.content_blocks[index] = block;
            }
            StreamEvent::MessageDelta { stop_reason, usage } => {
                if let Some(sr) = stop_reason {
                    self.stop_reason = Some(sr);
                }
                if let Some(u) = usage {
                    self.usage.merge(&u);
                }
            }
            StreamEvent::MessageStop => {}
            StreamEvent::Ping => {}
            StreamEvent::Error { .. } => {}
        }
    }

    pub fn into_response(self) -> Result<super::CompletionResponse> {
        let message = Message {
            role: Role::Assistant,
            content: if self.content_blocks.is_empty() {
                MessageContent::Text(String::new())
            } else {
                MessageContent::Blocks(self.content_blocks)
            },
            id: self.message_id,
            metadata: Some(MessageMetadata {
                model: self.model,
                usage: Some(self.usage.clone()),
                stop_reason: self.stop_reason.clone(),
                provider_data: serde_json::Value::Null,
            }),
        };

        Ok(super::CompletionResponse {
            message,
            usage: self.usage,
            stop_reason: self.stop_reason.unwrap_or(StopReason::EndTurn),
        })
    }

    /// Get accumulated text so far (for streaming display).
    pub fn current_text(&self) -> String {
        self.partial_text.values().cloned().collect()
    }
}

impl Default for StreamAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thinking_block_carries_signature_from_signature_delta() {
        // Anthropic streams a thinking block's signature as a separate
        // signature_delta event. If it isn't accumulated onto the
        // ContentBlock::Thinking, the block round-trips with an empty
        // signature and the API rejects the next turn with HTTP 400. See
        // https://github.com/pacifio/cersei/issues/21.
        let mut acc = StreamAccumulator::new();
        acc.process_event(StreamEvent::ContentBlockStart {
            index: 0,
            block_type: "thinking".to_string(),
            id: None,
            name: None,
        });
        acc.process_event(StreamEvent::ThinkingDelta {
            index: 0,
            thinking: "let me think...".to_string(),
        });
        acc.process_event(StreamEvent::SignatureDelta {
            index: 0,
            signature: "sig-abc".to_string(),
        });
        acc.process_event(StreamEvent::ContentBlockStop { index: 0 });

        let response = acc.into_response().unwrap();
        let blocks = match response.message.content {
            MessageContent::Blocks(blocks) => blocks,
            _ => panic!("expected block content"),
        };
        match &blocks[0] {
            ContentBlock::Thinking { thinking, signature } => {
                assert_eq!(thinking, "let me think...");
                assert_eq!(signature, "sig-abc");
            }
            other => panic!("expected Thinking block, got {other:?}"),
        }
    }

    #[test]
    fn redacted_thinking_block_preserves_opaque_data() {
        // redacted_thinking arrives fully formed via a single
        // RedactedThinking event (no ContentBlockStart, no deltas), but
        // the API still sends a matching ContentBlockStop. Previously
        // this block_type wasn't in the ContentBlockStop match, so it
        // fell into the wildcard arm and became an empty Text block,
        // losing the data that must round-trip unchanged. See
        // https://github.com/pacifio/cersei/issues/21.
        let mut acc = StreamAccumulator::new();
        acc.process_event(StreamEvent::RedactedThinking {
            index: 0,
            data: "opaque-blob".to_string(),
        });
        acc.process_event(StreamEvent::ContentBlockStop { index: 0 });

        let response = acc.into_response().unwrap();
        let blocks = match response.message.content {
            MessageContent::Blocks(blocks) => blocks,
            _ => panic!("expected block content"),
        };
        match &blocks[0] {
            ContentBlock::RedactedThinking { data } => {
                assert_eq!(data, "opaque-blob");
            }
            other => panic!("expected RedactedThinking block, got {other:?}"),
        }
    }
}
