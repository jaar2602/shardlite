//! The OpenAI-compatible chat-completions provider, behind a trait so the harness is testable with a
//! scripted mock and the real provider is pure config (any `/v1/chat/completions` endpoint with tool
//! calling — OpenAI, Azure, or a self-hosted vLLM/Ollama/LiteLLM gateway).

use std::io::Read;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One chat message, in the OpenAI wire shape (role: system|user|assistant|tool).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// The chain-of-thought from a reasoning ("thinking mode") model. DeepSeek's deepseek-reasoner
    /// returns this alongside tool calls and **requires it to be sent back** on the assistant message
    /// that made those calls — omitting it fails the next request with "reasoning_content ... must be
    /// passed back". Non-reasoning providers never set it, so it is skipped when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Message {
    pub fn system(content: impl Into<String>) -> Self {
        Self::plain("system", content)
    }
    pub fn user(content: impl Into<String>) -> Self {
        Self::plain("user", content)
    }
    pub fn plain(role: &str, content: impl Into<String>) -> Self {
        Self {
            role: role.to_string(),
            content: Some(content.into()),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
            name: None,
        }
    }
    /// A `tool` message carrying one tool's result back to the model.
    pub fn tool_result(tool_call_id: &str, name: &str, content: String) -> Self {
        Self {
            role: "tool".into(),
            content: Some(content),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: Some(tool_call_id.to_string()),
            name: Some(name.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(default = "default_type")]
    pub r#type: String,
    pub function: FunctionCall,
}

fn default_type() -> String {
    "function".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    /// A JSON string of arguments (the OpenAI contract), parsed by the harness.
    pub arguments: String,
}

/// A tool the model may call, in the OpenAI `tools` shape.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// A JSON Schema object for the arguments.
    pub parameters: Value,
}

/// What the model returned for one step: either a final answer, tool calls, or both.
#[derive(Debug, Clone)]
pub struct Completion {
    pub content: Option<String>,
    /// Reasoning-model chain-of-thought, when the provider returns one. Preserved so the harness can
    /// echo it back on the assistant message (see [`Message::reasoning_content`]).
    pub reasoning_content: Option<String>,
    pub tool_calls: Vec<ToolCall>,
}

pub trait Provider: Send + Sync {
    fn chat(&self, model: &str, messages: &[Message], tools: &[ToolSpec])
    -> Result<Completion, String>;
}

/// The real provider: `POST {base_url}/chat/completions`.
pub struct OpenAiProvider {
    base_url: String,
    api_key: String,
}

impl OpenAiProvider {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
        }
    }
}

impl Provider for OpenAiProvider {
    fn chat(
        &self,
        model: &str,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Completion, String> {
        let tools_json: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();
        let mut body = serde_json::json!({ "model": model, "messages": messages });
        if !tools_json.is_empty() {
            body["tools"] = Value::Array(tools_json);
            body["tool_choice"] = Value::String("auto".into());
        }

        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(120))
            .build();
        let response = match agent
            .post(&format!("{}/chat/completions", self.base_url))
            .set("Authorization", &format!("Bearer {}", self.api_key))
            .set("Content-Type", "application/json")
            .send_json(body)
        {
            Ok(r) => r,
            Err(ureq::Error::Status(status, r)) => {
                let detail = r.into_string().unwrap_or_default();
                let detail: String = detail.chars().take(500).collect();
                return Err(format!("AI provider returned HTTP {status}: {detail}"));
            }
            Err(ureq::Error::Transport(e)) => {
                return Err(format!("cannot reach AI provider: {e}"));
            }
        };

        // Bound the response so a misbehaving provider can't exhaust memory.
        let mut bytes = Vec::new();
        response
            .into_reader()
            .take(8 * 1024 * 1024)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        parse_completion(&bytes)
    }
}

/// Parse a chat-completions response body into a [`Completion`]. Split out so it is unit-testable.
pub fn parse_completion(bytes: &[u8]) -> Result<Completion, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| format!("bad AI response: {e}"))?;
    let message = value
        .pointer("/choices/0/message")
        .ok_or("AI response had no choices")?;
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let reasoning_content = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let tool_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .filter_map(|c| serde_json::from_value::<ToolCall>(c.clone()).ok())
                .collect()
        })
        .unwrap_or_default();
    Ok(Completion {
        content,
        reasoning_content,
        tool_calls,
    })
}
