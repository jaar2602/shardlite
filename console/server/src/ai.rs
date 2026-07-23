//! OpenAI-compatible provider settings for the assistant. `base_url` and `model` are not secret;
//! the API key is **sealed** with the same [`Sealer`](crate::crypto::Sealer) as every other secret
//! and never leaves the server. Persisted to `ai.json`.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::crypto::Sealer;
use crate::store::write_atomic;

/// Non-secret view returned by the API (never the key).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AiSettings {
    pub base_url: String,
    pub model: String,
    pub enabled: bool,
    /// Whether an API key is stored (so the UI can show configured/not without revealing it).
    pub has_key: bool,
    /// Per-turn cap on tool calls; 0 means the default (8).
    pub max_tool_calls: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Stored {
    base_url: String,
    model: String,
    enabled: bool,
    max_tool_calls: u32,
    sealed_key: Option<String>,
}

/// The decrypted provider config the harness needs.
#[derive(Debug, Clone)]
pub struct ResolvedAi {
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    pub max_tool_calls: u32,
}

pub struct AiConfig {
    path: PathBuf,
    sealer: Sealer,
    inner: RwLock<Stored>,
}

impl AiConfig {
    pub fn open(path: &Path, sealer: Sealer) -> Result<Self, String> {
        let inner = if path.exists() {
            serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            Stored::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            sealer,
            inner: RwLock::new(inner),
        })
    }

    pub fn settings(&self) -> AiSettings {
        let s = self.inner.read().unwrap();
        AiSettings {
            base_url: s.base_url.clone(),
            model: s.model.clone(),
            enabled: s.enabled,
            has_key: s.sealed_key.is_some(),
            max_tool_calls: s.max_tool_calls,
        }
    }

    /// The provider config, or `None` if the assistant is not fully configured/enabled.
    pub fn resolved(&self) -> Option<ResolvedAi> {
        let s = self.inner.read().unwrap();
        if !s.enabled || s.base_url.is_empty() || s.model.is_empty() {
            return None;
        }
        let key = s
            .sealed_key
            .as_ref()
            .and_then(|k| self.sealer.open(k))
            .map(|b| String::from_utf8_lossy(&b).into_owned())?;
        Some(ResolvedAi {
            base_url: s.base_url.trim_end_matches('/').to_string(),
            model: s.model.clone(),
            api_key: key,
            max_tool_calls: if s.max_tool_calls == 0 {
                8
            } else {
                s.max_tool_calls
            },
        })
    }

    /// Update settings. `api_key` follows preserve-on-omit: `None` keeps the stored key, `Some("")`
    /// clears it, `Some(k)` seals and stores it.
    pub fn update(
        &self,
        base_url: String,
        model: String,
        enabled: bool,
        max_tool_calls: u32,
        api_key: Option<String>,
    ) -> Result<(), String> {
        let mut s = self.inner.write().unwrap();
        s.base_url = base_url;
        s.model = model;
        s.enabled = enabled;
        s.max_tool_calls = max_tool_calls;
        match api_key {
            Some(k) if k.is_empty() => s.sealed_key = None,
            Some(k) => s.sealed_key = Some(self.sealer.seal(k.as_bytes())),
            None => {}
        }
        let bytes = serde_json::to_vec_pretty(&*s).map_err(|e| e.to_string())?;
        write_atomic(&self.path, &bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    fn scopeguard(path: &Path) -> Cleanup {
        Cleanup(path.to_path_buf())
    }

    #[test]
    fn key_is_sealed_and_preserved_on_omit() {
        let path = std::env::temp_dir().join(format!(
            "shardlite-console-ai-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _cleanup = scopeguard(&path);
        let cfg = AiConfig::open(&path, Sealer::from_passphrase("test-key")).unwrap();

        cfg.update(
            "https://api.openai.com/v1".into(),
            "gpt-4o".into(),
            true,
            0,
            Some("sk-secret".into()),
        )
        .unwrap();

        // The plaintext key is never on disk.
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert!(!on_disk.contains("sk-secret"), "the key must be sealed on disk");

        // resolved() decrypts it; settings() never reveals it.
        let resolved = cfg.resolved().unwrap();
        assert_eq!(resolved.api_key, "sk-secret");
        assert_eq!(resolved.max_tool_calls, 8);
        assert!(cfg.settings().has_key);

        // Editing with no key preserves it.
        cfg.update("https://api.openai.com/v1".into(), "gpt-4o-mini".into(), true, 5, None)
            .unwrap();
        let resolved = cfg.resolved().unwrap();
        assert_eq!(resolved.api_key, "sk-secret");
        assert_eq!(resolved.model, "gpt-4o-mini");
        assert_eq!(resolved.max_tool_calls, 5);

        // Reopening reads it back through the seal.
        let reopened = AiConfig::open(&path, Sealer::from_passphrase("test-key")).unwrap();
        assert_eq!(reopened.resolved().unwrap().api_key, "sk-secret");

        // Disabled → no resolved config.
        cfg.update("https://api.openai.com/v1".into(), "gpt-4o".into(), false, 0, None)
            .unwrap();
        assert!(cfg.resolved().is_none());
    }
}
