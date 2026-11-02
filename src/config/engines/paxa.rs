//! Paxa Labs engine configuration.

use serde::{Deserialize, Serialize};

use super::super::default_true;

/// Paxa Labs cloud STT configuration (Thai, English, Mandarin).
///
/// Paxa is a paid cloud STT provider. API key required: either set
/// `api_key` here or via the `PAXA_API_KEY` env var.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaxaConfig {
    /// API key (`pxa_...`). If unset, falls back to the PAXA_API_KEY env var.
    #[serde(default)]
    pub api_key: Option<String>,

    /// API host. Default: "api.paxalabs.com". Override for a proxy or a
    /// regional endpoint. A bare host or a full `https://` URL both work.
    #[serde(default = "default_paxa_endpoint")]
    pub endpoint: String,

    /// Realtime model used by the streaming WebSocket path.
    /// Default: "paxa-stt-lite-realtime-v1-preview".
    #[serde(default = "default_paxa_model")]
    pub model: String,

    /// Recording model used by the batch path (streaming = false).
    /// Default: "paxa-stt-lite-v1-preview".
    #[serde(default = "default_paxa_batch_model")]
    pub batch_model: String,

    /// Language hint (BCP 47, e.g. "th" or "en"). Paxa treats it as a
    /// hint, not a filter. Unset means auto-detect.
    #[serde(default)]
    pub language: Option<String>,

    /// Streaming mode. true = WebSocket session with live partials
    /// (push-to-talk is auto-promoted to toggle, same as Soniox).
    /// false = send one request on release (push-to-talk compatible).
    #[serde(default = "default_true")]
    pub streaming: bool,

    /// Type partials at the cursor as they arrive (streaming mode only).
    /// false = only finalized turns are typed. Default: true.
    #[serde(default = "default_true")]
    pub type_partials: bool,

    /// "verbatim" (default) or "clean" (drops fillers, merges broken
    /// phrases). Streaming only.
    #[serde(default)]
    pub style: Option<String>,

    /// "spoken" (default) or "written" (keeps digits, units and ๆ in
    /// Thai). Streaming only.
    #[serde(default)]
    pub convention: Option<String>,

    /// Vocabulary bias terms (up to 50, 50 chars each). Streaming only.
    #[serde(default)]
    pub vocabulary: Option<Vec<String>>,
}

fn default_paxa_endpoint() -> String {
    "api.paxalabs.com".to_string()
}

fn default_paxa_model() -> String {
    "paxa-stt-lite-realtime-v1-preview".to_string()
}

fn default_paxa_batch_model() -> String {
    "paxa-stt-lite-v1-preview".to_string()
}

impl Default for PaxaConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            endpoint: default_paxa_endpoint(),
            model: default_paxa_model(),
            batch_model: default_paxa_batch_model(),
            language: None,
            streaming: true,
            type_partials: true,
            style: None,
            convention: None,
            vocabulary: None,
        }
    }
}
