//! Paxa Labs cloud STT backend (Thai, English, Mandarin).
//!
//! Two paths, mirroring the Soniox backend:
//!
//! - [`StreamingTranscriber::start_stream`]: realtime session over
//!   `wss://<endpoint>/v1/stt/live`. Exposed when `[paxa] streaming = true`
//!   (the default). 16 kHz mono `pcm_s16le` binary frames go up; the server
//!   sends `transcript` events per *turn* (`is_final: false` interims carry
//!   the whole turn so far, one `is_final: true` closes the turn). On stop
//!   we send `{"type":"end"}`, which closes the open turn, and drain until
//!   `done`.
//!
//! - [`Transcriber::transcribe`]: one-shot `POST /v1/audio/transcriptions`
//!   (OpenAI-compatible multipart). Used when `streaming = false` and for
//!   `voxtype transcribe <file>` / meeting chunks.
//!
//! ## Turn reconciliation
//!
//! Voxtype's streaming output types deltas at the cursor. Paxa interims are
//! cumulative per turn, so the reconciler keeps the text already typed for
//! the open turn and emits only stable extensions as `Partial`. A turn's
//! final is diffed against that typed tail: a pure extension becomes
//! `Final`, a divergence becomes `Replace` (backspace the wrong tail, type
//! the corrected one). Turns after the first are joined with one space.

use super::streaming::{SegmentId, StreamHandle, StreamingEvent, StreamingTranscriber};
use super::Transcriber;
use crate::config::PaxaConfig;
use crate::error::TranscribeError;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

const SAMPLE_RATE: u32 = 16_000;

/// Connect / request timeout. Covers flat network faults only.
const PAXA_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait for `done` after sending `end`.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Paxa rejects binary frames over 64 KiB (close code 1009). 32 KiB is
/// about one second of 16 kHz pcm_s16le.
const MAX_FRAME_BYTES: usize = 32 * 1024;

const END_FRAME: &str = r#"{"type":"end"}"#;

const SEGMENT_ID: SegmentId = 0;

#[derive(Debug)]
pub struct PaxaTranscriber {
    config: PaxaConfig,
    api_key: String,
    http: std::sync::OnceLock<reqwest::Client>,
}

/// `(ws_url, http_base)` from the configured endpoint. Accepts a bare host
/// (`api.paxalabs.com`) or a URL; `http://` maps to plain `ws://` so a local
/// proxy works without TLS.
fn endpoint_urls(endpoint: &str) -> (String, String) {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let (insecure, host) = if let Some(rest) = trimmed.strip_prefix("http://") {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix("ws://") {
        (true, rest)
    } else {
        let rest = trimmed
            .strip_prefix("https://")
            .or_else(|| trimmed.strip_prefix("wss://"))
            .unwrap_or(trimmed);
        (false, rest)
    };
    if insecure {
        (format!("ws://{}", host), format!("http://{}", host))
    } else {
        (format!("wss://{}", host), format!("https://{}", host))
    }
}

impl PaxaTranscriber {
    pub fn new(config: PaxaConfig) -> Result<Self, TranscribeError> {
        let api_key = config
            .api_key
            .clone()
            .filter(|k| !k.trim().is_empty())
            .or_else(|| std::env::var("PAXA_API_KEY").ok())
            .filter(|k| !k.trim().is_empty())
            .ok_or_else(|| {
                TranscribeError::ConfigError(
                    "Paxa API key required: set [paxa] api_key or PAXA_API_KEY".into(),
                )
            })?;
        tracing::info!(
            "Paxa backend configured: endpoint={}, streaming={}, model={}, batch_model={}, language={}",
            config.endpoint,
            config.streaming,
            config.model,
            config.batch_model,
            config.language.as_deref().unwrap_or("auto"),
        );
        Ok(Self {
            config,
            api_key,
            http: std::sync::OnceLock::new(),
        })
    }

    fn http_client(&self) -> Result<&reqwest::Client, TranscribeError> {
        if let Some(c) = self.http.get() {
            return Ok(c);
        }
        let client = reqwest::Client::builder()
            .timeout(PAXA_TIMEOUT)
            .build()
            .map_err(|e| {
                TranscribeError::InferenceFailed(format!("reqwest client build failed: {}", e))
            })?;
        let _ = self.http.set(client);
        Ok(self.http.get().expect("just initialized"))
    }

    fn start_frame(&self) -> String {
        let mut obj = serde_json::json!({
            "type": "start",
            "model": self.config.model,
            "audio": { "encoding": "pcm_s16le", "sample_rate": SAMPLE_RATE },
        });
        let non_empty = |s: &Option<String>| s.as_ref().filter(|v| !v.trim().is_empty()).cloned();
        if let Some(lang) = non_empty(&self.config.language) {
            obj["language"] = serde_json::Value::String(lang);
        }
        if let Some(style) = non_empty(&self.config.style) {
            obj["style"] = serde_json::Value::String(style);
        }
        if let Some(conv) = non_empty(&self.config.convention) {
            obj["convention"] = serde_json::Value::String(conv);
        }
        if let Some(vocab) = &self.config.vocabulary {
            let terms: Vec<serde_json::Value> = vocab
                .iter()
                .map(|t| t.trim())
                .filter(|t| !t.is_empty())
                .map(|t| serde_json::Value::String(t.to_string()))
                .collect();
            if !terms.is_empty() {
                obj["vocabulary"] = serde_json::Value::Array(terms);
            }
        }
        obj.to_string()
    }

    async fn batch_transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        let (_, http_base) = endpoint_urls(&self.config.endpoint);
        let url = format!("{}/v1/audio/transcriptions", http_base);
        let wav = encode_wav_s16le(samples)?;
        let part = reqwest::multipart::Part::bytes(wav)
            .file_name("audio.wav")
            .mime_str("audio/wav")
            .map_err(|e| TranscribeError::InferenceFailed(format!("Paxa: multipart: {}", e)))?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", self.config.batch_model.clone())
            .text("response_format", "json");
        if let Some(lang) = self
            .config
            .language
            .as_ref()
            .filter(|l| !l.trim().is_empty())
        {
            form = form.text("language", lang.clone());
        }
        let resp = self
            .http_client()?
            .post(&url)
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| {
                TranscribeError::InferenceFailed(format!("Paxa: request failed: {}", e))
            })?;
        let status = resp.status();
        let credits = resp
            .headers()
            .get("x-credits-charged")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = resp
            .text()
            .await
            .map_err(|e| TranscribeError::InferenceFailed(format!("Paxa: read body: {}", e)))?;
        if !status.is_success() {
            return Err(TranscribeError::InferenceFailed(format!(
                "Paxa HTTP {}: {}",
                status, body
            )));
        }
        #[derive(Deserialize)]
        struct BatchResponse {
            text: String,
        }
        let parsed: BatchResponse = serde_json::from_str(&body).map_err(|e| {
            TranscribeError::InferenceFailed(format!("Paxa: bad response ({}): {}", e, body))
        })?;
        if let Some(c) = credits {
            tracing::debug!("Paxa batch charged {} credits", c);
        }
        Ok(parsed.text.trim().to_string())
    }
}

fn encode_wav_s16le(samples: &[f32]) -> Result<Vec<u8>, TranscribeError> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: SAMPLE_RATE,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::with_capacity(44 + samples.len() * 2));
    {
        let mut writer = hound::WavWriter::new(&mut cursor, spec)
            .map_err(|e| TranscribeError::AudioFormat(format!("WAV writer: {}", e)))?;
        for &s in samples {
            writer
                .write_sample(f32_to_i16(s))
                .map_err(|e| TranscribeError::AudioFormat(format!("WAV write: {}", e)))?;
        }
        writer
            .finalize()
            .map_err(|e| TranscribeError::AudioFormat(format!("WAV finalize: {}", e)))?;
    }
    Ok(cursor.into_inner())
}

fn f32_to_i16(s: f32) -> i16 {
    (s.clamp(-1.0, 1.0) * i16::MAX as f32).round() as i16
}

fn f32_to_s16le_bytes(samples: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(samples.len() * 2);
    for &s in samples {
        out.extend_from_slice(&f32_to_i16(s).to_le_bytes());
    }
    out
}

fn common_prefix_char_count(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

// === Paxa realtime protocol ===

#[derive(Deserialize, Debug, Default)]
struct ServerEvent {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    turn: Option<u64>,
    #[serde(default)]
    is_final: bool,
    #[serde(default)]
    text: String,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    credits: Option<f64>,
    #[serde(default)]
    total_credits: Option<f64>,
}

#[derive(Debug, Default)]
struct ReconcilerOutput {
    events: Vec<StreamingEvent>,
    terminate: bool,
}

/// Converts Paxa turn-based transcript events into voxtype cursor deltas.
#[derive(Debug, Default)]
struct Reconciler {
    /// Text already typed at the cursor for the open turn (prefix included).
    typed_partial: String,
    /// Turn currently being typed; 0 before the first transcript.
    turn: u64,
    /// Highest turn whose final has been applied.
    finalized_turn: u64,
    /// Separator for the open turn (" " once anything has been committed).
    prefix: String,
    /// Whether any non-empty text has been committed this session.
    committed_any: bool,
}

impl Reconciler {
    fn open_turn(&mut self, turn: u64) {
        if !self.typed_partial.is_empty() {
            // Previous turn never got a final; its partial stays on screen.
            self.committed_any = true;
            self.typed_partial.clear();
        }
        self.turn = turn;
        self.prefix = if self.committed_any {
            " ".to_string()
        } else {
            String::new()
        };
    }

    fn process(&mut self, ev: &ServerEvent, type_partials: bool) -> ReconcilerOutput {
        let mut out = ReconcilerOutput::default();
        match ev.kind.as_str() {
            "transcript" => {
                let turn = ev.turn.unwrap_or(self.turn.max(1));
                if turn <= self.finalized_turn || turn < self.turn {
                    return out;
                }
                if turn > self.turn {
                    self.open_turn(turn);
                }
                let text = ev.text.trim();
                let full = if text.is_empty() {
                    String::new()
                } else {
                    format!("{}{}", self.prefix, text)
                };

                if ev.is_final {
                    self.finalized_turn = turn;
                    if full.starts_with(&self.typed_partial) {
                        let delta = full[self.typed_partial.len()..].to_string();
                        if !delta.is_empty() {
                            out.events.push(StreamingEvent::Final {
                                text: delta,
                                segment_id: SEGMENT_ID,
                            });
                        }
                    } else {
                        let lcp = common_prefix_char_count(&self.typed_partial, &full);
                        let backspace = self.typed_partial.chars().count() - lcp;
                        let tail: String = full.chars().skip(lcp).collect();
                        tracing::debug!(
                            "Paxa turn {} revision: backspace {}, type {:?}",
                            turn,
                            backspace,
                            tail
                        );
                        out.events.push(StreamingEvent::Replace {
                            backspace,
                            text: tail,
                            segment_id: SEGMENT_ID,
                        });
                    }
                    if !full.is_empty() {
                        self.committed_any = true;
                    }
                    self.typed_partial.clear();
                } else if type_partials && !full.is_empty() && full.starts_with(&self.typed_partial)
                {
                    let delta = full[self.typed_partial.len()..].to_string();
                    if !delta.is_empty() {
                        out.events.push(StreamingEvent::Partial {
                            text: delta,
                            segment_id: SEGMENT_ID,
                        });
                        self.typed_partial = full;
                    }
                }
            }
            "error" => {
                let code = ev.code.as_deref().unwrap_or("unknown");
                if let Some(turn) = ev.turn {
                    // Per-turn failure: the connection stays open.
                    tracing::warn!(
                        "Paxa: turn {} failed ({}){}",
                        turn,
                        code,
                        ev.message
                            .as_deref()
                            .map(|m| format!(": {}", m))
                            .unwrap_or_default()
                    );
                } else {
                    out.events
                        .push(StreamingEvent::Error(TranscribeError::InferenceFailed(
                            format!(
                                "Paxa server error ({}){}",
                                code,
                                ev.message
                                    .as_deref()
                                    .map(|m| format!(": {}", m))
                                    .unwrap_or_default()
                            ),
                        )));
                    out.terminate = true;
                }
            }
            "charged" => {
                tracing::debug!(
                    "Paxa charged {:?} credits (session total {:?})",
                    ev.credits,
                    ev.total_credits
                );
            }
            "done" => {
                tracing::info!("Paxa session done: {:?} credits", ev.total_credits);
                out.terminate = true;
            }
            _ => {}
        }
        out
    }
}

impl Transcriber for PaxaTranscriber {
    fn transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        if samples.is_empty() {
            return Err(TranscribeError::AudioFormat("Empty audio buffer".into()));
        }
        let run = self.batch_transcribe(samples);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(run)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        TranscribeError::InferenceFailed(format!("Failed to create runtime: {}", e))
                    })?;
                rt.block_on(run)
            }
        }
    }

    fn as_streaming(&self) -> Option<&dyn StreamingTranscriber> {
        self.config.streaming.then_some(self as _)
    }
}

impl StreamingTranscriber for PaxaTranscriber {
    fn start_stream(
        &self,
        samples_rx: mpsc::Receiver<Vec<f32>>,
    ) -> Result<StreamHandle, TranscribeError> {
        let (events_tx, events_rx) = mpsc::channel::<StreamingEvent>(64);
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let (ws_url, _) = endpoint_urls(&self.config.endpoint);
        let mut request = format!("{}/v1/stt/live", ws_url)
            .into_client_request()
            .map_err(|e| TranscribeError::ConfigError(format!("Paxa: bad endpoint: {}", e)))?;
        let auth = HeaderValue::from_str(&format!("Bearer {}", self.api_key))
            .map_err(|_| TranscribeError::ConfigError("Paxa: invalid API key".into()))?;
        request.headers_mut().insert("Authorization", auth);

        let start = self.start_frame();
        let type_partials = self.config.type_partials;

        let task = tokio::spawn(async move {
            run_streaming_session(
                request,
                start,
                type_partials,
                samples_rx,
                events_tx,
                cancel_rx,
            )
            .await
        });

        Ok(StreamHandle {
            events: events_rx,
            cancel: cancel_tx,
            task,
        })
    }
}

async fn send_fatal(events_tx: &mpsc::Sender<StreamingEvent>, msg: String) {
    let _ = events_tx
        .send(StreamingEvent::Error(TranscribeError::InferenceFailed(msg)))
        .await;
    let _ = events_tx.send(StreamingEvent::Ended).await;
}

async fn run_streaming_session(
    request: tokio_tungstenite::tungstenite::handshake::client::Request,
    start_frame: String,
    type_partials: bool,
    mut samples_rx: mpsc::Receiver<Vec<f32>>,
    events_tx: mpsc::Sender<StreamingEvent>,
    mut cancel_rx: oneshot::Receiver<()>,
) -> Result<(), TranscribeError> {
    let ws_stream =
        match tokio::time::timeout(PAXA_TIMEOUT, tokio_tungstenite::connect_async(request)).await {
            Ok(Ok((s, _))) => s,
            Ok(Err(e)) => {
                send_fatal(&events_tx, format!("Paxa: WS connect failed: {}", e)).await;
                return Ok(());
            }
            Err(_) => {
                send_fatal(&events_tx, "Paxa: connect timeout".into()).await;
                return Ok(());
            }
        };
    let (mut write, mut read) = ws_stream.split();

    tracing::debug!(target: "voxtype::paxa::wire", "-> {}", start_frame);
    if let Err(e) = write.send(Message::Text(start_frame)).await {
        send_fatal(&events_tx, format!("Paxa: send start failed: {}", e)).await;
        return Ok(());
    }

    let mut reconciler = Reconciler::default();
    let mut samples_closed = false;
    let mut drain_deadline: Option<tokio::time::Instant> = None;

    loop {
        let drain_timer = async {
            match drain_deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            biased;

            _ = &mut cancel_rx => {
                tracing::debug!("Paxa streaming session cancelled");
                break;
            }

            _ = drain_timer, if drain_deadline.is_some() => {
                tracing::warn!(
                    "Paxa drain timeout ({}s) after end; trailing text may be lost",
                    DRAIN_TIMEOUT.as_secs()
                );
                break;
            }

            chunk = samples_rx.recv(), if !samples_closed => {
                match chunk {
                    Some(c) if !c.is_empty() => {
                        let bytes = f32_to_s16le_bytes(&c);
                        let mut failed = false;
                        for frame in bytes.chunks(MAX_FRAME_BYTES) {
                            if let Err(e) = write.send(Message::Binary(frame.to_vec())).await {
                                let _ = events_tx.send(StreamingEvent::Error(
                                    TranscribeError::InferenceFailed(format!(
                                        "Paxa: send audio failed: {}", e
                                    ))
                                )).await;
                                failed = true;
                                break;
                            }
                        }
                        if failed {
                            break;
                        }
                    }
                    Some(_) => {}
                    None => {
                        samples_closed = true;
                        if let Err(e) = write.send(Message::Text(END_FRAME.into())).await {
                            tracing::warn!("Paxa: end send failed: {}", e);
                        }
                        drain_deadline = Some(tokio::time::Instant::now() + DRAIN_TIMEOUT);
                        tracing::debug!("Paxa: sent end; draining");
                    }
                }
            }

            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        let _ = events_tx.send(StreamingEvent::Error(
                            TranscribeError::InferenceFailed(format!("Paxa: WS error: {}", e))
                        )).await;
                        break;
                    }
                    None => break,
                };
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Close(frame) => {
                        if let Some(f) = frame {
                            let code: u16 = f.code.into();
                            if code != 1000 {
                                let _ = events_tx.send(StreamingEvent::Error(
                                    TranscribeError::InferenceFailed(format!(
                                        "Paxa closed the session ({}): {}", code, f.reason
                                    ))
                                )).await;
                            }
                        }
                        break;
                    }
                    _ => continue,
                };
                tracing::debug!(target: "voxtype::paxa::wire", "<- {}", text);
                let parsed: ServerEvent = match serde_json::from_str(&text) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!("Paxa: unparseable message ({}): {}", e, text);
                        continue;
                    }
                };
                let out = reconciler.process(&parsed, type_partials);
                let mut send_failed = false;
                for ev in out.events {
                    if events_tx.send(ev).await.is_err() {
                        send_failed = true;
                        break;
                    }
                }
                if send_failed || out.terminate {
                    break;
                }
            }
        }
    }

    let _ = write.send(Message::Close(None)).await;
    let _ = events_tx.send(StreamingEvent::Ended).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tr(turn: u64, is_final: bool, text: &str) -> ServerEvent {
        ServerEvent {
            kind: "transcript".into(),
            turn: Some(turn),
            is_final,
            text: text.into(),
            ..Default::default()
        }
    }

    fn extract(events: &[StreamingEvent]) -> Vec<(&'static str, usize, String)> {
        events
            .iter()
            .map(|e| match e {
                StreamingEvent::Partial { text, .. } => ("partial", 0, text.clone()),
                StreamingEvent::Final { text, .. } => ("final", 0, text.clone()),
                StreamingEvent::Replace {
                    backspace, text, ..
                } => ("replace", *backspace, text.clone()),
                StreamingEvent::Ended => ("ended", 0, String::new()),
                StreamingEvent::Error(_) => ("error", 0, String::new()),
            })
            .collect()
    }

    #[test]
    fn endpoint_urls_handles_bare_host_and_schemes() {
        assert_eq!(
            endpoint_urls("api.paxalabs.com"),
            (
                "wss://api.paxalabs.com".into(),
                "https://api.paxalabs.com".into()
            )
        );
        assert_eq!(
            endpoint_urls("https://api.paxalabs.com/"),
            (
                "wss://api.paxalabs.com".into(),
                "https://api.paxalabs.com".into()
            )
        );
        assert_eq!(
            endpoint_urls("http://localhost:8080"),
            ("ws://localhost:8080".into(), "http://localhost:8080".into())
        );
    }

    #[test]
    fn interims_type_only_stable_extensions() {
        let mut r = Reconciler::default();
        let a = r.process(&tr(1, false, "วันนี้"), true);
        assert_eq!(extract(&a.events), vec![("partial", 0, "วันนี้".into())]);
        let b = r.process(&tr(1, false, "วันนี้ผมจะ"), true);
        assert_eq!(extract(&b.events), vec![("partial", 0, "ผมจะ".into())]);
        // Revision of the typed tail is dropped until the final resolves it.
        let c = r.process(&tr(1, false, "วันนั้น"), true);
        assert!(c.events.is_empty());
    }

    #[test]
    fn final_extending_partial_types_only_tail() {
        let mut r = Reconciler::default();
        r.process(&tr(1, false, "deploy API"), true);
        let f = r.process(&tr(1, true, "deploy API ขึ้น production"), true);
        assert_eq!(
            extract(&f.events),
            vec![("final", 0, " ขึ้น production".into())]
        );
    }

    #[test]
    fn diverging_final_backspaces_by_unicode_scalars() {
        let mut r = Reconciler::default();
        r.process(&tr(1, false, "ลิงค์"), true);
        let f = r.process(&tr(1, true, "ลิงก์"), true);
        // "ลิง" shared; "ค์" (2 scalars) replaced by "ก์".
        assert_eq!(extract(&f.events), vec![("replace", 2, "ก์".into())]);
    }

    #[test]
    fn later_turns_are_space_separated() {
        let mut r = Reconciler::default();
        r.process(&tr(1, true, "สวัสดีครับ"), true);
        let p = r.process(&tr(2, false, "hello"), true);
        assert_eq!(extract(&p.events), vec![("partial", 0, " hello".into())]);
        let f = r.process(&tr(2, true, "hello world"), true);
        assert_eq!(extract(&f.events), vec![("final", 0, " world".into())]);
    }

    #[test]
    fn stale_turn_events_are_ignored() {
        let mut r = Reconciler::default();
        r.process(&tr(1, true, "one"), true);
        assert!(r.process(&tr(1, false, "one more"), true).events.is_empty());
        assert!(r.process(&tr(1, true, "one again"), true).events.is_empty());
    }

    #[test]
    fn type_partials_false_types_finals_only() {
        let mut r = Reconciler::default();
        assert!(r.process(&tr(1, false, "abc"), false).events.is_empty());
        let f = r.process(&tr(1, true, "abcd"), false);
        assert_eq!(extract(&f.events), vec![("final", 0, "abcd".into())]);
    }

    #[test]
    fn empty_final_after_typed_partial_backspaces_it() {
        let mut r = Reconciler::default();
        r.process(&tr(1, false, "อืม"), true);
        let f = r.process(&tr(1, true, ""), true);
        assert_eq!(extract(&f.events), vec![("replace", 3, String::new())]);
    }

    #[test]
    fn turn_error_keeps_session_but_fatal_error_terminates() {
        let mut r = Reconciler::default();
        let turn_err = ServerEvent {
            kind: "error".into(),
            code: Some("provider_error".into()),
            turn: Some(2),
            ..Default::default()
        };
        let o = r.process(&turn_err, true);
        assert!(!o.terminate && o.events.is_empty());
        let fatal = ServerEvent {
            kind: "error".into(),
            code: Some("unauthorized".into()),
            ..Default::default()
        };
        let o = r.process(&fatal, true);
        assert!(o.terminate);
        assert_eq!(extract(&o.events), vec![("error", 0, String::new())]);
    }

    #[test]
    fn done_terminates() {
        let mut r = Reconciler::default();
        let o = r.process(
            &ServerEvent {
                kind: "done".into(),
                ..Default::default()
            },
            true,
        );
        assert!(o.terminate);
    }

    #[test]
    fn start_frame_includes_optional_fields_only_when_set() {
        let mut cfg = PaxaConfig {
            api_key: Some("pxa_test".into()),
            ..PaxaConfig::default()
        };
        let t = PaxaTranscriber::new(cfg.clone()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&t.start_frame()).unwrap();
        assert_eq!(v["type"], "start");
        assert_eq!(v["audio"]["sample_rate"], 16000);
        assert!(v.get("language").is_none());
        assert!(v.get("vocabulary").is_none());

        cfg.language = Some("th".into());
        cfg.convention = Some("written".into());
        cfg.vocabulary = Some(vec!["Paxa Labs".into(), "  ".into()]);
        let t = PaxaTranscriber::new(cfg).unwrap();
        let v: serde_json::Value = serde_json::from_str(&t.start_frame()).unwrap();
        assert_eq!(v["language"], "th");
        assert_eq!(v["convention"], "written");
        assert_eq!(v["vocabulary"], serde_json::json!(["Paxa Labs"]));
        // The key travels in the Authorization header, never the frame.
        assert!(!t.start_frame().contains("pxa_test"));
    }

    #[test]
    fn wav_encoding_has_riff_header() {
        let wav = encode_wav_s16le(&[0.0, 0.5, -0.5]).unwrap();
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(wav.len(), 44 + 6);
    }
}
