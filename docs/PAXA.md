# Paxa Labs Backend

Voxtype supports [Paxa Labs](https://paxalabs.com) as a cloud speech-to-text backend for Thai, English and Mandarin. Like Soniox, nothing runs on your machine: audio goes to Paxa's servers and text comes back.

## What is Paxa?

Paxa Labs is a Bangkok-based provider of speech and language models for Thai, English and Mandarin Chinese. Its speech-to-text API offers:

- **Thai-first recognition**, including Thai-English code-switching ("deploy เวอร์ชันใหม่ของ API")
- **Two modes:** a realtime WebSocket (`/v1/stt/live`) and an OpenAI-compatible recording endpoint (`/v1/audio/transcriptions`)
- **Thai written convention:** `convention = "written"` keeps digits, units and the repetition mark ๆ instead of spelled-out numbers
- **Vocabulary biasing:** up to 50 terms

Paxa's speech-to-text models are labeled research preview.

## Privacy

Audio is sent to a third-party service over TLS. **Use a local engine (Whisper, Parakeet) if your dictation contains anything you cannot send off-device.**

## Cost

Paxa is paid and billed in credits. At the time of writing, `GET /v1/models` lists the recording model at 500 credits per audio hour and the realtime model at 750, with a 0.1-credit minimum per request or connection. Check your Paxa dashboard for current rates.

## Requirements

- A Paxa API key (`pxa_...`)
- Outbound HTTPS / WebSocket (wss://) access

Paxa ships in every release binary; no build flag needed. No local model files, no GPU.

## Quick Start

1. Create an API key in the Paxa dashboard.
2. Export it:
   ```bash
   export PAXA_API_KEY="pxa_..."
   ```
3. Minimal config in `~/.config/voxtype/config.toml`:
   ```toml
   engine = "paxa"

   [hotkey]
   mode = "toggle"           # required when streaming (default)

   [paxa]
   language = "th"
   ```
4. Restart the daemon: `systemctl --user restart voxtype`.

## Modes

### Realtime streaming (default)

`streaming = true` opens a WebSocket to `wss://<endpoint>/v1/stt/live` for each dictation and sends 16 kHz mono PCM as you speak.

- **Turns:** Paxa splits speech into turns at pauses and sends one final transcript per turn. Voxtype types each turn as it arrives, joined to the previous turn with a space.
- **Partials:** the protocol defines interim transcripts, and voxtype types stable extensions of them when `type_partials = true`. The current `paxa-stt-lite-realtime-v1-preview` model sends finals only, so text appears a sentence at a time after each pause.
- **Activation:** push-to-talk is auto-promoted to toggle, as with every streaming engine. To keep push-to-talk, set `[output] streaming_type_on_release = true`.
- **Stop:** voxtype sends `{"type":"end"}`, which closes the open turn, and waits up to 5 s for the final transcript.

### Recording request

`streaming = false` sends the whole recording to `/v1/audio/transcriptions` when you release the key. This path is also used for `voxtype transcribe <file>`. It works with push-to-talk.

## Configuration

```toml
[paxa]
# api_key = "pxa_..."          # or PAXA_API_KEY (preferred)
# endpoint = "api.paxalabs.com"
# model = "paxa-stt-lite-realtime-v1-preview"   # streaming
# batch_model = "paxa-stt-lite-v1-preview"      # streaming = false, files
language = "th"                # hint, not a filter; unset = auto-detect
# streaming = true
# type_partials = true
# style = "clean"              # drop fillers (streaming only)
# convention = "written"       # digits and units in Thai (streaming only)
# vocabulary = ["Paxa Labs", "voxtype"]
```

See [CONFIGURATION.md](CONFIGURATION.md#paxa) for every field.

## Troubleshooting

- **"Paxa API key required"**: set `PAXA_API_KEY` in the daemon's environment (for systemd, an `EnvironmentFile=` in a drop-in) or `[paxa] api_key`.
- **Session closes with 4402 or 4403**: the account is out of credits or the key's spending cap was reached.
- **Session closes with 4429**: too many concurrent live connections for your plan.
- **Text arrives only after pauses**: expected with the current realtime model, which sends finals only. Use `streaming = false` if you prefer one block of text after release.
