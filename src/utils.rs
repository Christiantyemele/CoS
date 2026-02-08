use anyhow::Result;
use async_openai::types::{ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs, ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs};
use async_openai::Client;
use reqwest::header;
use rodio::{Decoder, OutputStream, Sink};
use std::env;
use std::io::Cursor;

pub async fn openai_chat(system: &str, user: &str) -> Result<String> {
    let model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
    let client = Client::new();

    let system_msg: ChatCompletionRequestMessage = ChatCompletionRequestSystemMessageArgs::default()
        .content(system)
        .build()?
        .into();
    let user_msg: ChatCompletionRequestMessage = ChatCompletionRequestUserMessageArgs::default()
        .content(user)
        .build()?
        .into();

    let req = CreateChatCompletionRequestArgs::default()
        .model(model)
        .messages(vec![system_msg, user_msg])
        .build()?;

    let resp = client.chat().create(req).await?;
    let content = resp
        .choices
        .first()
        .and_then(|c| c.message.content.clone())
        .unwrap_or_default();
    Ok(content)
}

pub async fn elevenlabs_stt_from_file(path: &str) -> Result<String> {
    let api_key = env::var("ELEVEN_API_KEY")?;
    let client = reqwest::Client::new();
    let url = "https://api.elevenlabs.io/v1/speech-to-text";

    let data = tokio::fs::read(path).await?;
    let file_part = reqwest::multipart::Part::bytes(data)
        .file_name("audio")
        .mime_str("application/octet-stream")?;

    let form = reqwest::multipart::Form::new()
        .text("model_id", "scribe_v2")
        .part("file", file_part);

    let resp = client
        .post(url)
        .header("xi-api-key", api_key)
        .multipart(form)
        .send()
        .await?
        .error_for_status()?;

    let json: serde_json::Value = resp.json().await?;
    Ok(json
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string())
}

pub async fn elevenlabs_stt_from_bytes(data: Vec<u8>, mime: Option<&str>) -> Result<String> {
    let api_key = env::var("ELEVEN_API_KEY")?;
    let client = reqwest::Client::new();
    let url = "https://api.elevenlabs.io/v1/speech-to-text";

    let mut file_part = reqwest::multipart::Part::bytes(data).file_name("audio");
    if let Some(m) = mime {
        if !m.trim().is_empty() {
            file_part = file_part.mime_str(m)?;
        }
    }

    let form = reqwest::multipart::Form::new()
        .text("model_id", "scribe_v2")
        .part("file", file_part);

    let resp = client
        .post(url)
        .header("xi-api-key", api_key)
        .multipart(form)
        .send()
        .await?
        .error_for_status()?;

    let json: serde_json::Value = resp.json().await?;
    Ok(json
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string())
}

pub async fn elevenlabs_tts_to_mp3_bytes(text: &str) -> Result<Vec<u8>> {
    let api_key = env::var("ELEVEN_API_KEY")?;
    let voice_id = env::var("ELEVEN_VOICE_ID").unwrap_or_else(|_| "21m00Tcm4TlvDq8ikWAM".to_string());
    let model_id = env::var("ELEVEN_TTS_MODEL").unwrap_or_else(|_| "eleven_multilingual_v2".to_string());

    let url = format!(
        "https://api.elevenlabs.io/v1/text-to-speech/{}",
        voice_id
    );

    let body = serde_json::json!({
        "text": text,
        "model_id": model_id,
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.75
        }
    });

    let client = reqwest::Client::new();
    let bytes = client
        .post(url)
        .header("xi-api-key", api_key)
        .header(header::ACCEPT, "audio/mpeg")
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    Ok(bytes.to_vec())
}

pub fn play_mp3_bytes(mp3: &[u8]) -> Result<()> {
    let (_stream, stream_handle) = OutputStream::try_default()?;
    let sink = Sink::try_new(&stream_handle)?;
    let cursor = Cursor::new(mp3.to_vec());
    let source = Decoder::new(cursor)?;
    sink.append(source);
    sink.sleep_until_end();
    Ok(())
}
