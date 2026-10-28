//! DeepSeek HTTP 调用、用量审计与输入预算；不启动 AI 子进程。
use super::{Pipeline, ai_prompt::PromptPolicy};
use crate::subtitle::Cue;
use crate::{config::BatchMode, db::Database, model::AiUsage};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug)]
pub(super) struct AiResult {
    pub(super) value: Value,
    pub(super) output: AiCallMetrics,
}

#[derive(Debug)]
pub(super) struct AiCallMetrics {
    pub(super) duration_ms: i64,
    // HTTP 没有可独立测量的子进程 RSS；0 表示不适用，不是进程实际内存。
    pub(super) peak_rss_kib: u64,
}

pub(super) const AI_PROMPT_OVERHEAD_TOKENS: usize = 2_048;
pub(super) const AI_METADATA_OUTPUT_RESERVE_TOKENS: usize = 1_024;
// 保留既有请求预算与批次边界，不因替换传输层而改变分句/翻译逻辑。
pub(super) const AI_MAX_PAYLOAD_BYTES: usize = 96 * 1024;
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
#[error("AI 全局故障（HTTP {status}）: {message}")]
pub(super) struct AiGlobalFault {
    status: u16,
    message: String,
}

pub(super) fn is_ai_global_fault(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AiGlobalFault>().is_some()
}

pub(super) fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent("y2b-rs/0.1")
        .connect_timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(4)
        .build()
        .expect("固定 AI HTTP 客户端配置必须有效")
}

pub(crate) fn read_api_key(path: &std::path::Path) -> Result<String> {
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;
    let mut file = std::fs::File::open(path).context("无法读取 DeepSeek 专用认证文件")?;
    let meta = file.metadata().context("无法检查 DeepSeek 认证文件")?;
    anyhow::ensure!(
        meta.is_file() && meta.permissions().mode() & 0o077 == 0,
        "DeepSeek 认证文件必须是仅当前用户可读的普通文件（0600）"
    );
    let mut bytes = Vec::new();
    (&mut file).take(16 * 1024 + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 16 * 1024, "DeepSeek 认证文件过大");
    // 不把 serde 错误链（可能带源文本）或凭据写进日志。
    let doc: Value = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("DeepSeek 认证文件 JSON 无效"))?;
    anyhow::ensure!(
        doc.as_object().is_some_and(|o| o.len() == 1) && doc["deepseek"]["type"] == "api_key",
        "DeepSeek 认证文件必须只包含 deepseek api_key"
    );
    let key = doc["deepseek"]["key"]
        .as_str()
        .context("DeepSeek 认证文件缺少 key")?;
    anyhow::ensure!(
        !key.is_empty()
            && key.is_ascii()
            && !key.chars().any(|c| c.is_whitespace() || c.is_control()),
        "DeepSeek API key 格式无效"
    );
    Ok(key.to_owned())
}

fn parse_usage(v: &Value) -> AiUsage {
    let prompt = v["prompt_tokens"].as_i64().unwrap_or(0).max(0);
    let cache_read = v["prompt_cache_hit_tokens"]
        .as_i64()
        .or_else(|| v["prompt_tokens_details"]["cached_tokens"].as_i64())
        .unwrap_or(0)
        .clamp(0, prompt);
    let output = v["completion_tokens"].as_i64().unwrap_or(0).max(0);
    AiUsage {
        input: prompt - cache_read,
        output,
        reasoning: v["completion_tokens_details"]["reasoning_tokens"]
            .as_i64()
            .unwrap_or(0)
            .max(0),
        cache_read,
        cache_write: 0,
        total: v["total_tokens"]
            .as_i64()
            .unwrap_or(prompt.saturating_add(output))
            .max(0),
        // 官方 usage 不返回费用，不推算或伪造账单。
        cost: None,
    }
}

async fn read_body(mut response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
    {
        bail!("DeepSeek 响应超过 {MAX_RESPONSE_BYTES} 字节上限");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.without_url())? {
        anyhow::ensure!(
            chunk.len() <= MAX_RESPONSE_BYTES.saturating_sub(body.len()),
            "DeepSeek 响应超过 {MAX_RESPONSE_BYTES} 字节上限"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(super) fn provider_error(status: u16, body: &Value, key: &str) -> anyhow::Error {
    let message = body["error"]["message"]
        .as_str()
        .unwrap_or("DeepSeek 请求失败");
    let message = message
        .replace(key, "[redacted]")
        .chars()
        .take(512)
        .collect::<String>();
    match status {
        401 | 402 => AiGlobalFault { status, message }.into(),
        _ => anyhow::anyhow!("DeepSeek HTTP {status}: {message}"),
    }
}

fn parse_completion(body: &Value) -> Result<Value> {
    let choice = body["choices"]
        .get(0)
        .context("DeepSeek 响应缺少 choices")?;
    anyhow::ensure!(
        choice["finish_reason"] == "stop",
        "DeepSeek 响应未完整结束: finish_reason={}",
        choice["finish_reason"]
    );
    let text = choice["message"]["content"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .context("DeepSeek 响应缺少最终文本")?;
    let clean = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    serde_json::from_str(clean).context("DeepSeek 最终文本不是 JSON")
}

/// 每次实际请求 DeepSeek 前先落一条 `started`，所有正常返回和错误路径再原位收尾。
/// future 被并发失败取消时 Drop 会留下 `interrupted`，保证调用次数不静默丢失。
struct AiCallGuard {
    db: Database,
    id: i64,
    finished: bool,
}

impl AiCallGuard {
    #[allow(clippy::too_many_arguments)]
    fn begin(
        db: &Database,
        job_id: &str,
        stage_id: i64,
        task: &str,
        provider: &str,
        model: &str,
        thinking: &str,
        input_json: &str,
    ) -> Result<Self> {
        Ok(Self {
            db: db.clone(),
            id: db.begin_ai_call(
                job_id, stage_id, task, provider, model, thinking, input_json,
            )?,
            finished: false,
        })
    }

    fn finish(
        &mut self,
        status: &str,
        usage: &AiUsage,
        duration_ms: i64,
        output_json: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        self.db
            .finish_ai_call(self.id, status, usage, duration_ms, output_json, error)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for AiCallGuard {
    fn drop(&mut self) {
        if !self.finished
            && let Err(error) = self
                .db
                .interrupt_ai_call(self.id, "AI 调用未正常收尾（future 被取消或审计写回失败）")
        {
            tracing::error!(ai_call_id = self.id, error = %error, "AI 调用中断状态写入失败");
        }
    }
}

pub(super) fn parse_ranges(v: &Value) -> Result<Vec<(usize, usize)>> {
    v.get("ranges")
        .and_then(Value::as_array)
        .context("分句结果缺少 ranges")?
        .iter()
        .map(|x| {
            Ok((
                x["start"].as_u64().context("range.start")? as usize,
                x["end"].as_u64().context("range.end")? as usize,
            ))
        })
        .collect()
}

pub(super) fn parse_translations(v: &Value) -> Result<Vec<(usize, String)>> {
    v.get("translations")
        .and_then(Value::as_array)
        .context("翻译结果缺少 translations")?
        .iter()
        .map(|x| {
            Ok((
                x["i"].as_u64().context("translation.i")? as usize,
                x["text"].as_str().context("translation.text")?.to_string(),
            ))
        })
        .collect()
}

pub(super) fn estimate_text_tokens(text: &str) -> usize {
    let mut tokens: usize = 0;
    let mut ascii_run: usize = 0;
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '\'') {
            ascii_run += 1;
            continue;
        }
        if ascii_run > 0 {
            tokens += ascii_run.div_ceil(4);
            ascii_run = 0;
        }
        if !ch.is_whitespace() {
            tokens += 1;
        }
    }
    if ascii_run > 0 {
        tokens += ascii_run.div_ceil(4);
    }
    tokens.max(1)
}

pub(super) fn segment_cue_tokens(cue: &Cue) -> usize {
    estimate_text_tokens(&cue.source) + 22
}

pub(super) fn translation_cue_tokens(cue: &Cue) -> usize {
    let source = estimate_text_tokens(&cue.source);
    source.saturating_mul(2) + 20
}

pub(super) fn estimate_segment_tokens(cues: &[Cue]) -> usize {
    AI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(segment_cue_tokens).sum::<usize>()
}

pub(super) fn segment_cue_argument_bytes(index: usize, cue: &Cue) -> usize {
    serde_json::to_string(&json!({
        "i": index,
        "start": cue.start,
        "end": cue.end,
        "text": cue.source
    }))
    .map_or(usize::MAX, |value| value.len().saturating_add(1))
}

pub(super) fn estimate_segment_argument_bytes(cues: &[Cue]) -> usize {
    512usize.saturating_add(
        cues.iter()
            .enumerate()
            .map(|(index, cue)| segment_cue_argument_bytes(index, cue))
            .sum::<usize>(),
    )
}

pub(super) fn estimate_translation_tokens(cues: &[Cue]) -> usize {
    AI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(translation_cue_tokens).sum::<usize>()
}

pub(super) fn batch_mode_name(mode: BatchMode) -> &'static str {
    match mode {
        BatchMode::WholeVideo => "whole_video",
        BatchMode::Adaptive => "adaptive",
    }
}

impl Pipeline {
    pub(super) async fn call_ai(
        &self,
        job_id: &str,
        stage_id: i64,
        payload: Value,
    ) -> Result<AiResult> {
        let task = payload
            .get("task")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let model = if task == "translate" {
            &self.config.ai.translation_model
        } else {
            &self.config.ai.model
        };
        let input_json = payload.to_string();
        let policy = self
            .ai_policy
            .get_or_try_init(|| async { PromptPolicy::load(&self.config.ai.policy) })
            .await?;
        let system = policy.system_prompt(&input_json);
        // 每次请求读取同一专用文件，换钥立即生效；不继承 Pi 或环境变量的凭据优先级。
        let key = read_api_key(&self.config.ai.auth_file)?;
        let request = json!({
            "model": model,
            "messages": [{"role":"system","content":system}, {"role":"user","content":input_json}],
            "thinking": {"type":"disabled"},
            "response_format": {"type":"json_object"},
            "max_tokens": self.config.ai.max_output_tokens,
            "stream": false
        });
        let mut audit = AiCallGuard::begin(
            &self.db,
            job_id,
            stage_id,
            task,
            &self.config.ai.provider,
            model,
            &self.config.ai.thinking,
            &input_json,
        )?;
        let started = Instant::now();
        let mut usage = empty_usage();
        let mut raw_text = None;
        let mut status = "request_error";
        let result: Result<Value> = async {
            let response = self
                .ai_http
                .post(format!(
                    "{}/chat/completions",
                    self.config.ai.base_url.trim_end_matches('/')
                ))
                .bearer_auth(&key)
                .timeout(Duration::from_secs(self.config.ai.timeout_seconds))
                .json(&request)
                .send()
                .await
                .map_err(|e| e.without_url())?;
            let code = response.status().as_u16();
            let bytes = read_body(response).await?;
            let body: Value = match serde_json::from_slice(&bytes) {
                Ok(body) => body,
                Err(_) if !(200..300).contains(&code) => {
                    status = "provider_error";
                    return Err(provider_error(code, &Value::Null, &key));
                }
                Err(_) => {
                    status = "parse_error";
                    bail!("DeepSeek 响应不是 JSON");
                }
            };
            usage = parse_usage(&body["usage"]);
            if !(200..300).contains(&code) || body.get("error").is_some_and(|e| !e.is_null()) {
                status = "provider_error";
                return Err(provider_error(code, &body, &key));
            }
            status = "parse_error";
            raw_text = body["choices"][0]["message"]["content"]
                .as_str()
                .map(str::to_owned);
            parse_completion(&body)
        }
        .await;
        let elapsed = started.elapsed().as_millis().min(i64::MAX as u128) as i64;
        match result {
            Ok(value) => {
                audit.finish("success", &usage, elapsed, Some(&value.to_string()), None)?;
                Ok(AiResult {
                    value,
                    output: AiCallMetrics {
                        duration_ms: elapsed,
                        peak_rss_kib: 0,
                    },
                })
            }
            Err(error) => {
                audit.finish(
                    status,
                    &usage,
                    elapsed,
                    raw_text.as_deref(),
                    Some(&error.to_string()),
                )?;
                Err(error)
            }
        }
    }

    pub(super) fn ai_token_budget(&self) -> Result<usize> {
        let context = self.config.ai.context_window_tokens;
        let safe = self.config.ai.safe_context_tokens;
        if context == 0 || safe == 0 || safe > context {
            bail!("AI token 配置无效: safe_context_tokens={safe}, context_window_tokens={context}")
        }
        Ok(safe)
    }
}

fn empty_usage() -> AiUsage {
    parse_usage(&Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::testing::{completion, mock_ai};
    use crate::{config::Config, db::NewJob, model::TransferMode};
    use axum::http::StatusCode;

    #[tokio::test]
    async fn http_calls_keep_usage_on_parse_errors_and_stop_global_faults() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        mock_ai(&mut config, directory.path(), |request| {
            let payload: Value =
                serde_json::from_str(request["messages"][1]["content"].as_str().unwrap()).unwrap();
            match payload["task"].as_str().unwrap() {
                "segment" => {
                    let mut body = completion(json!({"ranges":[{"start":0,"end":1}]}));
                    body["error"] = Value::Null;
                    (StatusCode::OK, body)
                }
                "truncated" => {
                    let mut body = completion(json!({}));
                    body["choices"][0]["finish_reason"] = json!("length");
                    (StatusCode::OK, body)
                }
                "invalid" => {
                    let mut body = completion(json!({}));
                    body["choices"][0]["message"]["content"] = json!("not JSON");
                    (StatusCode::OK, body)
                }
                "unauthorized" => (
                    StatusCode::UNAUTHORIZED,
                    json!({"error":{"message":"invalid fixture-key"}}),
                ),
                _ => (
                    StatusCode::PAYMENT_REQUIRED,
                    json!({"error":{"message":"Insufficient Balance"}}),
                ),
            }
        })
        .await;
        let db = Database::open(&directory.path().join("state.db")).unwrap();
        let job = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "api",
                url: "https://youtu.be/api",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        let stage = db
            .start_stage(
                &job,
                "segmentation",
                Some("deepseek"),
                Some("deepseek-flash"),
                Some("off"),
            )
            .unwrap();
        let pipeline = Pipeline::new(config, db.clone());
        let result = pipeline
            .call_ai(&job, stage, json!({"task":"segment"}))
            .await
            .unwrap();
        assert_eq!(parse_ranges(&result.value).unwrap(), [(0, 1)]);
        for task in ["truncated", "invalid", "unauthorized", "balance"] {
            let error = pipeline
                .call_ai(&job, stage, json!({"task":task}))
                .await
                .unwrap_err();
            assert_eq!(
                is_ai_global_fault(&error),
                matches!(task, "unauthorized" | "balance")
            );
            assert!(!error.to_string().contains("fixture-key"));
        }
        let totals = db.ai_totals_for_job(&job).unwrap();
        assert_eq!(
            (totals.input, totals.cache_read, totals.output, totals.total),
            (24, 9, 21, 54)
        );
        assert_eq!(totals.cost, None);
        let connection = rusqlite::Connection::open(directory.path().join("state.db")).unwrap();
        let states: Vec<String> = connection
            .prepare("SELECT status FROM ai_calls ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            states,
            [
                "success",
                "parse_error",
                "parse_error",
                "provider_error",
                "provider_error"
            ]
        );
    }

    #[tokio::test]
    async fn cancelled_request_leaves_interrupted_audit() {
        use axum::{Router, routing::post};
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        mock_ai(&mut config, directory.path(), |_| unreachable!()).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.ai.base_url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route(
            "/chat/completions",
            post(|| async { std::future::pending::<String>().await }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let db = Database::open(&directory.path().join("state.db")).unwrap();
        let job = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "cancel",
                url: "https://youtu.be/cancel",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        let stage = db
            .start_stage(&job, "translation", None, None, None)
            .unwrap();
        let pipeline = Pipeline::new(config, db);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                pipeline.call_ai(&job, stage, json!({"task":"translate"}))
            )
            .await
            .is_err()
        );
        let connection = rusqlite::Connection::open(directory.path().join("state.db")).unwrap();
        let state: String = connection
            .query_row("SELECT status FROM ai_calls", [], |r| r.get(0))
            .unwrap();
        assert_eq!(state, "interrupted");
    }
}
