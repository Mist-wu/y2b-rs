//! Pi 调用与 token 预算：子进程调用、事件流解析和输入规模估算。
use super::Pipeline;
use crate::config::BatchMode;
use crate::db::Database;
use crate::model::AiUsage;
use crate::process::{ProcessOutput, process_error_output, run_monitored};
use crate::subtitle::Cue;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;

#[derive(Debug)]
pub(super) struct PiResult {
    pub(super) value: Value,
    pub(super) output: ProcessOutput,
}

struct PiStreamOutcome {
    value: Result<Value>,
    usage: AiUsage,
    raw_text: Option<String>,
}

/// 每次实际启动 Pi 前先落一条 `started`，所有正常返回和错误路径再原位收尾。
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
                .interrupt_ai_call(self.id, "Pi 调用未正常收尾（future 被取消或审计写回失败）")
        {
            tracing::error!(ai_call_id = self.id, error = %error, "AI 调用中断状态写入失败");
        }
    }
}

pub(super) const PI_PROMPT_OVERHEAD_TOKENS: usize = 2_048;

pub(super) const PI_METADATA_OUTPUT_RESERVE_TOKENS: usize = 1_024;

/// 分句窗口的单次参数大小上限：2GB 内存的服务器上 pi 处理超过 ~96KB
/// 的输入会因内存/swap 拖慢甚至 OOM（实测 128KB 窗口触发 kill），
/// 96KB（≈30k tokens）窗口单次约 160s，可稳定完成。
pub(super) const PI_MAX_PROMPT_ARGUMENT_BYTES: usize = 96 * 1024;

#[derive(Debug, Error)]
#[error("AI 全局故障（HTTP {status}）: {message}")]
pub(super) struct AiGlobalFault {
    status: u16,
    message: String,
}

pub(super) fn is_ai_global_fault(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AiGlobalFault>().is_some()
}

fn error_status(value: &Value) -> Option<u16> {
    ["status", "statusCode", "httpStatus"]
        .into_iter()
        .find_map(|field| {
            value[field]
                .as_u64()
                .and_then(|status| u16::try_from(status).ok())
                .or_else(|| value[field].as_str()?.parse().ok())
        })
        .or_else(|| {
            value.get("error").and_then(|error| {
                ["status", "statusCode", "httpStatus"]
                    .into_iter()
                    .find_map(|field| {
                        error[field]
                            .as_u64()
                            .and_then(|status| u16::try_from(status).ok())
                            .or_else(|| error[field].as_str()?.parse().ok())
                    })
            })
        })
}

fn explicit_error_message(value: &Value) -> Option<&str> {
    value["errorMessage"]
        .as_str()
        .or_else(|| value["error_message"].as_str())
        .or_else(|| value["error"]["message"].as_str())
}

fn classify_global_fault(message: &str, status: Option<u16>) -> Option<AiGlobalFault> {
    let lower = message.to_ascii_lowercase();
    let unauthorized = status == Some(401)
        || lower.contains("http 401")
        || lower.contains("status 401")
        || lower.contains("unauthorized")
        || lower.contains("invalid api key")
        || lower.contains("authentication failed");
    let insufficient_balance = status == Some(402)
        || lower.contains("http 402")
        || lower.contains("status 402")
        || lower.contains("insufficient balance")
        || lower.contains("insufficient_balance")
        || lower.contains("余额不足");
    let status = if unauthorized {
        401
    } else if insufficient_balance {
        402
    } else {
        return None;
    };
    Some(AiGlobalFault {
        status,
        message: message.trim().to_string(),
    })
}

fn pi_event_error(value: &Value) -> Option<anyhow::Error> {
    let stop_reason = value["stopReason"]
        .as_str()
        .or_else(|| value["stop_reason"].as_str());
    let explicit_message = explicit_error_message(value);
    let status = error_status(value);
    let stopped_with_error = stop_reason.is_some_and(|reason| reason.eq_ignore_ascii_case("error"));
    let failed_status = status.is_some_and(|status| (400..=599).contains(&status));
    if !stopped_with_error && explicit_message.is_none() && !failed_status {
        return None;
    }
    let message = explicit_message
        .or_else(|| value["message"].as_str())
        .unwrap_or("Pi 返回未说明原因的错误");
    Some(match classify_global_fault(message, status) {
        Some(error) => error.into(),
        None => anyhow::anyhow!("Pi 返回错误: {message}"),
    })
}

fn classify_process_error(error: anyhow::Error) -> anyhow::Error {
    classify_global_fault(&error.to_string(), None)
        .map(Into::into)
        .unwrap_or(error)
}

fn recover_usage_from_process_error(error: &anyhow::Error) -> (AiUsage, i64, Option<String>) {
    process_error_output(error)
        .map(|output| {
            let parsed = inspect_pi_stream(&output.stdout);
            (parsed.usage, output.duration_ms, parsed.raw_text)
        })
        .unwrap_or_else(|| (empty_usage(), 0, None))
}

fn inspect_pi_stream(stream: &str) -> PiStreamOutcome {
    let mut text = None;
    let mut usage = AiUsage {
        input: 0,
        output: 0,
        reasoning: 0,
        cache_read: 0,
        cache_write: 0,
        total: 0,
        cost: None,
    };
    let mut terminal_error = None;
    for line in stream.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if v["type"] == "agent_end" {
            let mut current_error = pi_event_error(&v);
            if let Some(messages) = v["messages"].as_array()
                && let Some(m) = messages.iter().rev().find(|m| m["role"] == "assistant")
            {
                if current_error.is_none() {
                    current_error = pi_event_error(m);
                }
                text = m["content"]
                    .as_array()
                    .and_then(|a| {
                        a.iter()
                            .filter_map(|x| x.get("text").and_then(Value::as_str))
                            .next_back()
                    })
                    .map(str::to_string);
                usage = parse_usage(&m["usage"]);
            }
            terminal_error = current_error;
        }
    }
    let value = match terminal_error {
        Some(error) => Err(error),
        None => text
            .as_deref()
            .context("Pi JSON 流中没有最终文本")
            .and_then(|raw| {
                let clean = raw
                    .trim()
                    .trim_start_matches("```json")
                    .trim_start_matches("```")
                    .trim_end_matches("```")
                    .trim();
                serde_json::from_str(clean).context("Pi 最终文本不是 JSON")
            }),
    };
    PiStreamOutcome {
        value,
        usage,
        raw_text: text,
    }
}

#[cfg(test)]
pub(super) fn parse_pi_stream(stream: &str) -> Result<(Value, AiUsage)> {
    let PiStreamOutcome { value, usage, .. } = inspect_pi_stream(stream);
    Ok((value?, usage))
}

pub(super) fn parse_usage(v: &Value) -> AiUsage {
    AiUsage {
        input: v["input"].as_i64().unwrap_or(0),
        output: v["output"].as_i64().unwrap_or(0),
        reasoning: v["reasoning"].as_i64().unwrap_or(0),
        cache_read: v["cacheRead"].as_i64().unwrap_or(0),
        cache_write: v["cacheWrite"].as_i64().unwrap_or(0),
        total: v["totalTokens"].as_i64().unwrap_or(0),
        cost: v["cost"]["total"].as_f64(),
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
    PI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(segment_cue_tokens).sum::<usize>()
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
    PI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(translation_cue_tokens).sum::<usize>()
}

pub(super) fn batch_mode_name(mode: BatchMode) -> &'static str {
    match mode {
        BatchMode::WholeVideo => "whole_video",
        BatchMode::Adaptive => "adaptive",
    }
}

impl Pipeline {
    pub(super) async fn call_pi(
        &self,
        job_id: &str,
        stage_id: i64,
        payload: Value,
    ) -> Result<PiResult> {
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
        let mut cmd = Command::new(&self.config.ai.pi);
        cmd.args([
            "--mode",
            "json",
            "--print",
            "--no-session",
            "--no-tools",
            "--no-skills",
            "--no-context-files",
            "--no-prompt-templates",
            "--no-extensions",
            "--extension",
        ]);
        cmd.arg(&self.config.ai.extension);
        cmd.args([
            "--provider",
            &self.config.ai.provider,
            "--model",
            model,
            "--thinking",
            &self.config.ai.thinking,
            "--no-approve",
        ]);
        cmd.env("Y2B_PI_POLICY_PATH", &self.config.ai.policy);
        cmd.arg(input_json);
        let out =
            match run_monitored(cmd, Duration::from_secs(self.config.ai.timeout_seconds)).await {
                Ok(output) => output,
                Err(process_error) => {
                    let (usage, duration_ms, raw_text) =
                        recover_usage_from_process_error(&process_error);
                    let error = classify_process_error(process_error);
                    let message = error.to_string();
                    audit.finish(
                        "process_error",
                        &usage,
                        duration_ms,
                        raw_text.as_deref(),
                        Some(&message),
                    )?;
                    return Err(error);
                }
            };
        let PiStreamOutcome {
            value,
            usage,
            raw_text,
        } = inspect_pi_stream(&out.stdout);
        match value {
            Ok(value) => {
                let output_json = value.to_string();
                audit.finish("success", &usage, out.duration_ms, Some(&output_json), None)?;
                Ok(PiResult { value, output: out })
            }
            Err(error) => {
                let status = if is_ai_global_fault(&error)
                    || error.to_string().starts_with("Pi 返回错误:")
                {
                    "provider_error"
                } else {
                    "parse_error"
                };
                let message = error.to_string();
                audit.finish(
                    status,
                    &usage,
                    out.duration_ms,
                    raw_text.as_deref(),
                    Some(&message),
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
    AiUsage {
        input: 0,
        output: 0,
        reasoning: 0,
        cache_read: 0,
        cache_write: 0,
        total: 0,
        cost: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::testing::cue;

    #[test]
    fn pi_json() {
        let s = r#"{"type":"agent_end","status":200,"messages":[{"role":"assistant","content":[{"type":"text","text":"{\"ranges\":[{\"start\":0,\"end\":1}]}"}],"usage":{"input":2,"output":3,"totalTokens":5}}]}"#;
        let (v, u) = parse_pi_stream(s).unwrap();
        assert_eq!(parse_ranges(&v).unwrap(), vec![(0, 1)]);
        assert_eq!(u.total, 5);
    }

    #[test]
    fn invalid_final_json_retains_usage_for_audit() {
        let stream = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[{"type":"text","text":"not json"}],"usage":{"input":11,"output":7,"cacheRead":3,"totalTokens":21,"cost":{"total":0.0123}}}]}"#;
        let parsed = inspect_pi_stream(stream);
        assert_eq!(parsed.raw_text.as_deref(), Some("not json"));
        assert_eq!(parsed.usage.input, 11);
        assert_eq!(parsed.usage.output, 7);
        assert_eq!(parsed.usage.cache_read, 3);
        assert_eq!(parsed.usage.total, 21);
        assert_eq!(parsed.usage.cost, Some(0.0123));
        assert_eq!(
            parsed.value.unwrap_err().to_string(),
            "Pi 最终文本不是 JSON"
        );
    }

    #[test]
    fn provider_error_retains_usage_for_audit() {
        let stream = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[],"stopReason":"error","errorMessage":"provider temporarily unavailable","usage":{"input":13,"output":2,"totalTokens":15,"cost":{"total":0.004}}}]}"#;
        let parsed = inspect_pi_stream(stream);
        assert_eq!(parsed.usage.total, 15);
        assert_eq!(parsed.usage.cost, Some(0.004));
        assert_eq!(
            parsed.value.unwrap_err().to_string(),
            "Pi 返回错误: provider temporarily unavailable"
        );
    }

    #[test]
    fn pi_json_surfaces_insufficient_balance_as_global_fault() {
        let stream = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[],"stopReason":"error","errorMessage":"Insufficient Balance"}]}"#;
        let error = parse_pi_stream(stream).unwrap_err();
        assert!(is_ai_global_fault(&error));
        assert!(error.to_string().contains("HTTP 402"));
        assert!(error.to_string().contains("Insufficient Balance"));
    }

    #[test]
    fn pi_json_surfaces_unauthorized_as_global_fault() {
        let stream = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[],"stopReason":"error","errorMessage":"Unauthorized","statusCode":401}]}"#;
        let error = parse_pi_stream(stream).unwrap_err();
        assert!(is_ai_global_fault(&error));
        assert!(error.to_string().contains("HTTP 401"));
        assert!(is_ai_global_fault(&error.context("wrapped by pipeline")));
    }

    #[test]
    fn pi_json_surfaces_event_level_errors_without_messages() {
        let stream = r#"{"type":"agent_end","stopReason":"error","errorMessage":"Insufficient Balance","status":402}"#;
        let error = parse_pi_stream(stream).unwrap_err();
        assert!(is_ai_global_fault(&error));
        assert!(error.to_string().contains("HTTP 402"));
    }

    #[test]
    fn nonzero_pi_process_errors_are_classified_too() {
        let error = classify_process_error(anyhow::anyhow!(
            "子进程退出码 Some(1): HTTP 401 Unauthorized"
        ));
        assert!(is_ai_global_fault(&error));
        assert!(error.to_string().contains("HTTP 401"));
    }

    #[test]
    fn pi_json_surfaces_non_global_errors_instead_of_missing_text() {
        let stream = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[],"stopReason":"error","errorMessage":"provider temporarily unavailable"}]}"#;
        let error = parse_pi_stream(stream).unwrap_err();
        assert!(!is_ai_global_fault(&error));
        assert_eq!(
            error.to_string(),
            "Pi 返回错误: provider temporarily unavailable"
        );
    }

    #[test]
    fn thirty_minute_subtitles_fit_default_safe_budget() {
        let cues = (0..1_800)
            .map(|i| {
                cue(
                    i,
                    "this is a representative subtitle sentence with useful context",
                )
            })
            .collect::<Vec<_>>();
        assert!(estimate_segment_tokens(&cues) < 200_000);
        assert!(estimate_translation_tokens(&cues) < 200_000);
    }

    #[tokio::test]
    async fn timeout_process_error_recovers_usage_from_captured_output() {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf '{\"type\":\"agent_end\",\"messages\":[{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"hello\"}],\"usage\":{\"input\":5,\"output\":3,\"totalTokens\":8,\"cost\":{\"total\":0.02}}}]}'; sleep 30",
        ]);
        let error = run_monitored(command, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(
            error
                .downcast_ref::<crate::process::ProcessTimeoutFailure>()
                .is_some()
        );
        let (usage, duration_ms, raw_text) = recover_usage_from_process_error(&error);
        assert_eq!(usage.total, 8);
        assert_eq!(usage.cost, Some(0.02));
        assert!(duration_ms > 0);
        assert_eq!(raw_text.as_deref(), Some("hello"));
    }
}
