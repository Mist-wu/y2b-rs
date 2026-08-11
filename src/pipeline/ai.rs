//! Pi 调用与 token 预算：子进程调用、事件流解析和输入规模估算。
use super::Pipeline;
use crate::config::BatchMode;
use crate::model::AiUsage;
use crate::process::{ProcessOutput, run_monitored};
use crate::subtitle::Cue;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;

#[derive(Debug)]
pub(super) struct PiResult {
    pub(super) value: Value,
    pub(super) usage: AiUsage,
    pub(super) output: ProcessOutput,
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

pub(super) fn parse_pi_stream(stream: &str) -> Result<(Value, AiUsage)> {
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
    for line in stream.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if v["type"] == "agent_end" {
            if let Some(error) = pi_event_error(&v) {
                return Err(error);
            }
            if let Some(messages) = v["messages"].as_array()
                && let Some(m) = messages.iter().rev().find(|m| m["role"] == "assistant")
            {
                if let Some(error) = pi_event_error(m) {
                    return Err(error);
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
        }
    }
    let raw = text.context("Pi JSON 流中没有最终文本")?;
    let clean = raw
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    Ok((
        serde_json::from_str(clean).context("Pi 最终文本不是 JSON")?,
        usage,
    ))
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
    pub(super) async fn call_pi(&self, payload: Value) -> Result<PiResult> {
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
            &self.config.ai.model,
            "--thinking",
            &self.config.ai.thinking,
            "--no-approve",
        ]);
        cmd.env("Y2B_PI_POLICY_PATH", &self.config.ai.policy);
        cmd.arg(payload.to_string());
        let out = run_monitored(cmd, Duration::from_secs(self.config.ai.timeout_seconds))
            .await
            .map_err(classify_process_error)?;
        let (value, usage) = parse_pi_stream(&out.stdout)?;
        Ok(PiResult {
            value,
            usage,
            output: out,
        })
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
}
