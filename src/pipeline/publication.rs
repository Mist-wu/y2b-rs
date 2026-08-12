//! 投稿元数据：Pi 生成中文标题/动态/标签，以及确定性的简介与 biliup 参数。
use super::Pipeline;
use super::StageGuard;
use super::ai::{
    PI_MAX_PROMPT_ARGUMENT_BYTES, PI_METADATA_OUTPUT_RESERVE_TOKENS, PI_PROMPT_OVERHEAD_TOKENS,
    estimate_text_tokens,
};
use crate::model::{PublicationMetadata, TransferMode, VideoMetadata};
use crate::subtitle::Cue;
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

pub(super) const BILIBILI_TID: i64 = 172;

pub(super) const CORE_TAG: &str = "荒野乱斗";

pub(super) const MAX_TITLE_WIDTH: usize = 70;

pub(super) const MAX_DYNAMIC_WIDTH: usize = 120;

/// 投稿元数据校验失败（如标题宽度超限）时，把原因作为反馈让 AI 重写的最大次数。
pub(super) const PUBLICATION_FEEDBACK_RETRIES: usize = 2;

pub(super) const MAX_TAG_CHARS: usize = 20;

pub(super) const MAX_TAGS: usize = 4;

pub(super) fn build_publication_payload(
    mode: TransferMode,
    meta: &VideoMetadata,
    cues: Option<&[Cue]>,
    budget: usize,
) -> Result<Value> {
    if mode == TransferMode::Translated && cues.is_none_or(<[Cue]>::is_empty) {
        bail!("翻译字幕模式缺少双语字幕，不能生成投稿元数据")
    }
    let all_cues = cues.unwrap_or_default();
    let full_indices = (0..all_cues.len()).collect::<Vec<_>>();
    let full = publication_payload_value(mode, meta, all_cues, &full_indices, false);
    if publication_payload_fits(&full, budget) {
        return Ok(full);
    }
    if mode == TransferMode::Direct {
        bail!(
            "YouTube 元数据超过 Pi 输入限制: estimated_tokens={}, bytes={}",
            estimate_publication_tokens(&full),
            full.to_string().len()
        )
    }

    let mut count = all_cues.len().saturating_sub(1).max(2);
    loop {
        let indices = uniform_sample_indices(all_cues.len(), count);
        let sampled = publication_payload_value(mode, meta, all_cues, &indices, true);
        let estimated = estimate_publication_tokens(&sampled);
        let bytes = sampled.to_string().len();
        if estimated <= budget && bytes <= PI_MAX_PROMPT_ARGUMENT_BYTES {
            return Ok(sampled);
        }
        if count == 2 {
            bail!(
                "保留首尾字幕后投稿元数据仍超过 Pi 输入限制: estimated_tokens={estimated}, bytes={bytes}"
            )
        }
        let token_count = count
            .saturating_mul(budget)
            .checked_div(estimated)
            .unwrap_or(0);
        let byte_count = count
            .saturating_mul(PI_MAX_PROMPT_ARGUMENT_BYTES)
            .checked_div(bytes)
            .unwrap_or(0);
        let shrunk = token_count.min(byte_count);
        count = shrunk.clamp(2, count - 1);
    }
}

pub(super) fn publication_payload_value(
    mode: TransferMode,
    meta: &VideoMetadata,
    cues: &[Cue],
    indices: &[usize],
    sampled: bool,
) -> Value {
    let source_url = meta.webpage_url.as_deref().unwrap_or(&meta.url);
    json!({
        "task": "publish_metadata",
        "transfer_mode": mode.to_string(),
        "youtube": {
            "title": meta.title,
            "description": meta.description.as_deref().unwrap_or(""),
            "url": source_url,
            "uploader": meta.uploader.as_deref().or(meta.channel.as_deref()).unwrap_or(""),
            "published_date": publication_date(meta),
        },
        "subtitle_sampling": {
            "sampled": sampled,
            "total": cues.len(),
            "included": indices.len(),
        },
        "subtitles": indices.iter().map(|&i| {
            let cue = &cues[i];
            json!({
                "i": i,
                "start": cue.start,
                "end": cue.end,
                "source": cue.source,
                "translation": cue.translation.as_deref().unwrap_or(""),
            })
        }).collect::<Vec<_>>(),
    })
}

pub(super) fn estimate_publication_tokens(payload: &Value) -> usize {
    PI_PROMPT_OVERHEAD_TOKENS
        .saturating_add(PI_METADATA_OUTPUT_RESERVE_TOKENS)
        .saturating_add(estimate_text_tokens(&payload.to_string()))
}

pub(super) fn publication_payload_fits(payload: &Value, token_budget: usize) -> bool {
    estimate_publication_tokens(payload) <= token_budget
        && payload.to_string().len() <= PI_MAX_PROMPT_ARGUMENT_BYTES
}

pub(super) fn uniform_sample_indices(len: usize, count: usize) -> Vec<usize> {
    if count >= len {
        return (0..len).collect();
    }
    if len == 0 || count == 0 {
        return Vec::new();
    }
    if count == 1 {
        return vec![0];
    }
    (0..count)
        .map(|slot| slot * (len - 1) / (count - 1))
        .collect()
}

pub(super) fn parse_publication_metadata(value: &Value) -> Result<PublicationMetadata> {
    let title = sanitize_publication_text(
        value
            .get("title")
            .and_then(Value::as_str)
            .context("Pi 投稿元数据缺少字符串 title")?,
    );
    let dynamic = sanitize_publication_text(
        value
            .get("dynamic")
            .and_then(Value::as_str)
            .context("Pi 投稿元数据缺少字符串 dynamic")?,
    );
    let raw_tags = value
        .get("tags")
        .and_then(Value::as_array)
        .context("Pi 投稿元数据缺少数组 tags")?;
    if raw_tags.is_empty() {
        bail!("Pi 投稿元数据 tags 为空")
    }
    let raw_tags = raw_tags
        .iter()
        .map(|tag| {
            tag.as_str()
                .map(str::to_string)
                .context("Pi 投稿元数据 tags 含非字符串项")
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = PublicationMetadata {
        title,
        dynamic,
        tags: sanitize_tags(&raw_tags),
        tid: BILIBILI_TID,
        raw_json: value.to_string(),
    };
    // 注意：这里只负责解析，不做内容校验——校验统一由调用方（publish_metadata
    // 重试循环）执行，否则校验错误会走“解析失败”分支而跳过反馈重试。
    Ok(metadata)
}

/// 对已落库的元数据重新跑一遍解析期的清洗，尽量把它救回可投稿状态。
pub(super) fn repair_publication_metadata(metadata: &PublicationMetadata) -> PublicationMetadata {
    PublicationMetadata {
        title: sanitize_publication_text(&metadata.title),
        dynamic: sanitize_publication_text(&metadata.dynamic),
        tags: sanitize_tags(&metadata.tags),
        tid: BILIBILI_TID,
        raw_json: metadata.raw_json.clone(),
    }
}

pub(super) fn validate_publication_metadata(metadata: &PublicationMetadata) -> Result<()> {
    validate_text_field("标题", &metadata.title, MAX_TITLE_WIDTH)?;
    validate_text_field("动态", &metadata.dynamic, MAX_DYNAMIC_WIDTH)?;
    if metadata.title.contains(['#', '＃'])
        || LINK_NEEDLES
            .iter()
            .any(|needle| metadata.title.to_ascii_lowercase().contains(needle))
    {
        bail!("标题含链接或话题")
    }
    if metadata.tid != BILIBILI_TID {
        bail!("投稿分区必须为 {BILIBILI_TID}，实际为 {}", metadata.tid)
    }
    if metadata.tags.is_empty()
        || metadata.tags.len() > MAX_TAGS
        || metadata.tags.first().map(String::as_str) != Some(CORE_TAG)
    {
        bail!("投稿标签必须以“{CORE_TAG}”开头且总数为 1～{MAX_TAGS}")
    }
    if metadata
        .tags
        .iter()
        .any(|tag| tag.is_empty() || tag.chars().count() > MAX_TAG_CHARS)
    {
        bail!("投稿标签为空或超过 {MAX_TAG_CHARS} 字")
    }
    if metadata.dynamic.contains(['#', '＃'])
        || LINK_NEEDLES
            .iter()
            .any(|needle| metadata.dynamic.to_ascii_lowercase().contains(needle))
        || ["关注我", "点赞", "投币", "三连", "订阅频道", "转发"]
            .iter()
            .any(|needle| metadata.dynamic.contains(needle))
    {
        bail!("动态含链接、话题或引导互动内容")
    }
    // 新解析的元数据已经被 sanitize_publication_text 剥过 emoji，但这里仍要查：
    // publish_metadata 对数据库里已保存的旧元数据也会跑一遍校验。
    if metadata.title.chars().any(is_emoji) || metadata.dynamic.chars().any(is_emoji) {
        bail!("标题或动态含 emoji")
    }
    let sentence_ends = metadata
        .dynamic
        .chars()
        .filter(|ch| matches!(ch, '。' | '！' | '？' | '!' | '?'))
        .count();
    if sentence_ends > 2 {
        bail!("动态超过 2 句")
    }
    Ok(())
}

pub(super) fn validate_text_field(label: &str, text: &str, max_width: usize) -> Result<()> {
    if text.trim().is_empty() || text.chars().any(char::is_control) {
        bail!("{label}为空或含控制字符")
    }
    let width = chinese_width(text);
    if width > max_width {
        bail!("{label}宽度 {width} 超过上限 {max_width}")
    }
    Ok(())
}

pub(super) fn chinese_width(text: &str) -> usize {
    text.chars()
        .map(|ch| if ch.is_ascii() { 1 } else { 2 })
        .sum()
}

pub(super) fn is_emoji(ch: char) -> bool {
    matches!(ch as u32,
        0x1F000..=0x1FAFF | 0x2600..=0x26FF | 0x2700..=0x27BF | 0xFE00..=0xFE0F)
}

pub(super) fn sanitize_publication_text(text: &str) -> String {
    let cleaned = text
        .chars()
        .filter(|ch| !is_emoji(*ch) && !matches!(*ch, '\u{200D}' | '\u{20E3}'))
        .collect::<String>();
    let stripped = join_tokens(&cleaned, strip_link_and_hashtag);
    if !stripped.is_empty() {
        return stripped;
    }
    // 整段都是话题或链接（真实案例：原视频标题就叫 `#sync`，且没有简介）。
    // 此时保词去标记，好过清洗出空标题——空标题一样过不了校验，任务照样进死信。
    join_tokens(&cleaned, drop_link_keep_words)
}

fn join_tokens(text: &str, keep: fn(&str) -> Option<String>) -> String {
    text.split_whitespace()
        .filter_map(keep)
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) const LINK_NEEDLES: [&str; 3] = ["http://", "https://", "www."];

/// 砍掉 token 中从链接或话题标记开始的部分，整段变空时丢弃这个 token。
///
/// YouTube 原标题常带 `#bs #brawlstars` 这类尾巴，AI 忠实翻译时会照抄；校验拒绝
/// 后即使带反馈重试也未必纠正得掉，最终把任务推进死信。这里在解析阶段确定性地
/// 剥掉，校验只作为兜底。
fn strip_link_and_hashtag(token: &str) -> Option<String> {
    let lower = token.to_ascii_lowercase();
    let cut = token
        .char_indices()
        .find(|(_, ch)| matches!(ch, '#' | '＃'))
        .map(|(index, _)| index)
        .into_iter()
        .chain(LINK_NEEDLES.iter().filter_map(|needle| lower.find(needle)))
        .min()
        .unwrap_or(token.len());
    let kept = token[..cut].trim();
    (!kept.is_empty()).then(|| kept.to_string())
}

/// 退让版清洗：只删话题标记本身、整词丢掉链接，词还留着。
fn drop_link_keep_words(token: &str) -> Option<String> {
    let lower = token.to_ascii_lowercase();
    if LINK_NEEDLES.iter().any(|needle| lower.contains(needle)) {
        return None;
    }
    let kept = token
        .chars()
        .filter(|ch| !matches!(ch, '#' | '＃'))
        .collect::<String>();
    let kept = kept.trim();
    (!kept.is_empty()).then(|| kept.to_string())
}

pub(super) fn sanitize_tags(raw: &[String]) -> Vec<String> {
    let mut tags = vec![CORE_TAG.to_string()];
    for tag in raw {
        let clean = tag
            .chars()
            .filter(|ch| !ch.is_control() && !matches!(ch, '#' | '＃' | ',' | '，'))
            .collect::<String>();
        let clean = clean
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(MAX_TAG_CHARS)
            .collect::<String>();
        if clean.is_empty() || tags.iter().any(|existing| existing == &clean) {
            continue;
        }
        tags.push(clean);
        if tags.len() == MAX_TAGS {
            break;
        }
    }
    tags
}

pub(super) fn publication_date(meta: &VideoMetadata) -> String {
    if let Some(date) = meta.upload_date.as_deref()
        && date.len() == 8
        && date.chars().all(|ch| ch.is_ascii_digit())
    {
        return format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..]);
    }
    meta.timestamp
        .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "未知".to_string())
}

pub(super) fn build_description(meta: &VideoMetadata) -> String {
    let source_url = meta.webpage_url.as_deref().unwrap_or(&meta.url);
    let uploader = meta
        .uploader
        .as_deref()
        .or(meta.channel.as_deref())
        .unwrap_or("未知");
    let mut lines = Vec::with_capacity(5);
    if let Some(title) = original_title_without_hashtags(&meta.title) {
        lines.push(format!("原标题：{title}"));
    }
    lines.extend([
        format!("来源：{source_url}"),
        format!("原作者：{uploader}"),
        format!("原视频发布时间：{}", publication_date(meta)),
        "处理工具：https://github.com/Mist-wu/y2b-rs".to_string(),
    ]);
    lines.join("\n")
}

pub(super) fn original_title_without_hashtags(title: &str) -> Option<String> {
    let clean = title
        .split_whitespace()
        .filter(|part| !part.starts_with('#') && !part.starts_with('＃'))
        .collect::<Vec<_>>()
        .join(" ");
    (!clean.is_empty()).then_some(clean)
}

pub(super) fn build_upload_args(
    metadata: &PublicationMetadata,
    meta: &VideoMetadata,
) -> Vec<String> {
    vec![
        "--submit".into(),
        "web".into(),
        "--title".into(),
        metadata.title.clone(),
        "--desc".into(),
        build_description(meta),
        "--tag".into(),
        metadata.tags.join(","),
        "--tid".into(),
        BILIBILI_TID.to_string(),
        "--dynamic".into(),
        metadata.dynamic.clone(),
        "--copyright".into(),
        "1".into(),
        "--no-reprint".into(),
        "0".into(),
        "--limit".into(),
        "1".into(),
    ]
}

impl Pipeline {
    pub(super) async fn publish_metadata(
        &self,
        job_id: &str,
        mode: TransferMode,
        meta: &VideoMetadata,
        cues: Option<&[Cue]>,
    ) -> Result<PublicationMetadata> {
        if let Some(saved) = self.db.publication_metadata(job_id)? {
            if validate_publication_metadata(&saved).is_ok() {
                return Ok(saved);
            }
            // 已落库的元数据可能是在清洗/校验规则收紧之前写入的。能就地清洗就复用，
            // 否则丢弃重新生成——否则每次重试都拿同一份坏数据失败，直到进死信。
            let repaired = repair_publication_metadata(&saved);
            if validate_publication_metadata(&repaired).is_ok() {
                self.db.save_publication_metadata(job_id, &repaired)?;
                return Ok(repaired);
            }
            tracing::warn!(job_id, "已保存的投稿元数据无法清洗通过校验，重新生成");
        }
        let budget = self.ai_token_budget()?;
        let mut stage = StageGuard::start(
            &self.db,
            job_id,
            "publish_metadata",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let mut feedback: Option<String> = None;
        for attempt in 0..=PUBLICATION_FEEDBACK_RETRIES {
            let mut payload = build_publication_payload(mode, meta, cues, budget)?;
            if let Some(message) = &feedback {
                payload["feedback"] = json!(message);
            }
            let input_json = payload.to_string();
            let r = match self.call_pi(payload).await {
                Ok(result) => result,
                Err(error) => {
                    let elapsed = stage.elapsed_ms();
                    return Err(stage.fail(error, elapsed, 0));
                }
            };
            self.db.record_ai_call(
                job_id,
                stage.id(),
                "publish_metadata",
                &self.config.ai.provider,
                &self.config.ai.model,
                &self.config.ai.thinking,
                &r.usage,
                r.output.duration_ms,
                &input_json,
                &r.value.to_string(),
            )?;
            let metadata = match parse_publication_metadata(&r.value) {
                Ok(metadata) => metadata,
                Err(error) => {
                    return Err(stage.fail(error, r.output.duration_ms, r.output.peak_rss_kib));
                }
            };
            if let Err(error) = validate_publication_metadata(&metadata) {
                if attempt < PUBLICATION_FEEDBACK_RETRIES {
                    feedback = Some(format!(
                        "上一轮输出被拒绝：{error}。请按原因修正（通常需要缩短标题/动态）后重新输出完整 JSON。"
                    ));
                    continue;
                }
                return Err(stage.fail(error, r.output.duration_ms, r.output.peak_rss_kib));
            }
            if let Err(error) = self.db.save_publication_metadata(job_id, &metadata) {
                return Err(stage.fail(error, r.output.duration_ms, r.output.peak_rss_kib));
            }
            stage.finish(
                "completed",
                r.output.duration_ms,
                r.output.peak_rss_kib,
                Some(&format!("{} 次尝试后通过校验", attempt + 1)),
            )?;
            return Ok(metadata);
        }
        unreachable!("publish_metadata 重试循环必须收敛")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::testing::{cue, metadata};

    #[test]
    fn publication_metadata_cleans_tags_and_forces_core_tag() {
        let value = json!({
            "title": "2026年最佳排位赛",
            "dynamic": "最后一局上演极限翻盘。",
            "tags": ["#排位赛", "荒野乱斗", "排位赛", "英雄，技巧", "超长标签超长标签超长标签超长标签超长标签"]
        });
        let parsed = parse_publication_metadata(&value).unwrap();
        assert_eq!(parsed.tid, 172);
        assert_eq!(parsed.tags[0], "荒野乱斗");
        assert_eq!(parsed.tags.len(), 4);
        assert!(
            parsed
                .tags
                .iter()
                .all(|tag| !tag.contains(['#', ',', '，']))
        );
        assert!(parsed.tags.iter().all(|tag| tag.chars().count() <= 20));
    }

    #[test]
    fn publication_metadata_rejects_invalid_title_or_dynamic() {
        // 解析只负责结构；内容校验由 validate_publication_metadata 负责，
        // 调用方（publish_metadata 反馈重试循环）对校验失败带反馈重试。
        let parsed = parse_publication_metadata(&json!({
            "title": "",
            "dynamic": "精彩对局。",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert!(validate_publication_metadata(&parsed).is_err());

        let parsed = parse_publication_metadata(&json!({
            "title": "精彩对局",
            "dynamic": "欢迎点赞投币关注我！",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert!(validate_publication_metadata(&parsed).is_err());
    }

    #[test]
    fn publication_metadata_removes_emoji_from_text_fields() {
        let parsed = parse_publication_metadata(&json!({
            "title": "新英雄登场 💀🔥",
            "dynamic": "本期展示冠军对局。🏆",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert_eq!(parsed.title, "新英雄登场");
        assert_eq!(parsed.dynamic, "本期展示冠军对局。");
    }

    #[test]
    fn publication_metadata_strips_hashtags_and_links_instead_of_failing() {
        // AI 照抄 YouTube 原标题里的 hashtag/链接曾让校验反复失败，最终把任务推进死信。
        let parsed = parse_publication_metadata(&json!({
            "title": "可怜的艾莉 #bs ＃brawlstars 第2078集",
            "dynamic": "本期展示冠军对局，完整版见 https://youtu.be/abc 和 www.example.com。",
            "tags": ["荒野乱斗", "排位赛"]
        }))
        .unwrap();
        assert_eq!(parsed.title, "可怜的艾莉 第2078集");
        assert_eq!(parsed.dynamic, "本期展示冠军对局，完整版见 和");
        validate_publication_metadata(&parsed).unwrap();
    }

    #[test]
    fn publication_metadata_strips_hashtag_and_link_inside_a_token() {
        let parsed = parse_publication_metadata(&json!({
            "title": "全球第一玩家#brawlstars",
            "dynamic": "开局连续三杀。详情见https://youtu.be/abc",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert_eq!(parsed.title, "全球第一玩家");
        assert_eq!(parsed.dynamic, "开局连续三杀。详情见");
        validate_publication_metadata(&parsed).unwrap();
    }

    #[test]
    fn title_made_entirely_of_hashtags_keeps_the_words() {
        // 线上死信 jBb5bAqhLKY：原标题就是 `#sync`、简介为空，direct 模式下 AI
        // 除了照抄没有别的素材。剥成空标题同样过不了校验，所以退让到保词去标记。
        let parsed = parse_publication_metadata(&json!({
            "title": "#sync",
            "dynamic": "该视频内容暂无可用描述。",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert_eq!(parsed.title, "sync");
        validate_publication_metadata(&parsed).unwrap();

        let parsed = parse_publication_metadata(&json!({
            "title": "#同步 ＃brawlstars",
            "dynamic": "该视频内容暂无可用描述。",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert_eq!(parsed.title, "同步 brawlstars");
        validate_publication_metadata(&parsed).unwrap();
    }

    #[test]
    fn title_made_entirely_of_links_stays_invalid() {
        // 没有任何词可留时不硬编一个标题，交回反馈重试让 AI 重写。
        let parsed = parse_publication_metadata(&json!({
            "title": "https://youtu.be/abc www.example.com",
            "dynamic": "该视频内容暂无可用描述。",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert!(parsed.title.is_empty());
        assert!(validate_publication_metadata(&parsed).is_err());
    }

    #[test]
    fn saved_metadata_with_hashtag_is_repaired_rather_than_failing_forever() {
        let saved = PublicationMetadata {
            title: "可怜的艾莉 #bs".into(),
            dynamic: "最后一局上演极限翻盘。".into(),
            tags: vec!["荒野乱斗".into(), "排位赛".into()],
            tid: BILIBILI_TID,
            raw_json: "{}".into(),
        };
        assert!(validate_publication_metadata(&saved).is_err());
        let repaired = repair_publication_metadata(&saved);
        assert_eq!(repaired.title, "可怜的艾莉");
        validate_publication_metadata(&repaired).unwrap();

        // 清洗后标题为空的元数据救不回来，调用方应重新生成而不是复用。
        let empty = PublicationMetadata {
            title: "https://youtu.be/abc".into(),
            ..saved
        };
        assert!(validate_publication_metadata(&repair_publication_metadata(&empty)).is_err());
    }

    #[test]
    fn metadata_payload_uses_full_or_uniformly_sampled_bilingual_subtitles() {
        let mut cues = (0..100)
            .map(|i| cue(i, "a representative subtitle sentence with context"))
            .collect::<Vec<_>>();
        for (index, cue) in cues.iter_mut().enumerate() {
            cue.translation = Some(format!("第{index}句译文"));
        }
        let full =
            build_publication_payload(TransferMode::Translated, &metadata(), Some(&cues), 200_000)
                .unwrap();
        assert_eq!(full["subtitle_sampling"]["sampled"], false);
        assert_eq!(full["subtitles"].as_array().unwrap().len(), cues.len());

        let minimum = publication_payload_value(
            TransferMode::Translated,
            &metadata(),
            &cues,
            &[0, cues.len() - 1],
            true,
        );
        let sampled = build_publication_payload(
            TransferMode::Translated,
            &metadata(),
            Some(&cues),
            estimate_publication_tokens(&minimum) + 250,
        )
        .unwrap();
        let included = sampled["subtitles"].as_array().unwrap();
        assert_eq!(sampled["subtitle_sampling"]["sampled"], true);
        assert!(included.len() < cues.len());
        assert_eq!(included.first().unwrap()["i"], 0);
        assert_eq!(included.last().unwrap()["i"], cues.len() - 1);

        let mut large = (0..1_075)
            .map(|i| cue(i, "a representative subtitle sentence with context"))
            .collect::<Vec<_>>();
        for (index, cue) in large.iter_mut().enumerate() {
            cue.translation = Some(format!("第{index}句包含足够长度的中文字幕内容"));
        }
        let bounded =
            build_publication_payload(TransferMode::Translated, &metadata(), Some(&large), 200_000)
                .unwrap();
        assert!(bounded.to_string().len() <= PI_MAX_PROMPT_ARGUMENT_BYTES);
        // 128KB 上限下 1075 条双语字幕（约 300KB）触发均匀采样。
        assert_eq!(bounded["subtitle_sampling"]["sampled"], true);
        assert!(bounded["subtitles"].as_array().unwrap().len() < large.len());
    }

    #[test]
    fn direct_metadata_accepts_empty_description_but_translated_needs_subtitles() {
        let mut meta = metadata();
        meta.description = None;
        let payload =
            build_publication_payload(TransferMode::Direct, &meta, None, 200_000).unwrap();
        assert_eq!(payload["youtube"]["description"], "");
        assert!(build_publication_payload(TransferMode::Translated, &meta, None, 200_000).is_err());
    }

    #[test]
    fn upload_args_are_fixed_original_metadata_and_detailed_description() {
        let publication = parse_publication_metadata(&json!({
            "title": "2026年最佳排位赛",
            "dynamic": "最后一局上演极限翻盘。",
            "tags": ["荒野乱斗", "排位赛"]
        }))
        .unwrap();
        let args = build_upload_args(&publication, &metadata());
        let value_after = |flag: &str| {
            let index = args.iter().position(|value| value == flag).unwrap();
            args[index + 1].as_str()
        };
        assert_eq!(value_after("--tid"), "172");
        assert_eq!(value_after("--submit"), "web");
        assert_eq!(value_after("--copyright"), "1");
        assert_eq!(value_after("--no-reprint"), "0");
        assert!(!args.iter().any(|value| value == "--source"));
        assert_eq!(value_after("--tag"), "荒野乱斗,排位赛");
        assert_eq!(value_after("--dynamic"), "最后一局上演极限翻盘。");
        let description = value_after("--desc");
        assert!(description.contains("原标题：Best Ranked Match 2026"));
        assert!(description.contains("来源：https://www.youtube.com/watch?v=video"));
        assert!(description.contains("原作者：Player One"));
        assert!(description.contains("原视频发布时间："));
        assert!(!description.contains("原发布日期："));
        assert!(description.contains("处理工具：https://github.com/Mist-wu/y2b-rs"));
        assert!(!description.contains("处理方式"));
    }

    #[test]
    fn description_removes_hashtags_and_includes_publication_date() {
        let mut meta = metadata();
        meta.title = "Poor   Alli #bs #brawlstars ＃keepbrawlalive".into();
        meta.url = "https://www.youtube.com/watch?v=F8yN5-ctCZw".into();
        meta.webpage_url = Some(meta.url.clone());
        meta.uploader = Some("Bazilious".into());
        let description = build_description(&meta);
        let expected = format!(
            "原标题：Poor Alli\n来源：https://www.youtube.com/watch?v=F8yN5-ctCZw\n原作者：Bazilious\n原视频发布时间：{}\n处理工具：https://github.com/Mist-wu/y2b-rs",
            publication_date(&meta)
        );
        assert_eq!(description, expected);
    }

    #[test]
    fn description_omits_title_when_it_contains_only_hashtags() {
        let mut meta = metadata();
        meta.title = "#bs ＃brawlstars".into();
        let description = build_description(&meta);
        assert!(!description.contains("原标题："));
        assert!(description.starts_with("来源：https://www.youtube.com/watch?v=video\n"));
        assert!(description.contains("处理工具：https://github.com/Mist-wu/y2b-rs"));
        assert!(!description.contains("处理方式"));
    }
}
