//! 字幕流程：下载英文字幕、Pi 分句、Pi 翻译，以及各级缓存/检查点。
use super::ai::{
    PI_MAX_PROMPT_ARGUMENT_BYTES, PI_PROMPT_OVERHEAD_TOKENS, batch_mode_name,
    estimate_segment_argument_bytes, estimate_segment_tokens, estimate_translation_tokens,
    is_ai_global_fault, parse_ranges, parse_translations, segment_cue_argument_bytes,
    segment_cue_tokens, translation_cue_tokens,
};
use super::publication::chinese_width;
use super::{Pipeline, StageGuard};
use crate::config::BatchMode;
use crate::model::Job;
use crate::model::VideoMetadata;
use crate::monitor::ytdlp_command;
use crate::process::run_monitored;
use crate::subtitle::{self, Cue};
use anyhow::{Context, Result, bail};
use futures::{StreamExt, stream};
use regex::Regex;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
pub(super) struct PreparedSubtitle {
    pub(super) cues: Vec<Cue>,
}

/// Pi 负责选择语义边界，但这些可读性限制必须由程序兜底，不能只依赖提示词。
pub(super) const SEGMENT_REQUIRED_GAP_SECONDS: f64 = 0.8;
pub(super) const SEGMENT_MAX_DURATION_SECONDS: f64 = 8.0;
pub(super) const SEGMENT_MAX_SOURCE_CHARS: usize = 72;
pub(super) const SEGMENT_MAX_SOURCE_WORDS: usize = 16;
pub(super) const SEGMENT_ORPHAN_MAX_DURATION_SECONDS: f64 = 1.0;

/// 译文以 32 中文宽度为目标；64 是防止整段串入单句等异常输出的硬上限。
/// 这里允许目标值两倍的余量，避免为了机械缩短而丢失名字、数字或事实。
pub(super) const TRANSLATION_MAX_WIDTH: usize = 64;

pub(super) fn max_segment_window_end(
    cues: &[Cue],
    start: usize,
    token_budget: usize,
    byte_budget: usize,
) -> Result<usize> {
    if start >= cues.len() {
        bail!("分句窗口起点越界: {start}/{}", cues.len())
    }
    let mut total = PI_PROMPT_OVERHEAD_TOKENS;
    let mut bytes = 512usize;
    let mut end = None;
    for (index, cue) in cues.iter().enumerate().skip(start) {
        let item = segment_cue_tokens(cue);
        let item_bytes = segment_cue_argument_bytes(index - start, cue);
        if total.saturating_add(item) > token_budget
            || bytes.saturating_add(item_bytes) > byte_budget
        {
            break;
        }
        total += item;
        bytes += item_bytes;
        end = Some(index);
    }
    end.with_context(|| {
        format!(
            "单条字幕已超过安全输入阈值: cue={start}, estimated_tokens={}/{token_budget}, bytes={}/{byte_budget}",
            PI_PROMPT_OVERHEAD_TOKENS + segment_cue_tokens(&cues[start]),
            512 + segment_cue_argument_bytes(0, &cues[start])
        )
    })
}

pub(super) fn translation_batches(
    cues: &[Cue],
    budget: usize,
    max_cues: usize,
) -> Result<Vec<(usize, usize)>> {
    if cues.is_empty() {
        return Ok(Vec::new());
    }
    if max_cues == 0 {
        bail!("translation_batch_cues 必须大于 0")
    }
    let mut batches = Vec::new();
    let mut start = 0;
    let mut total = PI_PROMPT_OVERHEAD_TOKENS;
    for (index, cue) in cues.iter().enumerate() {
        let item = translation_cue_tokens(cue);
        if PI_PROMPT_OVERHEAD_TOKENS.saturating_add(item) > budget {
            bail!(
                "单句翻译已超过安全 token 阈值 {budget}: cue={index}, estimated={}",
                PI_PROMPT_OVERHEAD_TOKENS + item
            )
        }
        if index > start && (index - start >= max_cues || total.saturating_add(item) > budget) {
            batches.push((start, index));
            start = index;
            total = PI_PROMPT_OVERHEAD_TOKENS;
        }
        total += item;
    }
    batches.push((start, cues.len()));
    Ok(batches)
}

pub(super) fn validate_ranges_cover(len: usize, ranges: &[(usize, usize)]) -> Result<()> {
    let mut expected = 0;
    for &(start, end) in ranges {
        if start != expected || end < start || end >= len {
            bail!("Pi 分句范围不连续或越界: {start}..{end}, expected={expected}, len={len}")
        }
        expected = end + 1;
    }
    if expected != len {
        bail!("Pi 分句没有覆盖完整窗口: {expected}/{len}")
    }
    Ok(())
}

fn source_range_metrics(cues: &[Cue], start: usize, end: usize) -> (f64, usize, usize) {
    let duration = cues[end].end - cues[start].start;
    let chars = cues[start..=end]
        .iter()
        .map(|cue| cue.source.chars().count())
        .sum::<usize>()
        + end.saturating_sub(start);
    let words = cues[start..=end]
        .iter()
        .map(|cue| cue.source.split_whitespace().count())
        .sum();
    (duration, chars, words)
}

fn source_range_within_limits(cues: &[Cue], start: usize, end: usize) -> bool {
    let (duration, chars, words) = source_range_metrics(cues, start, end);
    duration <= SEGMENT_MAX_DURATION_SECONDS
        && chars <= SEGMENT_MAX_SOURCE_CHARS
        && words <= SEGMENT_MAX_SOURCE_WORDS
}

/// 在 Pi 返回的语义范围内做确定性细分：遇到静音间隔或加入下一条后超过任一
/// 硬限制，就在原始 cue 边界处断开。单个原始 cue 本身无法再精确拆时按提示词
/// 约定保留，不能伪造词级时间戳。
pub(super) fn enforce_segment_limits(
    cues: &[Cue],
    model_ranges: &[(usize, usize)],
) -> Result<Vec<(usize, usize)>> {
    validate_ranges_cover(cues.len(), model_ranges)?;
    let mut repaired = Vec::new();
    for &(model_start, model_end) in model_ranges {
        let mut start = model_start;
        for current in (model_start + 1)..=model_end {
            let gap = cues[current].start - cues[current - 1].end;
            if gap >= SEGMENT_REQUIRED_GAP_SECONDS
                || !source_range_within_limits(cues, start, current)
            {
                repaired.push((start, current - 1));
                start = current;
            }
        }
        repaired.push((start, model_end));
    }
    validate_ranges_cover(cues.len(), &repaired)?;
    Ok(repaired)
}

fn source_ends_sentence(source: &str) -> bool {
    source
        .trim_end_matches(|character: char| {
            character.is_whitespace() || matches!(character, '"' | '\'' | '”' | '’' | ')' | ']')
        })
        .ends_with(['.', '!', '?'])
}

/// 模型偶发把句末单词切成极短的孤儿 cue（如 `... feel` / `pressure.`）。若右侧
/// 不超过 1 秒且没有换说话人或静音，优先直接接回上一句；合并会超限时，把
/// 前一范围末尾的原子 cue 移到右侧，避免在满足硬上限的代价下留下半句话。
pub(super) fn merge_orphaned_short_ranges(
    cues: &[Cue],
    ranges: &[(usize, usize)],
) -> Result<Vec<(usize, usize)>> {
    validate_ranges_cover(cues.len(), ranges)?;
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for &(range_start, end) in ranges {
        let mut start = range_start;
        let Some(previous) = merged.last_mut() else {
            merged.push((start, end));
            continue;
        };
        let right_duration = cues[end].end - cues[start].start;
        let gap = cues[start].start - cues[previous.1].end;
        let starts_speaker = cues[start].source.trim_start().starts_with(">>");
        let orphaned = right_duration <= SEGMENT_ORPHAN_MAX_DURATION_SECONDS
            && gap < SEGMENT_REQUIRED_GAP_SECONDS
            && !starts_speaker
            && !source_ends_sentence(&cues[previous.1].source);
        if orphaned && source_range_within_limits(cues, previous.0, end) {
            previous.1 = end;
            continue;
        }
        if orphaned {
            while previous.0 < previous.1 {
                let shifted = previous.1;
                if !source_range_within_limits(cues, previous.0, shifted - 1)
                    || !source_range_within_limits(cues, shifted, end)
                {
                    break;
                }
                previous.1 = shifted - 1;
                start = shifted;
                if cues[end].end - cues[start].start > SEGMENT_ORPHAN_MAX_DURATION_SECONDS {
                    break;
                }
            }
        }
        merged.push((start, end));
    }
    validate_ranges_cover(cues.len(), &merged)?;
    Ok(merged)
}

pub(super) fn validate_translation_indexes(
    len: usize,
    translations: &[(usize, String)],
) -> Result<()> {
    if translations.len() != len {
        bail!("Pi 翻译数量不匹配: {}/{}", translations.len(), len)
    }
    for (expected, (index, _)) in translations.iter().enumerate() {
        if *index != expected {
            bail!("Pi 翻译索引无序或缺失: index={index}, expected={expected}")
        }
    }
    Ok(())
}

pub(super) fn validate_translation_output(
    source: &[Cue],
    translations: &[(usize, String)],
) -> Result<()> {
    validate_translation_indexes(source.len(), translations)?;
    let speaker_marker = Regex::new(r"(?:^|\s)(?:>>|＞＞)(?:\s|$)")?;
    for (index, text) in translations {
        let text = text.trim();
        if text.is_empty() && source[*index].source.chars().any(char::is_alphanumeric) {
            bail!("Pi 翻译第 {index} 条译文为空")
        }
        let lower = text.to_ascii_lowercase();
        let has_speaker_marker = speaker_marker.is_match(text);
        let has_music_marker = lower.contains("[music]")
            || text.contains("[音乐]")
            || text.contains("【音乐】")
            || text.contains("（音乐）");
        if has_speaker_marker || text.contains("&gt;") || has_music_marker {
            bail!("Pi 翻译第 {index} 条含有禁止显示的说话人/音乐标记")
        }
        let width = chinese_width(text);
        if width > TRANSLATION_MAX_WIDTH {
            bail!("Pi 翻译第 {index} 条中文宽度 {width} 超过硬上限 {TRANSLATION_MAX_WIDTH}")
        }
    }
    Ok(())
}

pub(super) fn load_curated_glossary(path: &Path) -> Result<Vec<(String, String)>> {
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("读取翻译策略词库失败: {}", path.display()))?;
    let glossary = value["glossary"]
        .as_object()
        .with_context(|| format!("翻译策略缺少 glossary: {}", path.display()))?;
    Ok(glossary
        .iter()
        .filter_map(|(term, translation)| {
            translation
                .as_str()
                .filter(|translation| !term.trim().is_empty() && !translation.trim().is_empty())
                .map(|translation| (term.clone(), translation.to_string()))
        })
        .collect())
}

fn source_contains_glossary_term(source: &str, term: &str) -> Result<bool> {
    let pattern = format!(
        "(?i)(?:^|[^A-Za-z0-9]){}(?:$|[^A-Za-z0-9])",
        regex::escape(term.trim())
    );
    Ok(Regex::new(&pattern)?.is_match(source))
}

pub(super) fn validate_translation_glossary(
    source: &[Cue],
    translations: &[(usize, String)],
    glossary: &[(String, String)],
) -> Result<()> {
    for (index, text) in translations {
        for (term, required) in glossary {
            if source_contains_glossary_term(&source[*index].source, term)?
                && !text.contains(required)
            {
                bail!("Pi 翻译第 {index} 条未按词库使用 {term} => {required}")
            }
        }
    }
    // 硬分句可能恰好切在多词术语中间（例如 `draft` / `diff`）。逐 cue
    // 校验看不到完整术语，模型就可能把它译成“阵容压制”。额外检查相邻 cue，
    // 要求跨边界的源术语在两条译文拼接后仍出现规定译名。
    for pair in translations.windows(2) {
        let (left_index, left_translation) = &pair[0];
        let (right_index, right_translation) = &pair[1];
        if *right_index != *left_index + 1 {
            continue;
        }
        let left_source = &source[*left_index].source;
        let right_source = &source[*right_index].source;
        let joined_source = format!("{left_source} {right_source}");
        let joined_translation = format!("{left_translation}{right_translation}");
        for (term, required) in glossary {
            if !source_contains_glossary_term(left_source, term)?
                && !source_contains_glossary_term(right_source, term)?
                && source_contains_glossary_term(&joined_source, term)?
                && !joined_translation.contains(required)
            {
                bail!(
                    "Pi 翻译第 {left_index}/{right_index} 条跨句术语未按词库使用 {term} => {required}"
                )
            }
        }
    }
    Ok(())
}

pub(super) fn load_segmented_cache(path: &Path) -> Result<Option<Vec<Cue>>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut cues = subtitle::load_json(path).with_context(|| format!("读取 {}", path.display()))?;
    validate_cached_cues(&cues)?;
    for cue in &mut cues {
        cue.translation = None;
    }
    Ok(Some(cues))
}

pub(super) fn load_translation_checkpoint(path: &Path, source: &[Cue]) -> Result<Option<Vec<Cue>>> {
    if !path.exists() {
        return Ok(None);
    }
    let cues = subtitle::load_json(path).with_context(|| format!("读取 {}", path.display()))?;
    validate_cached_cues(&cues)?;
    if cues.len() != source.len() {
        bail!("翻译缓存数量不匹配: {}/{}", cues.len(), source.len())
    }
    for (index, (cached, original)) in cues.iter().zip(source).enumerate() {
        if cached.source != original.source
            || (cached.start - original.start).abs() > 0.001
            || (cached.end - original.end).abs() > 0.001
        {
            bail!("翻译缓存第 {index} 条与分句缓存不匹配")
        }
        if let Some(translation) = cached.translation.as_deref() {
            if translation.trim().is_empty() && cached.source.chars().any(char::is_alphanumeric) {
                bail!("翻译缓存第 {index} 条译文为空")
            }
            if translation.chars().any(|ch| ch.is_control() && ch != '\n') {
                bail!("翻译缓存第 {index} 条含非法控制字符")
            }
            let width = chinese_width(translation.trim());
            if width > TRANSLATION_MAX_WIDTH {
                bail!("翻译缓存第 {index} 条中文宽度 {width} 超过硬上限 {TRANSLATION_MAX_WIDTH}")
            }
        }
    }
    Ok(Some(cues))
}

pub(super) fn translation_checkpoint_complete(cues: &[Cue]) -> bool {
    cues.iter().all(|cue| cue.translation.is_some())
}

pub(super) fn translation_batch_checkpointed(cues: &[Cue], start: usize, end: usize) -> bool {
    cues[start..end].iter().all(|cue| cue.translation.is_some())
}

pub(super) fn validate_cached_cues(cues: &[Cue]) -> Result<()> {
    if cues.is_empty() {
        bail!("字幕缓存为空")
    }
    for (index, cue) in cues.iter().enumerate() {
        if !cue.start.is_finite() || !cue.end.is_finite() || cue.start < 0.0 || cue.end <= cue.start
        {
            bail!("字幕缓存第 {index} 条时间无效")
        }
        if cue.source.trim().is_empty() {
            bail!("字幕缓存第 {index} 条原文为空")
        }
        if index > 0 && cue.start + 0.001 < cues[index - 1].start {
            bail!("字幕缓存第 {index} 条开始时间早于前一条")
        }
    }
    Ok(())
}

pub(super) fn choose_adaptive_boundary(
    local: &[(usize, usize)],
    window_start: usize,
    cursor: usize,
    preferred_end: usize,
    window_end: usize,
) -> Result<usize> {
    local
        .iter()
        .map(|(_, end)| window_start + end)
        .filter(|&end| end >= cursor && end <= window_end)
        .min_by_key(|&end| {
            (
                end.abs_diff(preferred_end),
                usize::from(end > preferred_end),
            )
        })
        .context("Pi 分句窗口中没有可用边界")
}

pub(super) fn append_core_ranges(
    output: &mut Vec<(usize, usize)>,
    local: &[(usize, usize)],
    window_start: usize,
    core_start: usize,
    core_end: usize,
) -> Result<()> {
    let mut expected = core_start;
    for &(start, end) in local {
        let global_start = window_start + start;
        let global_end = window_start + end;
        if global_end < core_start {
            continue;
        }
        if global_start > core_end {
            break;
        }
        let clipped_start = global_start.max(core_start);
        let clipped_end = global_end.min(core_end);
        if clipped_start != expected || clipped_end < clipped_start {
            bail!("重叠分句合并失败: {clipped_start}..{clipped_end}, expected={expected}")
        }
        output.push((clipped_start, clipped_end));
        expected = clipped_end + 1;
    }
    if expected != core_end + 1 {
        bail!("重叠分句未覆盖核心窗口: {expected}/{}", core_end + 1)
    }
    Ok(())
}

/// 选出用于分句/翻译的英文字幕文件。
///
/// `--sub-langs "en.*,en"` 可能同时落盘 `<id>.en.vtt`、`<id>.en-US.vtt`、
/// `<id>.en-orig.vtt`；`read_dir` 的顺序依赖文件系统，直接取第一个会让同一
/// 视频重跑时选到不同字幕源。这里固定优先级：精确 `en` > 其余语言标签字典序，
/// 保证结果可复现。
pub(super) fn pick_subtitle_file(work: &Path, video_id: &str) -> Result<Option<PathBuf>> {
    let mut candidates = fs::read_dir(work)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "vtt")
                && path.metadata().is_ok_and(|metadata| metadata.len() > 0)
        })
        .map(|path| (subtitle_language_tag(&path, video_id), path))
        .collect::<Vec<_>>();
    candidates.sort_by(|(left_lang, left_path), (right_lang, right_path)| {
        subtitle_language_rank(left_lang)
            .cmp(&subtitle_language_rank(right_lang))
            .then_with(|| left_lang.cmp(right_lang))
            .then_with(|| left_path.cmp(right_path))
    });
    Ok(candidates.into_iter().next().map(|(_, path)| path))
}

/// 语言标签优先级：精确 `en` > 具名变体（`en-US`…） > 无法识别的文件名。
pub(super) fn subtitle_language_rank(language: &str) -> u8 {
    match language {
        "en" => 0,
        "" => 2,
        _ => 1,
    }
}

/// 从 `<video_id>.<language>.vtt` 取出语言标签；文件名不匹配时返回空串。
pub(super) fn subtitle_language_tag(path: &Path, video_id: &str) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(video_id))
        .and_then(|rest| rest.strip_suffix(".vtt"))
        .unwrap_or_default()
        .trim_matches('.')
        .to_string()
}

impl Pipeline {
    pub(super) async fn prepare_translated_subtitle(
        &self,
        job: &Job,
        meta: &VideoMetadata,
        work: &Path,
    ) -> Result<Option<PreparedSubtitle>> {
        let segmented = work.join(format!("{}.en.segmented.json", meta.id));
        let translated = work.join(format!("{}.en-zh-CN.translated.json", meta.id));

        let cached = match load_segmented_cache(&segmented) {
            Ok(Some(cues)) => {
                self.db.event(
                    Some(&job.id),
                    "info",
                    &format!("复用分句缓存: {} cues", cues.len()),
                )?;
                Some(cues)
            }
            Ok(None) => None,
            Err(error) => {
                self.db
                    .event(Some(&job.id), "warn", &format!("忽略无效分句缓存: {error}"))?;
                None
            }
        };
        let mut cues = match cached {
            Some(cues) => cues,
            None => {
                let Some(fresh) = self.segment_uncached(job, meta, work, &segmented).await? else {
                    return Ok(None);
                };
                fresh
            }
        };

        match load_translation_checkpoint(&translated, &cues) {
            Ok(Some(cached)) if translation_checkpoint_complete(&cached) => {
                self.db.event(
                    Some(&job.id),
                    "info",
                    &format!("复用翻译缓存: {} cues", cached.len()),
                )?;
                cues = cached;
            }
            Ok(Some(cached)) => {
                let completed = cached
                    .iter()
                    .filter(|cue| cue.translation.is_some())
                    .count();
                self.db.event(
                    Some(&job.id),
                    "info",
                    &format!("续传翻译检查点: {completed}/{} cues", cached.len()),
                )?;
                cues = cached;
                self.translate_and_save(&job.id, &mut cues, &translated)
                    .await?;
            }
            Ok(None) => {
                self.translate_and_save(&job.id, &mut cues, &translated)
                    .await?
            }
            Err(error) => {
                self.db
                    .event(Some(&job.id), "warn", &format!("忽略无效翻译缓存: {error}"))?;
                self.translate_and_save(&job.id, &mut cues, &translated)
                    .await?;
            }
        }
        Ok(Some(PreparedSubtitle { cues }))
    }

    /// 无缓存时完整跑一遍：下载字幕 → 解析 VTT → 分句 → 落盘。
    pub(super) async fn segment_uncached(
        &self,
        job: &Job,
        meta: &VideoMetadata,
        work: &Path,
        segmented: &Path,
    ) -> Result<Option<Vec<Cue>>> {
        let Some(raw_sub) = self
            .download_subtitle(&job.id, &job.url, &meta.id, work)
            .await?
        else {
            return Ok(None);
        };
        let source = subtitle::parse_vtt(&raw_sub)?;
        let cues = self.segment(&job.id, &source).await?;
        subtitle::save_json(&cues, segmented)?;
        Ok(Some(cues))
    }

    /// 翻译并原子写检查点，供翻译缓存缺省/续传后复用。
    pub(super) async fn translate_and_save(
        &self,
        job_id: &str,
        cues: &mut [Cue],
        checkpoint: &Path,
    ) -> Result<()> {
        self.translate(job_id, cues, checkpoint).await?;
        subtitle::save_json(cues, checkpoint)?;
        Ok(())
    }

    pub(super) async fn download_subtitle(
        &self,
        job_id: &str,
        url: &str,
        video_id: &str,
        work: &Path,
    ) -> Result<Option<PathBuf>> {
        let mut stage = StageGuard::start(&self.db, job_id, "subtitle_download", None, None, None)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            "en.*,en",
            "--sub-format",
            "vtt",
            "--no-playlist",
            "--no-overwrites",
        ]);
        cmd.arg("-o")
            .arg(work.join(format!("{video_id}.%(language)s.%(ext)s")));
        cmd.arg(url);
        let result = run_monitored(cmd, Duration::from_secs(180)).await;
        match result {
            Ok(out) => {
                let found = pick_subtitle_file(work, video_id)?;
                let status = if found.is_some() {
                    "completed"
                } else {
                    "missing"
                };
                let detail = found
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned());
                stage.finish(status, out.duration_ms, out.peak_rss_kib, detail.as_deref())?;
                Ok(found)
            }
            Err(error) => {
                let elapsed = stage.elapsed_ms();
                Err(stage.fail(error, elapsed, 0)).context("字幕下载失败")
            }
        }
    }

    pub(super) async fn segment(&self, job_id: &str, cues: &[Cue]) -> Result<Vec<Cue>> {
        let mut stage = StageGuard::start(
            &self.db,
            job_id,
            "segmentation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let budget = self.ai_token_budget()?;
        let estimated = estimate_segment_tokens(cues);
        let estimated_bytes = estimate_segment_argument_bytes(cues);
        let mut ranges = Vec::new();
        let mut duration = 0;
        let mut peak = 0;
        let mut calls = 0;
        let fits_tokens = estimated <= budget && estimated_bytes <= PI_MAX_PROMPT_ARGUMENT_BYTES;
        let fits_window = fits_tokens && cues.len() <= self.config.ai.segment_max_cues;
        if self.config.ai.batch_mode == BatchMode::WholeVideo || fits_window {
            if !fits_tokens {
                bail!(
                    "whole_video 分句超过安全输入阈值: estimated_tokens={estimated}/{budget}, bytes={estimated_bytes}/{PI_MAX_PROMPT_ARGUMENT_BYTES}；请改用 adaptive"
                )
            }
            let batch = self
                .segment_batch(job_id, stage.id(), cues, 0, cues.len().saturating_sub(1))
                .await;
            let (local, elapsed, rss) = match batch {
                Ok(result) => result,
                Err(error) => {
                    let elapsed = stage.elapsed_ms();
                    return Err(stage.fail(error, elapsed, 0));
                }
            };
            ranges = local;
            duration += elapsed;
            peak = peak.max(rss);
            calls = 1;
        } else {
            let overlap = self.config.ai.segment_overlap_cues;
            let mut cursor = 0;
            while cursor < cues.len() {
                let window_start = cursor.saturating_sub(overlap);
                let budget_end = max_segment_window_end(
                    cues,
                    window_start,
                    budget,
                    PI_MAX_PROMPT_ARGUMENT_BYTES,
                )?;
                let window_end = budget_end.min(window_start + self.config.ai.segment_max_cues - 1);
                if window_end < cursor {
                    bail!("安全 token 阈值过小，无法容纳分句核心字幕")
                }
                let has_more = window_end + 1 < cues.len();
                let preferred_end = if has_more {
                    window_end.saturating_sub(overlap).max(cursor)
                } else {
                    cues.len() - 1
                };
                let batch = self
                    .segment_batch(
                        job_id,
                        stage.id(),
                        &cues[window_start..=window_end],
                        cursor - window_start,
                        preferred_end - window_start,
                    )
                    .await;
                let (local, elapsed, rss) = match batch {
                    Ok(result) => result,
                    Err(error) => {
                        let wall = stage.elapsed_ms();
                        return Err(stage.fail(error, wall, peak));
                    }
                };
                let chosen_end = if has_more {
                    choose_adaptive_boundary(
                        &local,
                        window_start,
                        cursor,
                        preferred_end,
                        window_end,
                    )?
                } else {
                    cues.len() - 1
                };
                append_core_ranges(&mut ranges, &local, window_start, cursor, chosen_end)?;
                cursor = chosen_end + 1;
                duration += elapsed;
                peak = peak.max(rss);
                calls += 1;
            }
        }
        let model_range_count = ranges.len();
        ranges = enforce_segment_limits(cues, &ranges)?;
        let hard_splits = ranges.len().saturating_sub(model_range_count);
        let before_orphan_merge = ranges.len();
        ranges = merge_orphaned_short_ranges(cues, &ranges)?;
        let orphan_merges = before_orphan_merge.saturating_sub(ranges.len());
        let result = subtitle::apply_ranges(cues, &ranges)?;
        stage.finish(
            "completed",
            duration,
            peak,
            Some(&format!(
                "{} -> {} cues; mode={}; estimated_tokens={estimated}; estimated_bytes={estimated_bytes}; calls={calls}; hard_splits={hard_splits}; orphan_merges={orphan_merges}",
                cues.len(),
                result.len(),
                batch_mode_name(self.config.ai.batch_mode)
            )),
        )?;
        Ok(result)
    }

    pub(super) async fn segment_batch(
        &self,
        job_id: &str,
        stage: i64,
        cues: &[Cue],
        core_start: usize,
        preferred_end: usize,
    ) -> Result<(Vec<(usize, usize)>, i64, u64)> {
        let payload = json!({
            "task":"segment",
            "source_lang":self.config.translation.source_lang,
            "core_start":core_start,
            "preferred_end":preferred_end,
            "tokens":cues.iter().enumerate().map(|(i,c)|json!({
                "i":i,
                "start":c.start,
                "end":c.end,
                "text":c.source
            })).collect::<Vec<_>>()
        });
        // 分句输出偶发 JSON 截断（deepseek 输出限制），失败时复用翻译的重试次数退避重试。
        let attempts = self.config.ai.translation_batch_retries.saturating_add(1);
        let mut last_error = None;
        let mut result = None;
        for attempt in 1..=attempts {
            let r = self.call_pi(job_id, stage, payload.clone()).await;
            match r {
                Ok(r) => match parse_ranges(&r.value).and_then(|local| {
                    validate_ranges_cover(cues.len(), &local)?;
                    Ok(local)
                }) {
                    Ok(local) => {
                        result = Some((r, local));
                        break;
                    }
                    Err(error) => last_error = Some(error),
                },
                Err(error) if is_ai_global_fault(&error) => return Err(error),
                Err(error) => last_error = Some(error),
            }
            if attempt < attempts {
                self.db.event(
                    Some(job_id),
                    "warn",
                    &format!(
                        "分句窗口 {}..{} 第 {attempt}/{attempts} 次失败: {}",
                        core_start,
                        preferred_end,
                        last_error.as_ref().expect("分句重试前必有错误")
                    ),
                )?;
                sleep(Duration::from_secs((1u64 << (attempt - 1)).min(30))).await;
            }
        }
        let (r, local) = result.ok_or_else(|| {
            last_error
                .unwrap_or_else(|| anyhow::anyhow!("分句窗口 {core_start}..{preferred_end} 失败"))
        })?;
        Ok((local, r.output.duration_ms, r.output.peak_rss_kib))
    }

    pub(super) async fn translate(
        &self,
        job_id: &str,
        cues: &mut [Cue],
        checkpoint: &Path,
    ) -> Result<()> {
        let mut stage = StageGuard::start(
            &self.db,
            job_id,
            "translation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.translation_model),
            Some(&self.config.ai.thinking),
        )?;
        let budget = self.ai_token_budget()?;
        let estimated = estimate_translation_tokens(cues);
        let batches = if self.config.ai.batch_mode == BatchMode::WholeVideo {
            if estimated > budget {
                bail!(
                    "whole_video 翻译预计需要 {estimated} tokens，超过安全阈值 {budget}；请改用 adaptive"
                )
            }
            vec![(0, cues.len())]
        } else {
            translation_batches(cues, budget, self.config.ai.translation_batch_cues)?
        };
        if self.config.ai.translation_concurrency == 0 {
            bail!("translation_concurrency 必须大于 0")
        }
        let batch_inputs = batches
            .iter()
            .filter(|(start, end)| !translation_batch_checkpointed(cues, *start, *end))
            .map(|(start, end)| (*start, cues[*start..*end].to_vec()))
            .collect::<Vec<_>>();
        let reused_batches = batches.len() - batch_inputs.len();
        let concurrency = self
            .config
            .ai
            .translation_concurrency
            .min(batch_inputs.len().max(1));
        let stage_id = stage.id();
        let mut results = stream::iter(batch_inputs)
            .map(|(start, chunk)| async move {
                self.translate_batch_with_retry(job_id, stage_id, start, &chunk)
                    .await
            })
            .buffer_unordered(concurrency);
        let mut aggregate_call_ms = 0;
        let mut peak = 0;
        let mut checkpointed_batches = reused_batches;
        let mut failure = None;
        while let Some(result) = results.next().await {
            let (start, local, duration_ms, peak_rss_kib) = match result {
                Ok(result) => result,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            };
            let global = local
                .into_iter()
                .map(|(index, translation)| (index + start, translation))
                .collect::<Vec<_>>();
            subtitle::apply_translations(cues, &global)?;
            subtitle::save_json(cues, checkpoint)?;
            checkpointed_batches += 1;
            aggregate_call_ms += duration_ms;
            peak = peak.max(peak_rss_kib);
        }
        // 先释放流，取消仍在飞的 Pi 调用，再收尾阶段行。
        drop(results);
        let wall_duration_ms = stage.elapsed_ms();
        if let Some(error) = failure {
            let detail = format!("checkpointed={checkpointed_batches}/{}", batches.len());
            let error = error.context(detail);
            return Err(stage.fail(error, wall_duration_ms, peak));
        }
        stage.finish(
            "completed",
            wall_duration_ms,
            peak,
            Some(&format!(
                "{} cues; mode={}; estimated_tokens={estimated}; calls={}; reused_batches={reused_batches}; concurrency={concurrency}; aggregate_call_ms={aggregate_call_ms}",
                cues.len(),
                batch_mode_name(self.config.ai.batch_mode),
                batches.len() - reused_batches
            )),
        )?;
        Ok(())
    }

    pub(super) async fn translate_batch_with_retry(
        &self,
        job_id: &str,
        stage: i64,
        start: usize,
        chunk: &[Cue],
    ) -> Result<(usize, Vec<(usize, String)>, i64, u64)> {
        let items = chunk
            .iter()
            .enumerate()
            .map(|(i, cue)| json!({"i":i,"text":cue.source}))
            .collect::<Vec<_>>();
        let payload = json!({
            "task":"translate",
            "source_lang":self.config.translation.source_lang,
            "target_lang":self.config.translation.target_lang,
            "items":items
        });
        let glossary = load_curated_glossary(&self.config.ai.policy)?;
        // 每次尝试都会带上不同的 feedback；call_pi 在调用边界统一登记审计行。
        let attempts = self.config.ai.translation_batch_retries.saturating_add(1);
        let mut aggregate_duration_ms = 0;
        let mut peak_rss_kib = 0;
        let mut last_error = None;
        let mut feedback: Option<String> = None;

        for attempt in 1..=attempts {
            let mut call_payload = payload.clone();
            if let Some(message) = &feedback {
                call_payload["feedback"] = json!(message);
            }
            match self.call_pi(job_id, stage, call_payload).await {
                Ok(result) => {
                    aggregate_duration_ms += result.output.duration_ms;
                    peak_rss_kib = peak_rss_kib.max(result.output.peak_rss_kib);
                    match parse_translations(&result.value).and_then(|translations| {
                        validate_translation_output(chunk, &translations)?;
                        validate_translation_glossary(chunk, &translations, &glossary)?;
                        Ok(translations)
                    }) {
                        Ok(translations) => {
                            return Ok((start, translations, aggregate_duration_ms, peak_rss_kib));
                        }
                        Err(error) => {
                            let error_message = error.to_string();
                            last_error = Some(error);
                            // 输出结构无效时原样重试大概率再次失败：先带反馈让 AI 修正格式；
                            // 反馈仍无效则减半拆分重试，定位问题批次，避免整批 token 白烧。
                            if attempt < attempts {
                                feedback = Some(format!(
                                    "上一轮输出未通过解析/校验：{error_message}。请只输出符合要求的 JSON，不要输出解释、Markdown 或额外字段；每条译文必须完整保留原意、名字和数字，同时将中文宽度压缩到 {TRANSLATION_MAX_WIDTH} 以内。说话人切换请改写为自然中文标点，不得输出 >>、&gt;、[music] 或 [音乐]。"
                                ));
                                self.db.event(
                                    Some(job_id),
                                    "warn",
                                    &format!(
                                        "翻译批次 {}..{} 输出无效，带反馈重试: {error_message}",
                                        start,
                                        start + chunk.len()
                                    ),
                                )?;
                                continue;
                            }
                            if chunk.len() > 1 {
                                return self
                                    .split_translation_batch(
                                        job_id,
                                        stage,
                                        start,
                                        chunk,
                                        aggregate_duration_ms,
                                        peak_rss_kib,
                                    )
                                    .await;
                            }
                        }
                    }
                }
                Err(error) if is_ai_global_fault(&error) => return Err(error),
                Err(error) => last_error = Some(error),
            }

            let error = last_error
                .as_ref()
                .expect("translation retry must retain the preceding error");
            self.db.event(
                Some(job_id),
                if attempt < attempts { "warn" } else { "error" },
                &format!(
                    "翻译批次 {}..{} 第 {attempt}/{attempts} 次失败: {error}",
                    start,
                    start + chunk.len()
                ),
            )?;
            if attempt < attempts {
                // 指数退避：1s, 2s, 4s… 对偶发故障收敛，同时限制上游 API 压力。
                sleep(Duration::from_secs((1u64 << (attempt - 1)).min(30))).await;
            }
        }

        // 重试耗尽仍失败：对可拆批次降半重试（缩小请求面定位问题），
        // 而不是让整个批次（可能 50-100 条）白烧后直接失败。
        if chunk.len() > 1 {
            self.db.event(
                Some(job_id),
                "warn",
                &format!(
                    "翻译批次 {}..{} 重试耗尽，自动降半重试",
                    start,
                    start + chunk.len()
                ),
            )?;
            return self
                .split_translation_batch(
                    job_id,
                    stage,
                    start,
                    chunk,
                    aggregate_duration_ms,
                    peak_rss_kib,
                )
                .await;
        }

        Err(last_error
            .expect("translation retry loop must run at least once")
            .context(format!(
                "翻译批次 {}..{} 在 {attempts} 次尝试后失败",
                start,
                start + chunk.len()
            )))
    }

    /// 把翻译批次拆成左右两半分别重试，合并结果；用于结构无效或持续失败定位问题。
    pub(super) async fn split_translation_batch(
        &self,
        job_id: &str,
        stage: i64,
        start: usize,
        chunk: &[Cue],
        aggregate_duration_ms: i64,
        peak_rss_kib: u64,
    ) -> Result<(usize, Vec<(usize, String)>, i64, u64)> {
        let mid = chunk.len() / 2;
        let (left, right) = chunk.split_at(mid);
        let (_, left_result, left_ms, left_peak) =
            Box::pin(self.translate_batch_with_retry(job_id, stage, start, left)).await?;
        let (_, right_result, right_ms, right_peak) =
            Box::pin(self.translate_batch_with_retry(job_id, stage, start + mid, right)).await?;
        let merged = left_result
            .into_iter()
            .chain(
                right_result
                    .into_iter()
                    .map(|(index, text)| (index + mid, text)),
            )
            .collect::<Vec<_>>();
        Ok((
            start,
            merged,
            aggregate_duration_ms + left_ms + right_ms,
            peak_rss_kib.max(left_peak).max(right_peak),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::testing::cue;

    #[test]
    fn hard_segment_limits_split_model_ranges_deterministically() {
        let mut cues = vec![
            cue(0, "one two three four five six seven eight"),
            cue(
                1,
                "nine ten eleven twelve thirteen fourteen fifteen sixteen",
            ),
            cue(2, "short sentence"),
            cue(3, "another short sentence"),
        ];
        // 2..3 单独都很短，但 2 与 3 之间的静音间隔达到 0.8 秒，必须断开。
        cues[3].start = cues[2].end + SEGMENT_REQUIRED_GAP_SECONDS;
        cues[3].end = cues[3].start + 1.0;

        let repaired = enforce_segment_limits(&cues, &[(0, 3)]).unwrap();
        assert_eq!(repaired, vec![(0, 0), (1, 2), (3, 3)]);
    }

    #[test]
    fn hard_segment_limits_split_ranges_over_eight_seconds() {
        let mut cues = vec![cue(0, "one"), cue(1, "two"), cue(2, "three")];
        cues[0].start = 0.0;
        cues[0].end = 3.0;
        cues[1].start = 3.1;
        cues[1].end = 6.0;
        cues[2].start = 6.1;
        cues[2].end = 9.0;

        let repaired = enforce_segment_limits(&cues, &[(0, 2)]).unwrap();
        assert_eq!(repaired, vec![(0, 1), (2, 2)]);
    }

    #[test]
    fn hard_segment_limits_preserve_valid_model_boundaries() {
        let cues = vec![cue(0, "hello"), cue(1, "world"), cue(2, "again")];
        let repaired = enforce_segment_limits(&cues, &[(0, 1), (2, 2)]).unwrap();
        assert_eq!(repaired, vec![(0, 1), (2, 2)]);
    }

    #[test]
    fn hard_segment_limits_keep_unavoidable_oversize_input_cue() {
        let cues = vec![cue(0, &"word ".repeat(20)), cue(1, "next")];
        let repaired = enforce_segment_limits(&cues, &[(0, 1)]).unwrap();
        assert_eq!(repaired, vec![(0, 0), (1, 1)]);
        assert!(!source_range_within_limits(&cues, 0, 0));
    }

    #[test]
    fn orphaned_short_fragment_merges_back_when_limits_allow() {
        let mut cues = vec![cue(0, "because I usually do not feel"), cue(1, "pressure.")];
        cues[0].start = 10.0;
        cues[0].end = 14.0;
        cues[1].start = 14.0;
        cues[1].end = 14.4;
        assert_eq!(
            merge_orphaned_short_ranges(&cues, &[(0, 0), (1, 1)]).unwrap(),
            vec![(0, 1)]
        );

        cues[1].source = ">> Yes.".into();
        assert_eq!(
            merge_orphaned_short_ranges(&cues, &[(0, 0), (1, 1)]).unwrap(),
            vec![(0, 0), (1, 1)]
        );
    }

    #[test]
    fn orphaned_short_fragment_rebalances_when_direct_merge_exceeds_limit() {
        let mut cues = vec![
            cue(
                0,
                "one two three four five six seven eight nine ten eleven twelve",
            ),
            cue(1, "usually do not feel"),
            cue(2, "pressure."),
        ];
        cues[0].start = 0.0;
        cues[0].end = 3.0;
        cues[1].start = 3.0;
        cues[1].end = 4.0;
        cues[2].start = 4.0;
        cues[2].end = 4.4;
        assert!(!source_range_within_limits(&cues, 0, 2));
        assert_eq!(
            merge_orphaned_short_ranges(&cues, &[(0, 1), (2, 2)]).unwrap(),
            vec![(0, 0), (1, 2)]
        );
    }

    #[test]
    fn translation_output_rejects_empty_and_overwide_text() {
        let source = vec![cue(0, "hello")];
        assert!(validate_translation_output(&source, &[(0, "你好".into())]).is_ok());
        assert!(validate_translation_output(&source, &[(0, "".into())]).is_err());
        assert!(validate_translation_output(&source, &[(0, "中".repeat(33))]).is_err());
        assert!(validate_translation_output(&source, &[(0, ">> 你好".into())]).is_err());
        assert!(validate_translation_output(&source, &[(0, "你好 [音乐]".into())]).is_err());
        assert!(validate_translation_output(&source, &[(0, "你好。世界。".into())]).is_ok());
    }

    #[test]
    fn translation_output_requires_curated_glossary_in_the_same_cue() {
        let source = vec![cue(0, "Max gives Rico her Hypercharge.")];
        let glossary = vec![
            ("Max".into(), "麦克斯".into()),
            ("Rico".into(), "瑞科".into()),
            ("Hypercharge".into(), "极限充能".into()),
        ];
        assert!(
            validate_translation_glossary(
                &source,
                &[(0, "麦克斯把极限充能给了瑞科。".into())],
                &glossary
            )
            .is_ok()
        );
        let error =
            validate_translation_glossary(&source, &[(0, "她把充能给了瑞科。".into())], &glossary)
                .unwrap_err();
        assert!(error.to_string().contains("Max => 麦克斯"));

        let unrelated = vec![cue(0, "maximum speed")];
        assert!(
            validate_translation_glossary(&unrelated, &[(0, "最高速度".into())], &glossary).is_ok()
        );
    }

    #[test]
    fn translation_output_requires_glossary_across_a_cue_boundary() {
        let source = vec![cue(0, "ranked no draft"), cue(1, "diff beats mechanics")];
        let glossary = vec![("draft diff".into(), "BP差距".into())];
        assert!(
            validate_translation_glossary(
                &source,
                &[(0, "排位，不，BP".into()), (1, "差距比操作重要".into())],
                &glossary
            )
            .is_ok()
        );
        let error = validate_translation_glossary(
            &source,
            &[(0, "排位，不，阵容".into()), (1, "压制比操作重要".into())],
            &glossary,
        )
        .unwrap_err();
        assert!(error.to_string().contains("跨句术语"));
    }

    #[test]
    fn adaptive_translation_batches_are_contiguous() {
        let cues = (0..20)
            .map(|i| cue(i, "a moderately sized subtitle sentence"))
            .collect::<Vec<_>>();
        let batches = translation_batches(&cues, 2_300, 50).unwrap();
        assert!(batches.len() > 1);
        assert_eq!(batches.first().map(|x| x.0), Some(0));
        assert_eq!(batches.last().map(|x| x.1), Some(cues.len()));
        assert!(batches.windows(2).all(|pair| pair[0].1 == pair[1].0));
    }

    #[test]
    fn adaptive_translation_batches_respect_cue_limit() {
        let cues = (0..107)
            .map(|i| cue(i, "short subtitle"))
            .collect::<Vec<_>>();
        assert_eq!(
            translation_batches(&cues, 200_000, 50).unwrap(),
            vec![(0, 50), (50, 100), (100, 107)]
        );
    }

    #[test]
    fn adaptive_segmentation_respects_process_argument_byte_limit() {
        // 5.5KB 长句 × 100 条 ≈ 556KB，超过 512KB 窗口上限但不超过 token 预算。
        let long_text = "x".repeat(5_500);
        let cues = (0..100)
            .map(|index| cue(index, &long_text))
            .collect::<Vec<_>>();
        assert!(estimate_segment_tokens(&cues) < 200_000);
        assert!(estimate_segment_argument_bytes(&cues) > PI_MAX_PROMPT_ARGUMENT_BYTES);

        let end = max_segment_window_end(&cues, 0, 200_000, PI_MAX_PROMPT_ARGUMENT_BYTES).unwrap();
        assert!(end < cues.len() - 1);
        assert!(estimate_segment_argument_bytes(&cues[..=end]) <= PI_MAX_PROMPT_ARGUMENT_BYTES);
    }

    #[test]
    fn adaptive_segmentation_uses_overlap_and_model_boundary() {
        let local = vec![(0, 2), (3, 5), (6, 8)];
        validate_ranges_cover(9, &local).unwrap();
        let boundary = choose_adaptive_boundary(&local, 10, 12, 15, 18).unwrap();
        assert_eq!(boundary, 15);
        let mut output = Vec::new();
        append_core_ranges(&mut output, &local, 10, 12, boundary).unwrap();
        assert_eq!(output, vec![(12, 12), (13, 15)]);
    }

    #[test]
    fn translation_batch_requires_every_index_in_order() {
        let valid = vec![(0, "甲".into()), (1, "乙".into())];
        validate_translation_indexes(2, &valid).unwrap();
        let duplicate = vec![(0, "甲".into()), (0, "乙".into())];
        assert!(validate_translation_indexes(2, &duplicate).is_err());
    }

    #[test]
    fn cached_cues_require_valid_timeline_and_source() {
        let valid = vec![cue(0, "hello"), cue(1, "world")];
        validate_cached_cues(&valid).unwrap();

        let mut invalid_time = valid.clone();
        invalid_time[1].start = f64::NAN;
        assert!(validate_cached_cues(&invalid_time).is_err());

        let mut invalid_source = valid;
        invalid_source[0].source.clear();
        assert!(validate_cached_cues(&invalid_source).is_err());
    }

    #[test]
    fn partial_translation_checkpoint_resumes_only_missing_batches() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("translated.json");
        let source = vec![cue(0, "hello"), cue(1, "world"), cue(2, "again")];
        let mut partial = source.clone();
        partial[0].translation = Some("你好".into());
        partial[1].translation = Some("世界".into());
        subtitle::save_json(&partial, &path).unwrap();

        let loaded = load_translation_checkpoint(&path, &source)
            .unwrap()
            .unwrap();
        assert!(!translation_checkpoint_complete(&loaded));
        assert!(translation_batch_checkpointed(&loaded, 0, 2));
        assert!(!translation_batch_checkpointed(&loaded, 2, 3));

        let mut completed = loaded;
        completed[2].translation = Some("再来".into());
        subtitle::save_json(&completed, &path).unwrap();
        let loaded = load_translation_checkpoint(&path, &source)
            .unwrap()
            .unwrap();
        assert!(translation_checkpoint_complete(&loaded));
    }

    #[test]
    fn subtitle_pick_is_deterministic_and_prefers_exact_english() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path();
        // `--sub-langs "en.*,en"` 可能同时落盘多个变体，read_dir 顺序不可靠。
        for name in [
            "vid.en-US.vtt",
            "vid.en-orig.vtt",
            "vid.en.vtt",
            "vid.en-GB.vtt",
        ] {
            std::fs::write(work.join(name), "WEBVTT\n").unwrap();
        }
        // 空文件和非字幕文件不参与挑选。
        std::fs::write(work.join("vid.raw.mp4"), "x").unwrap();
        std::fs::write(work.join("vid.zz.vtt"), "").unwrap();
        assert_eq!(
            pick_subtitle_file(work, "vid").unwrap(),
            Some(work.join("vid.en.vtt"))
        );

        // 没有精确 en 时按语言标签字典序取第一个，重复调用结果一致。
        std::fs::remove_file(work.join("vid.en.vtt")).unwrap();
        let first = pick_subtitle_file(work, "vid").unwrap();
        assert_eq!(first, Some(work.join("vid.en-GB.vtt")));
        assert_eq!(pick_subtitle_file(work, "vid").unwrap(), first);

        std::fs::remove_dir_all(work.join("empty")).ok();
        assert_eq!(
            pick_subtitle_file(temp.path().join("nope").as_path(), "vid").ok(),
            None
        );
    }
}
