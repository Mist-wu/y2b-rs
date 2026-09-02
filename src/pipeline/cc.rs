//! B站中文 CC 字幕补交（软字幕，提交后走平台审核）。
use super::{Pipeline, subtitle_flow::load_segmented_cache};
use crate::bilibili_api::{self, CcCue};
use crate::db::{SUBTITLE_CLAIM_KIND, SubtitleAttempt, SubtitleAttemptDecision};
use crate::model::{Job, JobStatus, VideoMetadata};
use crate::subtitle::{self, Cue};
use anyhow::{Context, Result, bail};
use std::fs;
use thiserror::Error;

/// 投稿后到首次尝试补 CC 字幕的等待：B站稿件刚上传时查询 bvid 会返回 -404。
pub const CC_INITIAL_DELAY_SECONDS: i64 = 90;

/// CC 补交的最大自动检查次数，耗尽后留给 `y2b subtitle add/all` 手动处理。
///
/// 每次都会先检查 B站是否已生成中文字幕；配合下面的退避，`-404` 的覆盖窗口
/// 约 10 小时、其余失败约 11.5 小时。线上观察到 B站自动字幕曾在投稿约 8 小时
/// 后才出现，因此窗口必须留出余量。上游没有英文字幕轨的情况不走这个上限，
/// 见 `CC_MISSING_MATERIAL_MAX_ATTEMPTS`。
pub const CC_MAX_ATTEMPTS: i64 = 16;

/// 上游暂无英文字幕轨时的独立探测次数。
///
/// 线上 14 天数据：约一半新视频投稿时 YouTube ASR 字幕还没生成，但绝大多数
/// 在 30～90 分钟内出现；剩下几条（频道关闭自动字幕、非英语口播）按通用
/// 16 次退避探测了 12 小时以上依然没有，最后只留下一条「需手动补交」的异常
/// 任务。这类视频本身没有问题——原视频已经投稿，只是没有字幕可补——所以
/// 探测要稀疏，耗尽后按无字幕完成收尾，而不是留在待补状态报警。
pub const CC_MISSING_MATERIAL_MAX_ATTEMPTS: i64 = 8;

/// 第 n 次「无字幕轨」失败后的等待秒数：前期密集覆盖 ASR 常见延迟，后期
/// 拉长到 8 小时以覆盖直播回放次日才出字幕的情况；累计约 16 小时。
const CC_MISSING_MATERIAL_DELAYS_SECONDS: [i64; 7] = [300, 900, 1800, 3600, 7200, 14400, 28800];

/// 稿件仍在 B站处理中（-404）时的退避基数：第 n 次等待 `min(30 × 2^n, 1h)`。
///
/// 这里曾用固定 60 秒，理由是「-404 只是短暂状态」——实测是错的：B站审核加
/// 转码常见几十分钟到数小时，固定间隔会在 8 分钟内烧完全部重试次数，等稿件
/// 真正就绪时已经放弃了。改成递增后前几次仍然密集（1/3/7 分钟），快速过审的
/// 稿件能及时补上字幕，之后逐步拉长到 1 小时。
pub(super) const CC_NOT_READY_BASE_SECONDS: i64 = 30;

/// 其余失败的退避基数与上限：第 n 次失败后等待 `min(90 × 2^n, 1h)`。
pub(super) const CC_RETRY_BASE_SECONDS: i64 = 90;

pub(super) const CC_RETRY_CAP_SECONDS: i64 = 3600;

#[derive(Debug, Error)]
#[error("{detail}")]
struct CcSubmissionUncertainError {
    detail: String,
}

fn is_cc_submission_uncertain(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CcSubmissionUncertainError>().is_some()
}

fn is_explicit_cc_rejection(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().starts_with("提交字幕失败: code="))
}

fn uncertain_attempt_error(attempt: &SubtitleAttempt, detail: impl Into<String>) -> anyhow::Error {
    CcSubmissionUncertainError {
        detail: format!(
            "CC 字幕 attempt={}（bvid={}）状态为 {}，{}；只查询平台已有 zh 字幕，禁止再次 submit",
            attempt.id,
            attempt.bvid,
            attempt.status,
            detail.into()
        ),
    }
    .into()
}

/// CC 补交第 `attempt` 次失败后到下次可领取的秒数。
pub(super) fn cc_retry_delay_seconds(attempt: i64, video_not_ready: bool) -> i64 {
    let base = if video_not_ready {
        CC_NOT_READY_BASE_SECONDS
    } else {
        CC_RETRY_BASE_SECONDS
    };
    base.saturating_mul(1i64 << attempt.clamp(0, 16))
        .min(CC_RETRY_CAP_SECONDS)
}

/// 上游无字幕轨第 `attempt` 次失败后到下次探测的秒数。
pub(super) fn cc_missing_material_delay_seconds(attempt: i64) -> i64 {
    let index = usize::try_from(attempt.saturating_sub(1)).unwrap_or(0);
    CC_MISSING_MATERIAL_DELAYS_SECONDS[index.min(CC_MISSING_MATERIAL_DELAYS_SECONDS.len() - 1)]
}

/// 归类 CC 补交失败原因用的错误前缀（`is_missing_subtitle_material` 依赖它们）。
pub(super) const MISSING_SUBTITLE_MATERIAL_PREFIX: &str = "缺少翻译素材，跳过自动 CC 提交";

pub(super) const EMPTY_TRANSLATION_PREFIX: &str = "翻译结果为空";

/// B站字幕接口对单条 content 的双重上限。线上返回的零基 `line 501/507`
/// 分别对应 107/102 个中文字符（305/306 UTF-8 字节），其余不超过 100 字符的
/// 条目均通过，因此提交前同时约束字符数和字节数。
const BILIBILI_CC_MAX_CHARS: usize = 100;
const BILIBILI_CC_MAX_BYTES: usize = 300;

fn is_cc_split_boundary(character: char) -> bool {
    character.is_whitespace()
        || matches!(
            character,
            '。' | '！' | '？' | '；' | '，' | '、' | '.' | '!' | '?' | ';' | ','
        )
}

fn split_cc_content(content: &str) -> Vec<String> {
    let mut remaining = content.trim();
    let mut chunks = Vec::new();
    while !remaining.is_empty() {
        let mut chars = 0usize;
        let mut hard_end = 0usize;
        let mut preferred_end = None;
        for (start, character) in remaining.char_indices() {
            let end = start + character.len_utf8();
            if chars >= BILIBILI_CC_MAX_CHARS || end > BILIBILI_CC_MAX_BYTES {
                break;
            }
            chars += 1;
            hard_end = end;
            // 避免在很靠前的标点就切开；后半段存在自然边界时才优先使用。
            if chars >= BILIBILI_CC_MAX_CHARS / 2 && is_cc_split_boundary(character) {
                preferred_end = Some(end);
            }
        }
        if hard_end == remaining.len() {
            chunks.push(remaining.to_string());
            break;
        }
        let split_at = preferred_end.unwrap_or(hard_end);
        let (head, tail) = remaining.split_at(split_at);
        let head = head.trim();
        if !head.is_empty() {
            chunks.push(head.to_string());
        }
        remaining = tail.trim_start();
    }
    chunks
}

/// 把双语 cue 转成 B站 CC 字幕条目：清理传输层/音乐标记，只保留有非空翻译且
/// 时间合法（end > start）的。这里保留最后一道清理，旧翻译缓存也不会把脏字符
/// 再次提交到 B站。
pub(super) fn cc_cues_from(cues: &[Cue], max_to: Option<f64>) -> Vec<CcCue> {
    cues.iter()
        .filter_map(|c| {
            if c.end <= c.start {
                return None;
            }
            let content = subtitle::sanitize_caption_text(c.translation.as_deref()?);
            if content.is_empty() {
                return None;
            }
            let from = c.start;
            let mut to = c.end;
            if let Some(max) = max_to {
                if from >= max {
                    return None;
                }
                to = to.min(max);
            }
            if to <= from {
                return None;
            }
            Some((from, to, content))
        })
        .flat_map(|(from, to, content)| {
            let chunks = split_cc_content(&content);
            let chunk_count = chunks.len();
            let total_weight = chunks
                .iter()
                .map(|chunk| chunk.chars().count())
                .sum::<usize>()
                .max(1);
            let mut consumed_weight = 0usize;
            let mut chunk_from = from;
            let mut result = Vec::with_capacity(chunk_count);
            for (index, content) in chunks.into_iter().enumerate() {
                consumed_weight += content.chars().count();
                let chunk_to = if index + 1 == chunk_count {
                    to
                } else {
                    from + (to - from) * consumed_weight as f64 / total_weight as f64
                };
                result.push(CcCue {
                    from: chunk_from,
                    to: chunk_to,
                    content,
                });
                chunk_from = chunk_to;
            }
            result
        })
        .collect()
}

/// B 站稿件上传后尚未处理完成时，view 接口返回 code=-404（“啥都木有”）。
/// 该错误是瞬时的，应在稍后重试。
/// 稿件刚上传、B站还在处理：查询会短暂返回 -404，值得退避重试。
pub(super) fn is_bilibili_video_not_ready(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("code=-404"))
}

/// 本地当前没有可提交的中文字幕素材。它本身不会凭空恢复，但 B站可能在数小时
/// 后生成 `zh-CN` 自动字幕；补交 worker 每次都会先查平台字幕，因此仍应保留
/// 有上限的退避检查，而不是第一次看到缺素材就伪装成已经失败 12 次。
pub(super) fn is_missing_subtitle_material(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.starts_with(MISSING_SUBTITLE_MATERIAL_PREFIX)
            || message.starts_with(EMPTY_TRANSLATION_PREFIX)
    })
}

impl Pipeline {
    /// 给已投稿视频补中文 CC 字幕（B站软字幕，提交后走平台审核）。
    ///
    /// 素材优先复用下载目录里的翻译缓存；缺失时重新下载英文字幕、分句并调 Pi
    /// 翻译。返回给人看的说明文字。
    pub async fn backfill_cc_subtitle(&self, bvid: &str) -> Result<String> {
        let job = self
            .db
            .job_by_bvid(bvid)?
            .with_context(|| format!("数据库中找不到 bvid={bvid} 的任务"))?;
        let job = self
            .db
            .claim_subtitle_job_now(&job.id)?
            .with_context(|| format!("任务 {} 的字幕正在由其他执行者处理", job.id))?;
        let result = self
            .run_with_claim_heartbeat(
                &job.id,
                SUBTITLE_CLAIM_KIND,
                self.backfill_cc_subtitle_for_job(&job, true),
            )
            .await;
        if result.is_err() && self.db.owns_job_claim(&job.id, SUBTITLE_CLAIM_KIND)? {
            self.db.release_job_claim(&job.id, SUBTITLE_CLAIM_KIND)?;
        }
        result
    }

    /// CC 字幕队列的一次尝试：素材缺失时**重新下载**，而不是只查本地缓存。
    ///
    /// 由 `watch` 的字幕 worker 驱动，成功时 `backfill_cc_subtitle_for_job`
    /// 会把任务置为 `completed`；失败时退避重试，达到上限后再明确耗尽。
    ///
    /// 曾经传 `false`，只复用本地翻译缓存。但那个缓存文件只有在 `subtitle_download`
    /// 成功时才会生成——而真正会走到这条重试路径的任务，恰恰就是当初
    /// `subtitle_download` 返回 `missing` 的那些，缓存按定义不存在。于是全部
    /// 16 次重试都在找一个不可能出现的文件，线上两条任务分别空转了 15 次和 16 次。
    ///
    /// 真正的瞬时状态在上游：YouTube 的 ASR 字幕生成有延迟，直播回放尤其明显
    /// （线上那条 `post_live` 任务入队时无字幕轨，一天后 `en-orig`/`en` 都有了）。
    /// 这恰恰是最值得重试的情况，却是唯一重试不到的。
    ///
    /// 成本可控：上游仍然没有字幕时 `segment_uncached` 直接返回 None，只花一次
    /// 字幕探测（线上均值约 9 秒），不会触发分句和翻译；一旦拿到字幕则做一次
    /// 完整流程并写入缓存，后续重试复用缓存。
    pub async fn submit_pending_subtitle(&self, job: Job) -> Result<()> {
        if !self.db.owns_job_claim(&job.id, SUBTITLE_CLAIM_KIND)? {
            bail!("任务 {} 没有当前进程持有的字幕领取权", job.id)
        }
        let result = self
            .run_with_claim_heartbeat(
                &job.id,
                SUBTITLE_CLAIM_KIND,
                self.backfill_cc_subtitle_for_job(&job, true),
            )
            .await;
        match result {
            Ok(message) => {
                tracing::info!(job_id = %job.id, "{message}");
                Ok(())
            }
            Err(error) => {
                if !self.db.owns_job_claim(&job.id, SUBTITLE_CLAIM_KIND)? {
                    return Err(error);
                }
                let detail = format!("{error:#}");
                let attempt = job.subtitle_attempt + 1;
                let uncertain_submission = is_cc_submission_uncertain(&error);
                let explicit_rejection = is_explicit_cc_rejection(&error);
                if is_missing_subtitle_material(&error) {
                    return self
                        .defer_or_complete_without_material(&job, attempt, &detail, error)
                        .await;
                }
                if attempt >= CC_MAX_ATTEMPTS {
                    let exhausted = if uncertain_submission {
                        format!(
                            "CC 字幕投稿结果在 {CC_MAX_ATTEMPTS} 次只读核对后仍无法确认，已禁止重投并转人工: {detail}"
                        )
                    } else {
                        format!(
                            "CC 字幕自动重试耗尽（第 {attempt}/{CC_MAX_ATTEMPTS} 次）: {detail}"
                        )
                    };
                    self.db.exhaust_claimed_pending_subtitle(
                        &job.id,
                        CC_MAX_ATTEMPTS,
                        &exhausted,
                    )?;
                    self.db.event(Some(&job.id), "warn", &exhausted)?;
                    return Err(error);
                }
                let delay = cc_retry_delay_seconds(attempt, is_bilibili_video_not_ready(&error));
                let retry_detail = if uncertain_submission {
                    format!("CC 字幕投稿结果不确定，仅安排平台只读核对: {detail}")
                } else if explicit_rejection {
                    format!("CC 字幕被平台明确拒绝，允许稍后创建新 attempt: {detail}")
                } else {
                    format!("CC 字幕提交前检查失败: {detail}")
                };
                self.db
                    .defer_claimed_pending_subtitle(&job.id, &retry_detail, delay)?;
                tracing::warn!(
                    job_id = %job.id,
                    attempt,
                    delay,
                    uncertain_submission,
                    explicit_rejection,
                    error = %error,
                    "CC 字幕处理未完成，稍后继续"
                );
                self.db.event(
                    Some(&job.id),
                    "warn",
                    &format!("{retry_detail}（第 {attempt}/{CC_MAX_ATTEMPTS} 次，{delay} 秒后）"),
                )?;
                Err(error)
            }
        }
    }

    /// 上游暂无英文字幕轨：按稀疏计划继续探测；探测耗尽说明这条视频就是没有
    /// 字幕，原视频已经投稿，直接按无字幕完成，不留异常。之后若上游补了字幕，
    /// `y2b subtitle add` 仍可对已完成任务手动补交。
    async fn defer_or_complete_without_material(
        &self,
        job: &Job,
        attempt: i64,
        detail: &str,
        error: anyhow::Error,
    ) -> Result<()> {
        if attempt >= CC_MISSING_MATERIAL_MAX_ATTEMPTS {
            let bvid = job.bvid.as_deref().unwrap_or_default();
            let message = format!(
                "上游在 {attempt} 次检查后仍无英文字幕轨，原视频已投稿，按无字幕完成；若之后出现字幕可手动执行 y2b subtitle add {bvid}"
            );
            self.db.finish_subtitle_claim(&job.id, true)?;
            self.db.event(Some(&job.id), "info", &message)?;
            tracing::info!(job_id = %job.id, attempt, "{message}");
            return Ok(());
        }
        let delay = cc_missing_material_delay_seconds(attempt);
        let retry_detail = format!("等待上游英文字幕轨: {detail}");
        self.db
            .defer_claimed_pending_subtitle(&job.id, &retry_detail, delay)?;
        tracing::info!(
            job_id = %job.id,
            attempt,
            delay,
            "上游暂无英文字幕轨，稍后再探测"
        );
        self.db.event(
            Some(&job.id),
            "info",
            &format!(
                "{retry_detail}（第 {attempt}/{CC_MISSING_MATERIAL_MAX_ATTEMPTS} 次，{delay} 秒后）"
            ),
        )?;
        Err(error)
    }

    pub(super) async fn backfill_cc_subtitle_for_job(
        &self,
        job: &Job,
        redownload_if_missing: bool,
    ) -> Result<String> {
        let bvid = job.bvid.as_deref().unwrap_or_default();
        if bvid.is_empty() {
            bail!("任务 {} 没有 BVID", job.id)
        }
        let mut blocking_attempt = self.db.blocking_subtitle_attempt(&job.id)?;
        if let Some(attempt) = blocking_attempt
            .as_mut()
            .filter(|attempt| attempt.status == "started")
        {
            let detail = "检测到上一次未完成的 started attempt，提交是否到达平台无法确认";
            if let Err(error) =
                self.db
                    .mark_subtitle_attempt_uncertain(&job.id, &attempt.id, detail)
            {
                return Err(uncertain_attempt_error(
                    attempt,
                    format!("{detail}；写入 uncertain 失败: {error:#}"),
                ));
            }
            attempt.status = "uncertain".into();
        }
        let meta = self
            .db
            .source_metadata(&job.id)?
            .unwrap_or_else(|| VideoMetadata {
                // 个别早期任务未持久化来源元数据：用 job 自身的 video_id/url 兜底。
                id: job.video_id.clone(),
                url: job.url.clone(),
                title: String::new(),
                description: None,
                uploader: None,
                upload_date: None,
                channel: None,
                channel_id: None,
                timestamp: None,
                duration: None,
                width: None,
                height: None,
                fps: None,
                thumbnail_url: None,
                webpage_url: None,
                live_status: None,
                default_audio_language: None,
            });
        let client =
            bilibili_api::BiliSubtitleClient::from_cookies_file(&self.config.bilibili.cookies)?;
        let view = client.view(bvid).await?;
        if client.has_subtitle_lan(view.cid, "zh").await? {
            // `zh` 是投稿者提交的中文 CC；平台自动翻译使用 `zh-CN`，不能在这里
            // 当成已完成，否则待补任务会被静默跳过。
            let mark_completed = job.status == JobStatus::UploadedOriginalPendingSubtitle;
            if let Some(attempt) = blocking_attempt.as_ref().filter(|attempt| {
                attempt.bvid == bvid && matches!(attempt.status.as_str(), "started" | "uncertain")
            }) {
                self.db
                    .reconcile_subtitle_attempt(&job.id, &attempt.id, mark_completed)?;
            } else {
                self.db.finish_subtitle_claim(&job.id, mark_completed)?;
            }
            if mark_completed {
                self.db.event(
                    Some(&job.id),
                    "info",
                    "检测到已提交中文 CC 字幕（zh），补字幕任务完成",
                )?;
            }
            return Ok(format!("{bvid} 已有已提交中文 CC 字幕（zh），跳过"));
        }
        if let Some(attempt) = blocking_attempt {
            return Err(uncertain_attempt_error(&attempt, "平台当前未返回 zh 字幕"));
        }
        let work = self.config.runtime.download_dir.join(&meta.id);
        let translated = work.join(format!("{}.en-zh-CN.translated.json", meta.id));
        let cues = if let Ok(cached) = subtitle::load_json(&translated) {
            self.db.event(
                Some(&job.id),
                "info",
                &format!("复用翻译缓存: {} cues", cached.len()),
            )?;
            cached
        } else if redownload_if_missing {
            fs::create_dir_all(&work)?;
            let segmented = work.join(format!("{}.en.segmented.json", meta.id));
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
                    let Some(cues) = self.segment_uncached(job, &meta, &work, &segmented).await?
                    else {
                        // 必须复用 MISSING_SUBTITLE_MATERIAL_PREFIX：自动重试现在也会走到
                        // 这里，分类错了会让退避和「达到上限」的提示都变成不可操作的泛化
                        // 文案，看不出该手动补交。
                        bail!(
                            "{MISSING_SUBTITLE_MATERIAL_PREFIX}：上游暂无英文字幕轨（可手动执行 y2b subtitle add {bvid}）"
                        )
                    };
                    cues
                }
            };
            self.translate_and_save(&job.id, &mut cues, &translated)
                .await?;
            cues
        } else {
            bail!("{MISSING_SUBTITLE_MATERIAL_PREFIX}（可手动执行 y2b subtitle add {bvid}）")
        };
        let cc_cues = cc_cues_from(&cues, Some(view.duration));
        if cc_cues.is_empty() {
            bail!("{EMPTY_TRANSLATION_PREFIX}，没有可提交的中文字幕")
        }
        let attempt_id = match self.db.begin_subtitle_attempt(&job.id, bvid)? {
            SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
            SubtitleAttemptDecision::QueryOnly(attempt) => {
                return Err(uncertain_attempt_error(
                    &attempt,
                    "创建 attempt 时发现已有提交记录",
                ));
            }
        };
        if let Err(error) = client.submit(&view, "zh", &cc_cues).await {
            if is_explicit_cc_rejection(&error) {
                if let Err(mark_error) =
                    self.db
                        .reject_subtitle_attempt(&job.id, &attempt_id, &format!("{error:#}"))
                {
                    return Err(CcSubmissionUncertainError {
                        detail: format!(
                            "平台明确拒绝字幕 attempt={attempt_id}，但本地写入 rejected 失败: {mark_error:#}；原错误: {error:#}"
                        ),
                    }
                    .into());
                }
                return Err(error);
            }
            let detail =
                format!("字幕 attempt={attempt_id} 调用后响应丢失、超时或无法验证: {error:#}");
            let detail =
                match self
                    .db
                    .mark_subtitle_attempt_uncertain(&job.id, &attempt_id, &detail)
                {
                    Ok(()) => detail,
                    Err(mark_error) => format!("{detail}；写入 uncertain 失败: {mark_error:#}"),
                };
            return Err(CcSubmissionUncertainError { detail }.into());
        }
        // 外部明确成功后，attempt 与任务/领取终态必须一起提交；本地失败则固定为
        // uncertain，下一次只查询平台，审计事件失败也不能触发第二次 submit。
        let mark_completed = job.status == JobStatus::UploadedOriginalPendingSubtitle;
        if let Err(error) = self
            .db
            .finish_subtitle_attempt(&job.id, &attempt_id, mark_completed)
        {
            let detail =
                format!("Bilibili 已接受字幕 attempt={attempt_id}，但本地确认事务失败: {error:#}");
            let detail =
                match self
                    .db
                    .mark_subtitle_attempt_uncertain(&job.id, &attempt_id, &detail)
                {
                    Ok(()) => detail,
                    Err(mark_error) => format!("{detail}；写入 uncertain 失败: {mark_error:#}"),
                };
            return Err(CcSubmissionUncertainError { detail }.into());
        }
        self.db.event(
            Some(&job.id),
            "info",
            &format!("已提交中文 CC 字幕（{} 条），等待 B站审核", cc_cues.len()),
        )?;
        Ok(format!(
            "{bvid} 已提交中文 CC 字幕（{} 条），等待 B站审核",
            cc_cues.len()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cc_submission_errors_only_treat_platform_code_as_explicit_rejection() {
        let rejected =
            anyhow::anyhow!("提交字幕失败: code=1001 参数错误").context("Bilibili 字幕提交失败");
        assert!(is_explicit_cc_rejection(&rejected));
        assert!(!is_cc_submission_uncertain(&rejected));

        for message in [
            "request timed out",
            "连接在发送后断开",
            "提交字幕失败: 响应缺少 code",
        ] {
            let error = anyhow::anyhow!(message);
            assert!(!is_explicit_cc_rejection(&error), "{message}");
        }
        let uncertain: anyhow::Error = CcSubmissionUncertainError {
            detail: "响应丢失".into(),
        }
        .into();
        assert!(is_cc_submission_uncertain(&uncertain));
    }

    #[test]
    fn cc_failures_distinguish_missing_material_from_platform_not_ready() {
        // 素材缺失需要单独标记，达到上限时给出可操作的人工补交说明；期间仍会
        // 继续检查 B站是否已经生成平台自动字幕。
        for message in [
            "缺少翻译素材，跳过自动 CC 提交（可手动执行 y2b subtitle add BV1x）",
            // 自动重试重新下载后，上游仍无字幕轨时走的分支：必须仍然归到
            // 「素材缺失」，否则达到上限时的提示看不出需要人工补交。
            "缺少翻译素材，跳过自动 CC 提交：上游暂无英文字幕轨（可手动执行 y2b subtitle add BV1x）",
            "翻译结果为空，没有可提交的中文字幕",
        ] {
            let error = anyhow::anyhow!(message.to_string()).context("submit failed");
            assert!(is_missing_subtitle_material(&error), "{message}");
            assert!(!is_bilibili_video_not_ready(&error), "{message}");
        }
        // 稿件处理中和网络故障都该重试。
        let not_ready = anyhow::anyhow!("查询稿件 BV1x 失败: code=-404 啥都木有");
        assert!(!is_missing_subtitle_material(&not_ready));
        assert!(is_bilibili_video_not_ready(&not_ready));
        assert!(!is_missing_subtitle_material(&anyhow::anyhow!(
            "network timeout"
        )));
    }

    #[test]
    fn cc_retry_delay_backs_off_for_both_failure_kinds() {
        // -404：前几次密集，快速过审的稿件能及时补字幕。
        assert_eq!(cc_retry_delay_seconds(1, true), 60);
        assert_eq!(cc_retry_delay_seconds(2, true), 120);
        assert_eq!(cc_retry_delay_seconds(3, true), 240);
        // 之后逐步拉长并封顶 1 小时——固定 60 秒会在 8 分钟内烧完全部重试，
        // 而 B站审核转码常见几十分钟到数小时。
        assert_eq!(cc_retry_delay_seconds(7, true), 3600);
        assert_eq!(cc_retry_delay_seconds(64, true), 3600);
        // 其余失败按 90 × 2^n 退避，同样封顶 1 小时。
        assert_eq!(cc_retry_delay_seconds(1, false), 180);
        assert_eq!(cc_retry_delay_seconds(5, false), 2880);
        assert_eq!(cc_retry_delay_seconds(6, false), 3600);
    }

    #[test]
    fn cc_not_ready_window_covers_bilibili_review_time() {
        // 最后一次检查之前累计覆盖的时长必须超过线上观察到的约 8 小时，
        // 否则会在平台自动字幕真正出现之前就放弃。
        let total: i64 = (1..CC_MAX_ATTEMPTS)
            .map(|attempt| cc_retry_delay_seconds(attempt, true))
            .sum();
        assert!(
            total >= 10 * 3600,
            "-404 覆盖窗口只有 {total}s，不足 10 小时"
        );
    }

    #[test]
    fn missing_material_probes_are_sparse_but_cover_next_day_captions() {
        // 前期覆盖 ASR 常见的 30～90 分钟延迟，后期拉长到 8 小时。
        assert_eq!(cc_missing_material_delay_seconds(1), 300);
        assert_eq!(cc_missing_material_delay_seconds(2), 900);
        assert_eq!(cc_missing_material_delay_seconds(4), 3600);
        assert_eq!(cc_missing_material_delay_seconds(7), 28800);
        assert_eq!(cc_missing_material_delay_seconds(99), 28800);
        // 比通用 16 次少一半探测，但总覆盖仍超过直播回放次日出字幕所需的 12 小时。
        let total: i64 = (1..CC_MISSING_MATERIAL_MAX_ATTEMPTS)
            .map(cc_missing_material_delay_seconds)
            .sum();
        assert!(total >= 12 * 3600, "无字幕轨探测窗口只有 {total}s");
    }

    #[test]
    fn bilibili_not_ready_error_is_classified_for_cc_retry() {
        assert!(is_bilibili_video_not_ready(&anyhow::anyhow!(
            "查询稿件 BV1qQMm6HEi3 失败: code=-404 啥都木有"
        )));
        assert!(is_bilibili_video_not_ready(&anyhow::anyhow!(
            "sub process failed: 查询稿件 BV1bQMU63Eam 失败: code=-404"
        )));
        assert!(!is_bilibili_video_not_ready(&anyhow::anyhow!(
            "缺少翻译素材，跳过自动 CC 提交"
        )));
        assert!(!is_bilibili_video_not_ready(&anyhow::anyhow!(
            "Bilibili 认证失效"
        )));
    }

    #[test]
    fn cc_cues_keep_only_translated_in_order() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 1.5,
                source: "hi".into(),
                translation: Some(" 你好 ".into()),
            },
            Cue {
                start: 1.5,
                end: 3.0,
                source: "empty translation".into(),
                translation: Some("   ".into()),
            },
            Cue {
                start: 3.0,
                end: 2.0,
                source: "inverted".into(),
                translation: Some("倒序".into()),
            },
            Cue {
                start: 4.0,
                end: 5.0,
                source: "none".into(),
                translation: None,
            },
        ];
        let cc = cc_cues_from(&cues, None);
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0].content, "你好");
        assert_eq!(cc[0].from, 0.0);
        assert_eq!(cc[0].to, 1.5);
    }

    #[test]
    fn cc_cues_clean_old_cached_entities_and_music_labels() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 2.0,
                source: "source".into(),
                translation: Some("&gt;&gt; 看起来不错。[音乐] 准备好了吗？".into()),
            },
            Cue {
                start: 2.0,
                end: 3.0,
                source: "music only".into(),
                translation: Some("【音乐】♪".into()),
            },
        ];

        let cc = cc_cues_from(&cues, None);
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0].content, "看起来不错。 准备好了吗？");
    }

    #[test]
    fn cc_cues_split_platform_oversize_content_without_truncation() {
        for original in ["中".repeat(107), "🧪".repeat(100)] {
            let cues = vec![Cue {
                start: 10.0,
                end: 20.0,
                source: "source".into(),
                translation: Some(original.clone()),
            }];
            let cc = cc_cues_from(&cues, None);
            assert!(cc.len() >= 2);
            assert_eq!(cc.first().unwrap().from, 10.0);
            assert_eq!(cc.last().unwrap().to, 20.0);
            assert_eq!(
                cc.iter()
                    .map(|cue| cue.content.as_str())
                    .collect::<String>(),
                original
            );
            for (index, cue) in cc.iter().enumerate() {
                assert!(cue.content.chars().count() <= BILIBILI_CC_MAX_CHARS);
                assert!(cue.content.len() <= BILIBILI_CC_MAX_BYTES);
                assert!(cue.to > cue.from);
                if let Some(next) = cc.get(index + 1) {
                    assert_eq!(cue.to, next.from);
                }
            }
        }
    }

    #[test]
    fn cc_cues_clip_beyond_video_duration() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 5.0,
                source: "a".into(),
                translation: Some("正常".into()),
            },
            Cue {
                start: 4.0,
                end: 9.0,
                source: "b".into(),
                translation: Some("跨越结尾".into()),
            },
            Cue {
                start: 7.0,
                end: 8.0,
                source: "c".into(),
                translation: Some("整体超出".into()),
            },
        ];
        let cc = cc_cues_from(&cues, Some(6.5));
        assert_eq!(cc.len(), 2);
        assert_eq!(cc[0].to, 5.0);
        assert_eq!(cc[1].to, 6.5);
    }
}
