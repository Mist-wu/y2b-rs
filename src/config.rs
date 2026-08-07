use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub runtime: RuntimeConfig,
    pub monitor: MonitorConfig,
    pub youtube: YoutubeConfig,
    pub bilibili: BilibiliConfig,
    pub ai: AiConfig,
    pub render: RenderConfig,
    pub storage: StorageConfig,
    pub translation: TranslationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub data_dir: PathBuf,
    pub database: PathBuf,
    pub download_dir: PathBuf,
    pub backup_dir: PathBuf,
    pub timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MonitorConfig {
    pub poll_seconds: u64,
    pub reconcile_hours: u64,
    pub reconcile_limit: usize,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct YoutubeConfig {
    pub yt_dlp: String,
    pub cookies: PathBuf,
    pub probe_url: String,
    pub max_pixels: u64,
    pub max_fps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BilibiliConfig {
    pub biliup: String,
    pub cookies: PathBuf,
    pub submit_interval_seconds: u64,
    pub rate_limit_cooldown_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiConfig {
    pub pi: String,
    pub extension: PathBuf,
    pub policy: PathBuf,
    pub provider: String,
    pub model: String,
    /// publish_metadata 等质量敏感任务的思考级别。
    pub thinking: String,
    /// 分句/翻译这类确定性转换任务的思考级别。实测 deepseek 下 `off` 的
    /// 流式输出约 1.5MB/批，`low` 涨到 14MB，`medium` 177MB——
    /// 大 thinking 会撑爆 2GB 服务器内存，必须用 `off`。
    pub translation_thinking: String,
    pub allowed_models: Vec<ModelConfig>,
    pub timeout_seconds: u64,
    pub batch_mode: BatchMode,
    pub context_window_tokens: usize,
    pub safe_context_tokens: usize,
    pub segment_overlap_cues: usize,
    /// 单次分句调用的最大 cue 数，防止超大调用超时/失败重来代价大。
    pub segment_max_cues: usize,
    #[serde(alias = "batch_size")]
    pub translation_batch_cues: usize,
    pub translation_concurrency: usize,
    pub translation_batch_retries: usize,
    pub daily_token_limit: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BatchMode {
    WholeVideo,
    #[default]
    Adaptive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub provider: String,
    pub model: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RenderConfig {
    pub ffmpeg: String,
    pub ffprobe: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub warn_free_gib: u64,
    pub stop_free_gib: u64,
    pub delete_large_after_upload: bool,
    pub daily_backups: usize,
    pub weekly_backups: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TranslationConfig {
    pub source_lang: String,
    pub target_lang: String,
}

fn base_dir() -> PathBuf {
    std::env::var_os("Y2B_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/lib/y2b"))
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let base = base_dir();
        Self {
            database: base.join("state.db"),
            download_dir: base.join("downloads"),
            backup_dir: base.join("backups"),
            data_dir: base,
            timezone: "Asia/Shanghai".into(),
        }
    }
}
impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            poll_seconds: 60,
            reconcile_hours: 6,
            reconcile_limit: 30,
            max_attempts: 5,
        }
    }
}
impl Default for YoutubeConfig {
    fn default() -> Self {
        let b = base_dir();
        Self {
            yt_dlp: "/usr/local/bin/yt-dlp".into(),
            cookies: b.join("youtube_cookies.txt"),
            probe_url: "https://www.youtube.com/watch?v=jNQXAC9IVRw".into(),
            max_pixels: 2_073_600,
            max_fps: 60.0,
        }
    }
}
impl Default for BilibiliConfig {
    fn default() -> Self {
        let b = base_dir();
        Self {
            biliup: "/usr/local/bin/biliup".into(),
            cookies: b.join("bilibili_cookies.json"),
            submit_interval_seconds: 1800,
            rate_limit_cooldown_seconds: 21600,
        }
    }
}
impl Default for AiConfig {
    fn default() -> Self {
        Self {
            pi: "/usr/local/bin/pi".into(),
            extension: "/opt/y2b/pi/y2b-extension.ts".into(),
            policy: "/opt/y2b/pi/policy.json".into(),
            provider: "openai-codex".into(),
            model: "gpt-5.6-luna".into(),
            thinking: "high".into(),
            translation_thinking: "off".into(),
            allowed_models: vec![
                ModelConfig {
                    provider: "openai-codex".into(),
                    model: "gpt-5.6-luna".into(),
                    label: "GPT-5.6 Luna".into(),
                },
                ModelConfig {
                    provider: "openai-codex".into(),
                    model: "gpt-5.6-sol".into(),
                    label: "GPT-5.6 Sol".into(),
                },
                ModelConfig {
                    provider: "openai-codex".into(),
                    model: "gpt-5.6-terra".into(),
                    label: "GPT-5.6 Terra".into(),
                },
            ],
            timeout_seconds: 300,
            batch_mode: BatchMode::Adaptive,
            context_window_tokens: 256_000,
            safe_context_tokens: 200_000,
            segment_overlap_cues: 12,
            segment_max_cues: 400,
            translation_batch_cues: 50,
            translation_concurrency: 4,
            translation_batch_retries: 2,
            daily_token_limit: None,
        }
    }
}
impl Default for RenderConfig {
    fn default() -> Self {
        Self {
            ffmpeg: "/usr/local/bin/ffmpeg".into(),
            ffprobe: "/usr/local/bin/ffprobe".into(),
        }
    }
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            warn_free_gib: 10,
            stop_free_gib: 5,
            delete_large_after_upload: true,
            daily_backups: 7,
            weekly_backups: 4,
        }
    }
}
impl Default for TranslationConfig {
    fn default() -> Self {
        Self {
            source_lang: "en".into(),
            target_lang: "zh-CN".into(),
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(path)
            .with_context(|| format!("读取配置失败: {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("配置格式无效: {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, toml::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        for p in [
            &self.runtime.data_dir,
            &self.runtime.download_dir,
            &self.runtime.backup_dir,
        ] {
            fs::create_dir_all(p)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_batch_modes_and_256k_defaults() {
        let adaptive: AiConfig = toml::from_str("batch_mode = \"adaptive\"").unwrap();
        assert_eq!(adaptive.batch_mode, BatchMode::Adaptive);
        assert_eq!(adaptive.context_window_tokens, 256_000);
        assert_eq!(adaptive.safe_context_tokens, 200_000);
        assert_eq!(adaptive.translation_batch_cues, 50);
        assert_eq!(adaptive.translation_concurrency, 4);
        assert_eq!(adaptive.translation_batch_retries, 2);

        let legacy: AiConfig = toml::from_str("batch_size = 25").unwrap();
        assert_eq!(legacy.translation_batch_cues, 25);

        let whole: AiConfig = toml::from_str("batch_mode = \"whole_video\"").unwrap();
        assert_eq!(whole.batch_mode, BatchMode::WholeVideo);
    }
}
