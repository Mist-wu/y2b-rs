use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

pub const AI_PROVIDER: &str = "deepseek";
pub const AI_MODEL: &str = "deepseek-v4-flash";
pub const AI_TRANSLATION_MODEL: &str = "deepseek-v4-pro";
pub const AI_THINKING: &str = "off";
const MAX_TRANSLATION_BATCH_RETRIES: usize = 10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub runtime: RuntimeConfig,
    pub monitor: MonitorConfig,
    pub youtube: YoutubeConfig,
    pub bilibili: BilibiliConfig,
    pub ai: AiConfig,
    pub render: RenderConfig,
    pub storage: StorageConfig,
    pub translation: TranslationConfig,
    pub websub: WebSubConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub data_dir: PathBuf,
    pub database: PathBuf,
    pub download_dir: PathBuf,
    pub backup_dir: PathBuf,
    pub timezone: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MonitorConfig {
    pub poll_seconds: u64,
    /// playlistItems.list 单次读取条数。该接口按调用计费，1 与 50 同为 1 单位。
    pub data_api_max_results: usize,
    /// 历史发布时间预测热窗的总宽度。
    pub prediction_window_minutes: u64,
    /// 预测热窗内的 Data API 轮询间隔。
    pub prediction_hot_poll_seconds: u64,
    /// 预测热窗外的 Data API 轮询间隔。
    pub prediction_cold_poll_minutes: u64,
    /// 历史样本不足时使用的固定 Data API 轮询间隔。
    pub prediction_fallback_poll_minutes: u64,
    /// 启用发布时间预测所需的最少 jobs.published_at 样本数。
    pub prediction_min_samples: usize,
    /// API 深扫周期；API 不可用时，同一周期才会触发 yt-dlp 校对兜底。
    pub reconcile_hours: u64,
    pub reconcile_limit: usize,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct YoutubeConfig {
    pub yt_dlp: String,
    pub cookies: PathBuf,
    pub probe_url: String,
    pub max_pixels: u64,
    pub max_fps: f64,
    /// 超过该时长的视频直接跳过，不入队。
    ///
    /// 主要针对直播回放（常见 1–4 小时）：下载会撞上 `video_download` 的 7200s
    /// 超时，`translated` 模式下的分句/翻译 token 成本也比普通视频高一个数量级。
    /// 设为 0 表示不限制。
    pub max_duration_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BilibiliConfig {
    pub biliup: String,
    pub cookies: PathBuf,
    pub submit_interval_seconds: u64,
    pub rate_limit_cooldown_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AiConfig {
    pub pi: String,
    pub extension: PathBuf,
    pub policy: PathBuf,
    pub provider: String,
    pub model: String,
    /// 长列表翻译需要更强的逐条对齐能力，与分句/元数据模型分开固定。
    pub translation_model: String,
    /// 所有 AI 阶段共用同一个思考级别，避免任务间配置漂移。
    pub thinking: String,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RenderConfig {
    pub ffmpeg: String,
    pub ffprobe: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub warn_free_gib: u64,
    pub stop_free_gib: u64,
    pub delete_large_after_upload: bool,
    pub daily_backups: usize,
    pub weekly_backups: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TranslationConfig {
    pub source_lang: String,
    pub target_lang: String,
    /// 已知的 defaultAudioLanguage 不匹配时是否硬拦截；缺失值始终放行。
    pub enforce_source_lang: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSubConfig {
    /// 默认关闭；关闭时不会绑定任何端口或发起订阅。
    pub enabled: bool,
    /// 本地 HTTP 监听地址，通常由公网 HTTPS 反向代理转发到这里。
    pub bind_addr: String,
    /// 公网 HTTPS 根地址，例如 https://push.example.com。
    pub callback_base_url: String,
    /// WebSub 启用后 Data API 的兜底轮询周期。
    pub data_api_poll_minutes: u64,
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
            data_api_max_results: 50,
            prediction_window_minutes: 120,
            prediction_hot_poll_seconds: 60,
            prediction_cold_poll_minutes: 30,
            prediction_fallback_poll_minutes: 5,
            prediction_min_samples: 5,
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
            max_duration_seconds: 7200,
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
            provider: AI_PROVIDER.into(),
            model: AI_MODEL.into(),
            translation_model: AI_TRANSLATION_MODEL.into(),
            thinking: AI_THINKING.into(),
            timeout_seconds: 900,
            batch_mode: BatchMode::Adaptive,
            context_window_tokens: 256_000,
            safe_context_tokens: 200_000,
            segment_overlap_cues: 12,
            segment_max_cues: 400,
            translation_batch_cues: 40,
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
            enforce_source_lang: false,
        }
    }
}
impl Default for WebSubConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: "127.0.0.1:8787".into(),
            callback_base_url: String::new(),
            data_api_poll_minutes: 30,
        }
    }
}
impl Config {
    /// 严格读取配置，文件不存在也返回错误。
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_inner(path, false)
    }

    /// 读取配置；仅当文件不存在时使用默认值，其他读取错误仍返回错误。
    pub fn load_or_default(path: &Path) -> Result<Self> {
        Self::load_inner(path, true)
    }

    fn load_inner(path: &Path, allow_missing: bool) -> Result<Self> {
        let config = match fs::read_to_string(path) {
            Ok(raw) => toml::from_str(&raw)
                .map_err(|error| anyhow::anyhow!("配置格式无效: {}: {error}", path.display()))?,
            Err(error) if allow_missing && error.kind() == std::io::ErrorKind::NotFound => {
                Self::default()
            }
            Err(error) => {
                return Err(error).with_context(|| format!("读取配置失败: {}", path.display()));
            }
        };
        config.validate()?;
        Ok(config)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
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

    pub fn validate(&self) -> Result<()> {
        self.validate_ai_profile()?;
        self.validate_discovery()?;
        self.validate_websub()?;
        anyhow::ensure!(
            self.ai.segment_max_cues > 0,
            "ai.segment_max_cues 必须大于 0"
        );
        anyhow::ensure!(
            self.ai.segment_overlap_cues < self.ai.segment_max_cues,
            "ai.segment_overlap_cues 必须小于 ai.segment_max_cues"
        );
        anyhow::ensure!(
            self.ai.context_window_tokens > 0,
            "ai.context_window_tokens 必须大于 0"
        );
        anyhow::ensure!(
            self.ai.safe_context_tokens > 0,
            "ai.safe_context_tokens 必须大于 0"
        );
        anyhow::ensure!(
            self.ai.safe_context_tokens <= self.ai.context_window_tokens,
            "ai.safe_context_tokens 必须小于等于 ai.context_window_tokens"
        );
        if let Some(limit) = self.ai.daily_token_limit {
            anyhow::ensure!(limit > 0, "ai.daily_token_limit 必须大于 0");
        }
        anyhow::ensure!(
            self.ai.translation_batch_cues > 0,
            "ai.translation_batch_cues 必须大于 0"
        );
        anyhow::ensure!(
            self.ai.translation_concurrency > 0,
            "ai.translation_concurrency 必须大于 0"
        );
        anyhow::ensure!(self.ai.timeout_seconds > 0, "ai.timeout_seconds 必须大于 0");
        anyhow::ensure!(
            self.ai.translation_batch_retries <= MAX_TRANSLATION_BATCH_RETRIES,
            "ai.translation_batch_retries 不能大于 {MAX_TRANSLATION_BATCH_RETRIES}"
        );
        anyhow::ensure!(
            self.monitor.max_attempts > 0,
            "monitor.max_attempts 必须大于 0"
        );
        anyhow::ensure!(
            self.storage.warn_free_gib >= self.storage.stop_free_gib,
            "storage.warn_free_gib 必须大于等于 storage.stop_free_gib"
        );
        anyhow::ensure!(
            self.youtube.max_fps.is_finite() && self.youtube.max_fps > 0.0,
            "youtube.max_fps 必须是有限且大于 0 的数"
        );
        anyhow::ensure!(self.youtube.max_pixels > 0, "youtube.max_pixels 必须大于 0");
        anyhow::ensure!(
            self.bilibili.submit_interval_seconds > 0,
            "bilibili.submit_interval_seconds 必须大于 0"
        );
        anyhow::ensure!(
            !self.translation.target_lang.trim().is_empty(),
            "translation.target_lang 不能为空"
        );
        anyhow::ensure!(
            self.translation.source_lang == "en" && self.translation.target_lang == "zh-CN",
            "翻译语言必须固定为 source_lang=en, target_lang=zh-CN；当前为 source_lang={}, target_lang={}",
            self.translation.source_lang,
            self.translation.target_lang
        );
        Ok(())
    }

    pub fn validate_ai_profile(&self) -> Result<()> {
        anyhow::ensure!(
            self.ai.provider == AI_PROVIDER
                && self.ai.model == AI_MODEL
                && self.ai.translation_model == AI_TRANSLATION_MODEL
                && self.ai.thinking == AI_THINKING,
            "AI 配置必须固定为 provider={AI_PROVIDER}, model={AI_MODEL}, translation_model={AI_TRANSLATION_MODEL}, thinking={AI_THINKING}；当前为 provider={}, model={}, translation_model={}, thinking={}",
            self.ai.provider,
            self.ai.model,
            self.ai.translation_model,
            self.ai.thinking
        );
        Ok(())
    }

    pub fn validate_websub(&self) -> Result<()> {
        self.websub
            .bind_addr
            .parse::<SocketAddr>()
            .with_context(|| format!("websub.bind_addr 无法解析: {}", self.websub.bind_addr))?;
        if self.websub.enabled {
            anyhow::ensure!(
                self.websub.callback_base_url.starts_with("https://"),
                "WebSub 启用时 callback_base_url 必须是公网 HTTPS 地址"
            );
            anyhow::ensure!(
                self.websub.data_api_poll_minutes > 0,
                "WebSub 启用时 data_api_poll_minutes 必须大于 0"
            );
        }
        Ok(())
    }

    pub fn validate_discovery(&self) -> Result<()> {
        self.runtime
            .timezone
            .parse::<chrono_tz::Tz>()
            .with_context(|| {
                format!(
                    "runtime.timezone 不是有效 IANA 时区: {}",
                    self.runtime.timezone
                )
            })?;
        anyhow::ensure!(
            (1..=50).contains(&self.monitor.data_api_max_results),
            "monitor.data_api_max_results 必须在 1..=50"
        );
        anyhow::ensure!(
            (1..=24 * 60).contains(&self.monitor.prediction_window_minutes),
            "monitor.prediction_window_minutes 必须在 1..=1440"
        );
        anyhow::ensure!(
            self.monitor.prediction_hot_poll_seconds > 0,
            "monitor.prediction_hot_poll_seconds 必须大于 0"
        );
        anyhow::ensure!(
            self.monitor.prediction_cold_poll_minutes > 0,
            "monitor.prediction_cold_poll_minutes 必须大于 0"
        );
        anyhow::ensure!(
            self.monitor.prediction_fallback_poll_minutes > 0,
            "monitor.prediction_fallback_poll_minutes 必须大于 0"
        );
        anyhow::ensure!(
            self.monitor.prediction_min_samples > 0,
            "monitor.prediction_min_samples 必须大于 0"
        );
        anyhow::ensure!(
            self.monitor.reconcile_hours > 0,
            "monitor.reconcile_hours 必须大于 0"
        );
        anyhow::ensure!(
            self.monitor.reconcile_limit > 0,
            "monitor.reconcile_limit 必须大于 0"
        );
        anyhow::ensure!(
            !self.translation.source_lang.trim().is_empty(),
            "translation.source_lang 不能为空"
        );
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
        assert_eq!(adaptive.translation_batch_cues, 40);
        assert_eq!(adaptive.translation_concurrency, 4);
        assert_eq!(adaptive.translation_batch_retries, 2);
        assert_eq!(adaptive.provider, AI_PROVIDER);
        assert_eq!(adaptive.model, AI_MODEL);
        assert_eq!(adaptive.translation_model, AI_TRANSLATION_MODEL);
        assert_eq!(adaptive.thinking, AI_THINKING);

        let legacy: AiConfig = toml::from_str("batch_size = 25").unwrap();
        assert_eq!(legacy.translation_batch_cues, 25);

        let whole: AiConfig = toml::from_str("batch_mode = \"whole_video\"").unwrap();
        assert_eq!(whole.batch_mode, BatchMode::WholeVideo);
    }

    #[test]
    fn strict_and_defaulting_loaders_handle_missing_files_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.toml");

        let error = Config::load(&path).unwrap_err().to_string();
        assert!(error.contains("读取配置失败"));
        assert!(error.contains("missing.toml"));
        assert_eq!(Config::load_or_default(&path).unwrap(), Config::default());
    }

    #[test]
    fn strict_load_is_independent_of_init_in_process_argv() {
        const PROBE_ENV: &str = "Y2B_TEST_STRICT_CONFIG_LOAD_ARGV_PROBE";
        if std::env::var_os(PROBE_ENV).is_some() {
            assert_eq!(std::env::args().nth(1).as_deref(), Some("init"));
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("missing.toml");
            let error = Config::load(&path).unwrap_err().to_string();
            assert!(error.contains("读取配置失败"));
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("init")
            .arg("--test-threads=1")
            .env(PROBE_ENV, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "argv 回归子进程失败：\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn both_loaders_reject_malformed_unknown_and_non_file_configs() {
        type Loader = fn(&Path) -> Result<Config>;
        let loaders: [(&str, Loader); 2] = [
            ("load", Config::load),
            ("load_or_default", Config::load_or_default),
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        for (raw, expected) in [
            ("[youtube\nmax_fps = 60", "配置格式无效"),
            ("[youtube]\nmax_fpss = 60", "max_fpss"),
        ] {
            fs::write(&path, raw).unwrap();
            for (name, loader) in loaders {
                let error = loader(&path).unwrap_err().to_string();
                assert!(
                    error.contains(expected),
                    "{name} 未拒绝配置或错误不明确: {error}"
                );
            }
        }

        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        for (name, loader) in loaders {
            let error = loader(&path).unwrap_err().to_string();
            assert!(
                error.contains("读取配置失败"),
                "{name} 错误地忽略了非 NotFound IO 错误: {error}"
            );
        }
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_config_level() {
        for (field, raw) in [
            ("root_typo", "root_typo = true"),
            ("data_dr", "[runtime]\ndata_dr = '/tmp'"),
            ("poll_second", "[monitor]\npoll_second = 1"),
            ("max_pixel", "[youtube]\nmax_pixel = 1"),
            (
                "submit_interval_second",
                "[bilibili]\nsubmit_interval_second = 1",
            ),
            ("timeout_second", "[ai]\ntimeout_second = 1"),
            ("ffmepg", "[render]\nffmepg = 'ffmpeg'"),
            ("warn_free_gibs", "[storage]\nwarn_free_gibs = 1"),
            ("target_lnag", "[translation]\ntarget_lnag = 'zh-CN'"),
            ("bind_adrr", "[websub]\nbind_adrr = '127.0.0.1:1'"),
        ] {
            let error = toml::from_str::<Config>(raw).unwrap_err().to_string();
            assert!(error.contains(field), "错误未指出字段 {field}: {error}");
        }
    }

    fn assert_invalid(config: &Config, field: &str) {
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains(field), "错误未指出字段 {field}: {error}");
    }

    #[test]
    fn rejects_invalid_config_boundaries() {
        let mut config = Config::default();
        config.ai.segment_max_cues = 0;
        assert_invalid(&config, "segment_max_cues");

        let mut config = Config::default();
        config.ai.segment_overlap_cues = config.ai.segment_max_cues;
        assert_invalid(&config, "segment_overlap_cues");

        let mut config = Config::default();
        config.ai.translation_batch_cues = 0;
        assert_invalid(&config, "translation_batch_cues");

        let mut config = Config::default();
        config.ai.translation_concurrency = 0;
        assert_invalid(&config, "translation_concurrency");

        let mut config = Config::default();
        config.ai.timeout_seconds = 0;
        assert_invalid(&config, "timeout_seconds");

        let mut config = Config::default();
        config.ai.translation_batch_retries = MAX_TRANSLATION_BATCH_RETRIES + 1;
        assert_invalid(&config, "translation_batch_retries");

        let mut config = Config::default();
        config.monitor.max_attempts = 0;
        assert_invalid(&config, "max_attempts");

        let mut config = Config::default();
        config.storage.warn_free_gib = config.storage.stop_free_gib - 1;
        assert_invalid(&config, "warn_free_gib");

        for max_fps in [0.0, f64::INFINITY, f64::NAN] {
            let mut config = Config::default();
            config.youtube.max_fps = max_fps;
            assert_invalid(&config, "max_fps");
        }

        let mut config = Config::default();
        config.youtube.max_pixels = 0;
        assert_invalid(&config, "max_pixels");

        let mut config = Config::default();
        config.bilibili.submit_interval_seconds = 0;
        assert_invalid(&config, "submit_interval_seconds");

        let mut config = Config::default();
        config.translation.target_lang.clear();
        assert_invalid(&config, "target_lang");

        let mut config = Config::default();
        config.translation.target_lang = "ja".into();
        assert_invalid(&config, "target_lang");

        let mut config = Config::default();
        config.translation.source_lang = "ja".into();
        assert_invalid(&config, "source_lang");

        let mut config = Config::default();
        config.websub.bind_addr = "not an address".into();
        assert_invalid(&config, "bind_addr");
    }

    #[test]
    fn rejects_non_positive_token_limits_and_bad_context_bounds() {
        for limit in [Some(0), Some(-1)] {
            let mut config = Config::default();
            config.ai.daily_token_limit = limit;
            assert_invalid(&config, "daily_token_limit");
        }

        let mut config = Config::default();
        config.ai.context_window_tokens = 0;
        assert_invalid(&config, "context_window_tokens");

        let mut config = Config::default();
        config.ai.safe_context_tokens = 0;
        assert_invalid(&config, "safe_context_tokens");

        let mut config = Config::default();
        config.ai.safe_context_tokens = config.ai.context_window_tokens + 1;
        assert_invalid(&config, "safe_context_tokens");
    }

    #[test]
    fn accepts_positive_token_limit_and_equal_context_bounds() {
        let mut config = Config::default();
        config.ai.daily_token_limit = Some(1);
        config.ai.safe_context_tokens = config.ai.context_window_tokens;
        config.validate().unwrap();
    }

    #[test]
    fn accepts_valid_config_boundaries() {
        let mut config = Config::default();
        config.ai.segment_max_cues = 1;
        config.ai.segment_overlap_cues = 0;
        config.ai.translation_batch_cues = 1;
        config.ai.translation_concurrency = 1;
        config.ai.timeout_seconds = 1;
        config.ai.translation_batch_retries = MAX_TRANSLATION_BATCH_RETRIES;
        config.monitor.max_attempts = 1;
        config.storage.warn_free_gib = config.storage.stop_free_gib;
        config.youtube.max_fps = f64::MIN_POSITIVE;
        config.youtube.max_pixels = 1;
        config.bilibili.submit_interval_seconds = 1;
        config.websub.bind_addr = "[::1]:1".into();
        config.validate().unwrap();
    }

    #[test]
    fn example_matches_rust_defaults() {
        let config: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn rejects_ai_profile_drift() {
        for (provider, model, translation_model, thinking) in [
            ("openai-codex", AI_MODEL, AI_TRANSLATION_MODEL, AI_THINKING),
            (
                AI_PROVIDER,
                "deepseek-v4-pro",
                AI_TRANSLATION_MODEL,
                AI_THINKING,
            ),
            (AI_PROVIDER, AI_MODEL, "deepseek-v4-flash", AI_THINKING),
            (AI_PROVIDER, AI_MODEL, AI_TRANSLATION_MODEL, "high"),
        ] {
            let mut config = Config::default();
            config.ai.provider = provider.into();
            config.ai.model = model.into();
            config.ai.translation_model = translation_model.into();
            config.ai.thinking = thinking.into();
            assert!(config.validate_ai_profile().is_err());
        }
    }

    #[test]
    fn websub_is_disabled_by_default_and_requires_https_when_enabled() {
        let mut config = Config::default();
        assert!(!config.websub.enabled);
        config.websub.enabled = true;
        assert!(config.validate_websub().is_err());
        config.websub.callback_base_url = "https://push.example.com".into();
        config.validate_websub().unwrap();
    }

    #[test]
    fn discovery_defaults_are_low_latency_and_validated() {
        let mut config = Config::default();
        assert_eq!(config.monitor.data_api_max_results, 50);
        assert_eq!(config.monitor.prediction_window_minutes, 120);
        assert_eq!(config.monitor.prediction_hot_poll_seconds, 60);
        assert_eq!(config.monitor.prediction_cold_poll_minutes, 30);
        assert_eq!(config.monitor.prediction_fallback_poll_minutes, 5);
        assert_eq!(config.monitor.prediction_min_samples, 5);
        assert_eq!(config.monitor.reconcile_hours, 6);
        assert!(!config.translation.enforce_source_lang);
        config.validate_discovery().unwrap();

        config.monitor.data_api_max_results = 51;
        assert!(config.validate_discovery().is_err());
    }
}
