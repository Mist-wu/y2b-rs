//! B站创作中心 CC 字幕客户端：给已投稿视频查询/提交软字幕。
//!
//! 实测接口（2026-08，api.bilibili.com）：
//! - GET  `/x/web-interface/view?bvid=`          稿件信息（aid/cid）
//! - GET  `/x/v2/dm/subtitle/lan/search/cid`     查询视频已有字幕语言
//! - POST `/x/v2/dm/subtitle/draft/save`         提交字幕（type=1, oid=cid, aid, lan, data, submit, csrf）
//! - POST `/x/v2/dm/subtitle/del`                删除字幕（oid, subtitle_id, csrf）
//!
//! 提交的 `data` 是 JSON 字符串：{"body":[{"from":秒,"to":秒,"content":"文本","location":1|2}]}
//! location: 1=底部, 2=顶部。语言用短码（zh/en/ja…）。
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

const VIEW_API: &str = "https://api.bilibili.com/x/web-interface/view";
const LAN_API: &str = "https://api.bilibili.com/x/v2/dm/subtitle/lan/search/cid";
const SUBMIT_API: &str = "https://api.bilibili.com/x/v2/dm/subtitle/draft/save";

#[derive(Debug, Clone)]
pub struct VideoView {
    pub aid: i64,
    pub cid: i64,
    pub title: String,
    /// 视频时长（秒），用于把字幕时间裁剪到视频长度内。
    pub duration: f64,
}

/// 一条可提交的 CC 字幕 cue（转成 {"body":[...]} 后提交）。
#[derive(Debug, Clone)]
pub struct CcCue {
    pub from: f64,
    pub to: f64,
    pub content: String,
}

pub struct BiliSubtitleClient {
    http: reqwest::Client,
    cookie_header: String,
    csrf: String,
}

#[derive(Deserialize)]
struct BiliupCookies {
    cookie_info: CookieInfo,
}
#[derive(Deserialize)]
struct CookieInfo {
    cookies: Vec<Cookie>,
}
#[derive(Deserialize)]
struct Cookie {
    name: String,
    value: String,
}

impl BiliSubtitleClient {
    /// 从 biliup 格式的 cookies JSON 文件读取登录态（SESSDATA/bili_jct 等）。
    pub fn from_cookies_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("读取 Bilibili cookies 失败: {}", path.display()))?;
        let parsed: BiliupCookies = serde_json::from_str(&raw)
            .with_context(|| format!("解析 Bilibili cookies 失败: {}", path.display()))?;
        let mut parts = Vec::new();
        let mut csrf = String::new();
        for cookie in parsed.cookie_info.cookies {
            if cookie.name == "bili_jct" {
                csrf = cookie.value.clone();
            }
            parts.push(format!("{}={}", cookie.name, cookie.value));
        }
        if csrf.is_empty() {
            bail!("cookies 缺少 bili_jct，请重新登录 Bilibili")
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .build()?,
            cookie_header: parts.join("; "),
            csrf,
        })
    }

    /// 查询稿件 aid/cid。
    pub async fn view(&self, bvid: &str) -> Result<VideoView> {
        let resp = self
            .http
            .get(VIEW_API)
            .query(&[("bvid", bvid)])
            .header("Referer", "https://www.bilibili.com/")
            .send()
            .await?;
        let value: serde_json::Value = resp.json().await?;
        if value["code"].as_i64() != Some(0) {
            bail!(
                "查询稿件 {bvid} 失败: code={} {}",
                value["code"],
                value["message"].as_str().unwrap_or("")
            )
        }
        Ok(VideoView {
            aid: value["data"]["aid"].as_i64().context("响应缺少 aid")?,
            cid: value["data"]["cid"].as_i64().context("响应缺少 cid")?,
            title: value["data"]["title"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            duration: value["data"]["duration"]
                .as_f64()
                .context("响应缺少 duration")?,
        })
    }

    /// 查询视频是否已有指定语言（短码，如 zh）的字幕。
    pub async fn has_subtitle_lan(&self, cid: i64, lan: &str) -> Result<bool> {
        let resp = self
            .http
            .get(LAN_API)
            .query(&[("type", "1"), ("oid", &cid.to_string())])
            .header("Referer", "https://member.bilibili.com/")
            .header("Cookie", &self.cookie_header)
            .send()
            .await?;
        let value: serde_json::Value = resp.json().await?;
        if value["code"].as_i64() != Some(0) {
            bail!(
                "查询字幕语言失败: code={} {}",
                value["code"],
                value["message"].as_str().unwrap_or("")
            )
        }
        Ok(value["data"]["lans"]
            .as_array()
            .map(|lans| lans.iter().any(|l| l["lan"].as_str() == Some(lan)))
            .unwrap_or(false))
    }

    /// 提交字幕（submit=1 直接发布，走 B站审核）。
    pub async fn submit(&self, view: &VideoView, lan: &str, cues: &[CcCue]) -> Result<()> {
        let body: Vec<serde_json::Value> = cues
            .iter()
            .map(|c| {
                json!({
                    "from": c.from,
                    "to": c.to,
                    "content": c.content,
                    "location": 1,
                })
            })
            .collect();
        let data = serde_json::to_string(&json!({ "body": body }))?;
        let resp = self
            .http
            .post(SUBMIT_API)
            .header("Referer", "https://member.bilibili.com/")
            .header("Cookie", &self.cookie_header)
            .form(&[
                ("type", "1"),
                ("oid", &view.cid.to_string()),
                ("aid", &view.aid.to_string()),
                ("lan", lan),
                ("data", &data),
                ("submit", "1"),
                ("csrf", &self.csrf),
            ])
            .send()
            .await?;
        let value: serde_json::Value = resp.json().await?;
        match value["code"].as_i64() {
            Some(0) => Ok(()),
            Some(code) => {
                let detail = value["data"]
                    .as_array()
                    .map(|rows| {
                        rows.iter()
                            .filter_map(|r| {
                                r["error_msg"]
                                    .as_str()
                                    .map(|m| format!("line {}: {m}", r["line"]))
                            })
                            .collect::<Vec<_>>()
                            .join("; ")
                    })
                    .unwrap_or_default();
                let base = value["message"].as_str().unwrap_or("");
                if detail.is_empty() {
                    bail!("提交字幕失败: code={code} {base}")
                }
                bail!("提交字幕失败: code={code} {base}（{detail}）")
            }
            None => bail!("提交字幕失败: 响应缺少 code"),
        }
    }
}
