use anyhow::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cue {
    pub start: f64,
    pub end: f64,
    pub source: String,
    pub translation: Option<String>,
}

pub fn parse_vtt(path: &Path) -> Result<Vec<Cue>> {
    let raw = fs::read_to_string(path)?;
    let timing =
        Regex::new(r"(?m)^(\d{2}:)?\d{2}:\d{2}\.\d{3}\s+-->\s+(\d{2}:)?\d{2}:\d{2}\.\d{3}")?;
    let tags = Regex::new(r"<[^>]+>")?;
    let mut cues = Vec::new();
    for block in raw.replace("\r\n", "\n").split("\n\n") {
        let lines: Vec<_> = block.lines().collect();
        let Some(i) = lines.iter().position(|l| timing.is_match(l)) else {
            continue;
        };
        let Some((a, b)) = lines[i].split_once(" --> ") else {
            continue;
        };
        let start = parse_ts(a)?;
        let end = parse_ts(b.split_whitespace().next().unwrap_or(b))?;
        let source = tags
            .replace_all(&lines[i + 1..].join(" "), "")
            .replace("&amp;", "&")
            .replace("&nbsp;", " ");
        let source = source.split_whitespace().collect::<Vec<_>>().join(" ");
        if !source.is_empty() {
            cues.push(Cue {
                start,
                end,
                source,
                translation: None,
            });
        }
    }
    dedup_overlaps(&mut cues);
    dedup_rolling(&mut cues);
    if cues.is_empty() {
        bail!("字幕文件没有有效 cue: {}", path.display())
    }
    Ok(cues)
}

fn parse_ts(s: &str) -> Result<f64> {
    let parts: Vec<_> = s.trim().split(':').collect();
    let (h, m, sec) = match parts.as_slice() {
        [m, s] => (0.0, m.parse::<f64>()?, s.parse::<f64>()?),
        [h, m, s] => (h.parse::<f64>()?, m.parse::<f64>()?, s.parse::<f64>()?),
        _ => bail!("无效字幕时间: {s}"),
    };
    Ok(h * 3600.0 + m * 60.0 + sec)
}
fn dedup_overlaps(c: &mut Vec<Cue>) {
    c.sort_by(|a, b| a.start.total_cmp(&b.start));
    c.dedup_by(|a, b| (a.start - b.start).abs() < 0.01 && a.source == b.source);
}

/// YouTube 自动字幕按“滚动窗口”生成：每条 cue 重复上一条的尾部文本再追加新词，
/// 相邻 cue 间存在大量前缀/后缀重叠。这里把每条 cue 裁剪为“相对上一条新增的内容”，
/// 使每个句子只出现一次，避免分句/翻译 token 浪费和译文重复结巴。
fn dedup_rolling(c: &mut Vec<Cue>) {
    const MIN_OVERLAP_CHARS: usize = 10;
    if c.len() < 2 {
        return;
    }
    let mut carry = String::new();
    let mut out = Vec::with_capacity(c.len());
    for cue in c.drain(..) {
        let original = cue.source.trim().to_string();
        let stripped = strip_rolling_overlap(&carry, &original, MIN_OVERLAP_CHARS);
        // carry 始终取“原始滚动窗口”文本，而不是裁剪后的，保证后续重叠检测正确。
        carry = original;
        if stripped.is_empty() {
            continue;
        }
        out.push(Cue {
            source: stripped,
            ..cue
        });
    }
    *c = out;
}

/// 若 `text` 以 `carry` 的一个“词边界对齐”的后缀开头（即文本滚动重叠），
/// 则裁掉该重叠前缀，只返回新增内容；无重叠时原样返回。
fn strip_rolling_overlap(carry: &str, text: &str, min_chars: usize) -> String {
    if carry.is_empty() || text.is_empty() {
        return text.to_string();
    }
    let limit = text.len().min(carry.len());
    if limit < min_chars {
        return text.to_string();
    }
    // 从最长重叠往下找：carry 的尾部 == text 的头部，且剪切点位于词边界。
    for overlap in (min_chars..=limit).rev() {
        if &text[..overlap] != &carry[carry.len() - overlap..] {
            continue;
        }
        if overlap < text.len() && !text.is_char_boundary(overlap) {
            continue;
        }
        let rest = &text[overlap..];
        if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
            continue;
        }
        return rest.trim_start().to_string();
    }
    text.to_string()
}

pub fn apply_ranges(cues: &[Cue], ranges: &[(usize, usize)]) -> Result<Vec<Cue>> {
    if cues.is_empty() {
        return Ok(Vec::new());
    }
    let mut expected = 0;
    let mut out = Vec::new();
    for &(start, end) in ranges {
        if start != expected || end < start || end >= cues.len() {
            bail!("分句范围不连续或越界: {start}..{end}, expected={expected}")
        }
        let source = cues[start..=end]
            .iter()
            .map(|x| x.source.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        out.push(Cue {
            start: cues[start].start,
            end: cues[end].end,
            source,
            translation: None,
        });
        expected = end + 1;
    }
    if expected != cues.len() {
        bail!("分句没有覆盖全部字幕: {expected}/{}", cues.len())
    }
    Ok(out)
}

pub fn apply_translations(cues: &mut [Cue], translations: &[(usize, String)]) -> Result<()> {
    for (i, text) in translations {
        if *i >= cues.len() {
            bail!("翻译索引越界: {i}")
        }
        let clean = text
            .chars()
            .filter(|c| !c.is_control() || *c == '\n')
            .collect::<String>()
            .trim()
            .to_string();
        // 单条空翻译直接跳过（保留 translation=None），避免整批失败；
        // 后续 CC 提交会过滤无翻译的 cue。
        if clean.is_empty() {
            continue;
        }
        cues[*i].translation = Some(clean);
    }
    Ok(())
}

pub fn save_json(cues: &[Cue], path: &Path) -> Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("subtitles.json");
    let temporary = path.with_file_name(format!(".{name}.tmp"));
    fs::write(&temporary, serde_json::to_vec_pretty(cues)?)?;
    fs::rename(&temporary, path)?;
    Ok(())
}
pub fn load_json(path: &Path) -> Result<Vec<Cue>> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_dedup_keeps_only_new_words() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 1.0,
                source: "bit. That's always just been a counter when these two brawlers weren't meta. So".into(),
                translation: None,
            },
            Cue {
                start: 1.1,
                end: 2.0,
                source: "when these two brawlers weren't meta. So".into(),
                translation: None,
            },
            Cue {
                start: 2.1,
                end: 3.0,
                source: "when these two brawlers weren't meta. So just bear that in mind. Crow is a good".into(),
                translation: None,
            },
            Cue {
                start: 3.1,
                end: 4.0,
                source: "just bear that in mind. Crow is a good".into(),
                translation: None,
            },
            Cue {
                start: 4.1,
                end: 5.0,
                source: "just bear that in mind. Crow is a good counter to me as well. Angel said".into(),
                translation: None,
            },
        ];
        let mut deduped = cues.clone();
        dedup_rolling(&mut deduped);
        let texts: Vec<&str> = deduped.iter().map(|c| c.source.as_str()).collect();
        assert_eq!(
            texts,
            vec![
                "bit. That's always just been a counter when these two brawlers weren't meta. So",
                "just bear that in mind. Crow is a good",
                "counter to me as well. Angel said",
            ]
        );
    }

    #[test]
    fn rolling_dedup_keeps_unrelated_consecutive_cues() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 1.0,
                source: "Hello everyone".into(),
                translation: None,
            },
            Cue {
                start: 1.1,
                end: 2.0,
                source: "Welcome back to the channel".into(),
                translation: None,
            },
        ];
        let mut deduped = cues.clone();
        dedup_rolling(&mut deduped);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].source, "Hello everyone");
        assert_eq!(deduped[1].source, "Welcome back to the channel");
    }
    #[test]
    fn ranges() {
        let c = vec![
            Cue {
                start: 0.,
                end: 1.,
                source: "hello".into(),
                translation: None,
            },
            Cue {
                start: 1.,
                end: 2.,
                source: "world".into(),
                translation: None,
            },
        ];
        let o = apply_ranges(&c, &[(0, 1)]).unwrap();
        assert_eq!(o[0].source, "hello world");
        assert!(apply_ranges(&c, &[(1, 1)]).is_err());
    }
    #[test]
    fn translations() {
        let mut c = vec![Cue {
            start: 0.,
            end: 1.,
            source: "hello".into(),
            translation: None,
        }];
        apply_translations(&mut c, &[(0, "你好".into())]).unwrap();
        assert_eq!(c[0].translation.as_deref(), Some("你好"));
        // 分批应用：只传部分条目不要求与总 cues 数量相等。
        let mut batch = vec![Cue {
            start: 1.,
            end: 2.,
            source: "world".into(),
            translation: None,
        }];
        apply_translations(&mut batch, &[(0, "世界".into())]).unwrap();
        assert_eq!(batch[0].translation.as_deref(), Some("世界"));
    }

    #[test]
    fn json_checkpoint_replaces_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("translated.json");
        let mut cues = vec![Cue {
            start: 0.,
            end: 1.,
            source: "hello".into(),
            translation: None,
        }];
        save_json(&cues, &path).unwrap();
        cues[0].translation = Some("你好".into());
        save_json(&cues, &path).unwrap();

        assert_eq!(load_json(&path).unwrap(), cues);
        assert!(!directory.path().join(".translated.json.tmp").exists());
    }
}
