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
    if translations.len() != cues.len() {
        bail!("翻译数量不匹配: {}/{}", translations.len(), cues.len())
    }
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
        if clean.is_empty() && cues[*i].source.chars().any(|c| c.is_alphanumeric()) {
            bail!("第 {i} 条翻译为空")
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
