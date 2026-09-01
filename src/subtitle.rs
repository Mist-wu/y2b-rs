use anyhow::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::OnceLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cue {
    pub start: f64,
    pub end: f64,
    pub source: String,
    pub translation: Option<String>,
}

#[derive(Debug)]
struct VttBlock {
    cue: Cue,
    inline_parts: Vec<Cue>,
}

const INLINE_ATOM_MAX_DURATION_SECONDS: f64 = 4.0;
const INLINE_ATOM_MAX_CHARS: usize = 40;
const INLINE_ATOM_MAX_WORDS: usize = 8;

pub fn parse_vtt(path: &Path) -> Result<Vec<Cue>> {
    let raw = fs::read_to_string(path)?;
    let timing = Regex::new(
        r"^(?<start>(?:\d{2,}:)?\d{2}:\d{2}\.\d{3})[ \t]+-->[ \t]+(?<end>(?:\d{2,}:)?\d{2}:\d{2}\.\d{3})(?:[ \t]+.*)?$",
    )?;
    let tags = Regex::new(r"<[^>]+>")?;
    let inline_timing = Regex::new(r"<(?:(?:\d{2,}):)?\d{2}:\d{2}\.\d{3}>")?;
    let mut blocks = Vec::new();
    for block in raw.replace("\r\n", "\n").split("\n\n") {
        let lines: Vec<_> = block.lines().collect();
        let Some((i, captures)) = lines
            .iter()
            .enumerate()
            .find_map(|(index, line)| timing.captures(line).map(|captures| (index, captures)))
        else {
            continue;
        };
        let start = parse_ts(&captures["start"])?;
        let end = parse_ts(&captures["end"])?;
        let raw_source = lines[i + 1..].join("\n");
        let source = clean_vtt_text(&raw_source, &tags);
        if !source.is_empty() {
            blocks.push(VttBlock {
                cue: Cue {
                    start,
                    end,
                    source,
                    translation: None,
                },
                inline_parts: parse_inline_parts(&raw_source, start, end, &tags, &inline_timing)?,
            });
        }
    }
    blocks.sort_by(|a, b| a.cue.start.total_cmp(&b.cue.start));
    blocks
        .dedup_by(|a, b| (a.cue.start - b.cue.start).abs() < 0.01 && a.cue.source == b.cue.source);
    let mut cues = refine_rolling_blocks(blocks);
    normalize_inline_ends(&mut cues);
    cues = group_inline_atoms(cues);
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

fn clean_vtt_text(raw: &str, tags: &Regex) -> String {
    normalize_caption_text(&tags.replace_all(raw, ""), false)
}

fn music_marker_regex() -> &'static Regex {
    static MUSIC_MARKERS: OnceLock<Regex> = OnceLock::new();
    MUSIC_MARKERS.get_or_init(|| {
        Regex::new(
            r"(?i)(?:\[\s*(?:music|音乐)\s*\]|【\s*音乐\s*】|[（(]\s*(?:music|音乐)\s*[）)]|[♪♫♬♩]+)",
        )
        .expect("音乐字幕标记正则必须有效")
    })
}

/// 清理不应出现在成品字幕里的传输层和无障碍字幕标记。
///
/// YouTube WebVTT 会把说话人提示写成 `&gt;&gt;`。源字幕先解码但保留 `>>`，
/// 让分句和翻译仍能识别说话人切换；成品字幕再移除，避免 B站显示 HTML 实体或
/// 箭头。背景音乐只删除方括号/括号形式的标签和音符，不删除正常语句中的“音乐”。
fn decode_named_html_entity(name: &str) -> Option<&'static str> {
    Some(match name {
        "amp" => "&",
        "apos" => "'",
        "gt" => ">",
        "lt" => "<",
        "quot" => "\"",
        "nbsp" => "\u{00a0}",
        "lrm" => "\u{200e}",
        "rlm" => "\u{200f}",
        "copy" => "©",
        "reg" => "®",
        "trade" => "™",
        "hellip" => "…",
        "ndash" => "–",
        "mdash" => "—",
        "lsquo" => "‘",
        "rsquo" => "’",
        "ldquo" => "“",
        "rdquo" => "”",
        "bull" => "•",
        "middot" => "·",
        "times" => "×",
        "divide" => "÷",
        "cent" => "¢",
        "pound" => "£",
        "yen" => "¥",
        "euro" => "€",
        "deg" => "°",
        "plusmn" => "±",
        "micro" => "µ",
        "para" => "¶",
        "sect" => "§",
        "laquo" => "«",
        "raquo" => "»",
        "iexcl" => "¡",
        "iquest" => "¿",
        _ => return None,
    })
}

fn decode_numeric_html_entity(value: &str) -> Option<char> {
    let (digits, radix) = value
        .strip_prefix(['x', 'X'])
        .map_or((value, 10), |digits| (digits, 16));
    if digits.is_empty() {
        return None;
    }
    let number = u32::from_str_radix(digits, radix).ok()?;
    let windows_1252 = match number {
        0x80 => Some('€'),
        0x82 => Some('‚'),
        0x83 => Some('ƒ'),
        0x84 => Some('„'),
        0x85 => Some('…'),
        0x86 => Some('†'),
        0x87 => Some('‡'),
        0x88 => Some('ˆ'),
        0x89 => Some('‰'),
        0x8a => Some('Š'),
        0x8b => Some('‹'),
        0x8c => Some('Œ'),
        0x8e => Some('Ž'),
        0x91 => Some('‘'),
        0x92 => Some('’'),
        0x93 => Some('“'),
        0x94 => Some('”'),
        0x95 => Some('•'),
        0x96 => Some('–'),
        0x97 => Some('—'),
        0x98 => Some('˜'),
        0x99 => Some('™'),
        0x9a => Some('š'),
        0x9b => Some('›'),
        0x9c => Some('œ'),
        0x9e => Some('ž'),
        0x9f => Some('Ÿ'),
        _ => None,
    };
    windows_1252
        .or_else(|| char::from_u32(number).filter(|_| number != 0))
        .or(Some('\u{fffd}'))
}

/// 按 HTML 的单次字符引用规则解码；未知实体保持原样，`&amp;gt;` 不会被二次解码。
fn decode_html_entities(raw: &str) -> String {
    let mut decoded = String::with_capacity(raw.len());
    let mut remaining = raw;
    while let Some(start) = remaining.find('&') {
        decoded.push_str(&remaining[..start]);
        let entity = &remaining[start + 1..];
        let Some(end) = entity.find(';').filter(|end| *end <= 32) else {
            decoded.push('&');
            remaining = entity;
            continue;
        };
        let name = &entity[..end];
        let replacement = name
            .strip_prefix('#')
            .and_then(decode_numeric_html_entity)
            .map(|character| character.to_string())
            .or_else(|| decode_named_html_entity(name).map(str::to_string));
        if let Some(replacement) = replacement {
            decoded.push_str(&replacement);
            remaining = &entity[end + 1..];
        } else {
            decoded.push('&');
            remaining = entity;
        }
    }
    decoded.push_str(remaining);
    decoded
}

fn normalize_caption_text(raw: &str, remove_speaker_markers: bool) -> String {
    let decoded = decode_html_entities(raw);
    let without_music = music_marker_regex().replace_all(&decoded, "");
    let speaker_cleaned = if remove_speaker_markers {
        without_music.replace(">>", "").replace("＞＞", "")
    } else {
        without_music.into_owned()
    };
    let printable = speaker_cleaned
        .chars()
        .filter(|c| !c.is_control() || *c == '\n')
        .filter(|c| {
            !matches!(
                *c,
                '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2060}'..='\u{206f}' | '\u{feff}'
            )
        })
        .collect::<String>();
    printable.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// 生成成品字幕文本，移除内部保留的说话人标记。
pub fn sanitize_caption_text(raw: &str) -> String {
    normalize_caption_text(raw, true)
}

fn parse_inline_parts(
    raw: &str,
    block_start: f64,
    block_end: f64,
    tags: &Regex,
    inline_timing: &Regex,
) -> Result<Vec<Cue>> {
    let tagged = raw
        .lines()
        .filter(|line| inline_timing.is_match(line))
        .collect::<Vec<_>>()
        .join(" ");
    let matches = inline_timing.find_iter(&tagged).collect::<Vec<_>>();
    if matches.is_empty() {
        return Ok(Vec::new());
    }
    let mut parts: Vec<(f64, String)> = Vec::new();
    let leading = clean_vtt_text(&tagged[..matches[0].start()], tags);
    if !leading.is_empty() {
        parts.push((block_start, leading));
    }
    for (index, timing) in matches.iter().enumerate() {
        let end = matches
            .get(index + 1)
            .map_or(tagged.len(), |next| next.start());
        let source = clean_vtt_text(&tagged[timing.end()..end], tags);
        if source.is_empty() {
            continue;
        }
        let start = parse_ts(&tagged[timing.start() + 1..timing.end() - 1])?;
        if let Some((previous_start, previous_source)) = parts.last_mut()
            && (start - *previous_start).abs() < 0.001
        {
            previous_source.push(' ');
            previous_source.push_str(&source);
        } else {
            parts.push((start, source));
        }
    }
    let mut cues = Vec::with_capacity(parts.len());
    for (index, (start, source)) in parts.iter().enumerate() {
        let end = parts
            .get(index + 1)
            .map(|next| next.0)
            .unwrap_or_else(|| block_end.min(start + 1.0));
        cues.push(Cue {
            start: *start,
            end: end.max(start + 0.01),
            source: source.clone(),
            translation: None,
        });
    }
    Ok(cues)
}

fn fallback_cue_end(cue: &Cue, rolling_overlap_removed: bool) -> f64 {
    if !rolling_overlap_removed && cue.end - cue.start <= 8.0 {
        return cue.end;
    }
    let estimated_speech = (cue.source.split_whitespace().count() as f64 * 0.45).clamp(0.8, 8.0);
    cue.end.min(cue.start + estimated_speech)
}

fn refine_rolling_blocks(blocks: Vec<VttBlock>) -> Vec<Cue> {
    const MIN_OVERLAP_CHARS: usize = 10;
    let mut carry = String::new();
    let mut out = Vec::new();
    for block in blocks {
        let original = block.cue.source.trim().to_string();
        let stripped = strip_rolling_overlap(&carry, &original, MIN_OVERLAP_CHARS);
        carry = original.clone();
        if stripped.is_empty() {
            continue;
        }
        let inline_source = block
            .inline_parts
            .iter()
            .map(|cue| cue.source.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        if !block.inline_parts.is_empty() && inline_source == stripped {
            out.extend(block.inline_parts);
            continue;
        }
        let mut cue = Cue {
            source: stripped,
            ..block.cue
        };
        cue.end = fallback_cue_end(&cue, cue.source != original);
        if cue.end > cue.start {
            out.push(cue);
        }
    }
    out
}

fn normalize_inline_ends(cues: &mut [Cue]) {
    cues.sort_by(|a, b| a.start.total_cmp(&b.start));
    for index in 0..cues.len().saturating_sub(1) {
        if cues[index].end > cues[index + 1].start {
            cues[index].end = cues[index + 1].start.max(cues[index].start + 0.01);
        }
    }
}

fn inline_atom_metrics(cues: &[Cue]) -> (f64, usize, usize) {
    let duration = cues.last().unwrap().end - cues[0].start;
    let chars = cues
        .iter()
        .map(|cue| cue.source.chars().count())
        .sum::<usize>()
        + cues.len().saturating_sub(1);
    let words = cues
        .iter()
        .map(|cue| cue.source.split_whitespace().count())
        .sum();
    (duration, chars, words)
}

fn ends_inline_atom(source: &str) -> bool {
    source
        .trim_end_matches(|character: char| {
            character.is_whitespace()
                || matches!(character, '"' | '\'' | '”' | '’' | ')' | ']' | '}')
        })
        .ends_with(['.', '!', '?', ',', ';', ':'])
}

fn merge_atom(cues: &[Cue]) -> Cue {
    Cue {
        start: cues[0].start,
        end: cues.last().unwrap().end,
        source: cues
            .iter()
            .map(|cue| cue.source.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        translation: None,
    }
}

fn group_inline_atoms(cues: Vec<Cue>) -> Vec<Cue> {
    let mut output = Vec::new();
    let mut current: Vec<Cue> = Vec::new();
    for cue in cues {
        if !current.is_empty() {
            let mut candidate = current.clone();
            candidate.push(cue.clone());
            let (duration, chars, words) = inline_atom_metrics(&candidate);
            let gap = cue.start - current.last().unwrap().end;
            if gap >= 0.8
                || duration > INLINE_ATOM_MAX_DURATION_SECONDS
                || chars > INLINE_ATOM_MAX_CHARS
                || words > INLINE_ATOM_MAX_WORDS
            {
                output.push(merge_atom(&current));
                current.clear();
            }
        }
        let boundary = ends_inline_atom(&cue.source);
        current.push(cue);
        if boundary {
            output.push(merge_atom(&current));
            current.clear();
        }
    }
    if !current.is_empty() {
        output.push(merge_atom(&current));
    }
    output
}

/// YouTube 自动字幕按“滚动窗口”生成：每条 cue 重复上一条的尾部文本再追加新词，
/// 相邻 cue 间存在大量前缀/后缀重叠。这里把每条 cue 裁剪为“相对上一条新增的内容”，
/// 使每个句子只出现一次，避免分句/翻译 token 浪费和译文重复结巴。
#[cfg(test)]
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
    // YouTube 会在滚动窗口之间插入 0.01 秒的短快照，例如上一条以 `match?`
    // 结尾，下一条只含 `match?`，再下一条又以 `match?` 开头。短词达不到常规
    // 10 字符阈值，但“较短一侧被完整包含”足以证明它是滚动重叠。
    if text.is_char_boundary(limit)
        && carry.is_char_boundary(carry.len() - limit)
        && text[..limit] == carry[carry.len() - limit..]
    {
        let rest = &text[limit..];
        if rest.is_empty() || rest.starts_with(char::is_whitespace) {
            return rest.trim_start().to_string();
        }
    }
    if limit < min_chars {
        return text.to_string();
    }
    // 从最长重叠往下找：carry 的尾部 == text 的头部，且剪切点位于词边界。
    //
    // `overlap` 是字节长度，必须在切片**之前**确认它同时落在两个字符串的字符
    // 边界上，否则含 `♪`、`’` 等多字节字符的字幕会直接 panic。
    for overlap in (min_chars..=limit).rev() {
        if !text.is_char_boundary(overlap) || !carry.is_char_boundary(carry.len() - overlap) {
            continue;
        }
        if text[..overlap] != carry[carry.len() - overlap..] {
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
        // 空译文也存档：模型按提示词丢弃语气词（Um、So…）属于有意留白，
        // 存 Some("") 才能让检查点判定该批次已完成，不会在后续 CC 重试时
        // 反复重翻同一批；CC 提交端会过滤空内容。
        cues[*i].translation = Some(sanitize_caption_text(text));
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
    fn youtube_inline_timestamps_create_natural_short_atoms() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("captions.vtt");
        fs::write(
            &path,
            r#"WEBVTT

00:00:01.000 --> 00:00:03.000 align:start position:0%
Hello<00:00:01.400><c> friends.</c><00:00:02.000><c> Today</c>

00:00:03.000 --> 00:00:03.010 align:start position:0%
Hello friends. Today

00:00:03.010 --> 00:00:05.000 align:start position:0%
Hello friends. Today
we<00:00:03.400><c> will</c><00:00:03.800><c> go.</c>

00:00:12.000 --> 00:00:30.000 align:start position:0%
Nice.
"#,
        )
        .unwrap();

        let cues = parse_vtt(&path).unwrap();
        assert_eq!(
            cues.iter()
                .map(|cue| cue.source.as_str())
                .collect::<Vec<_>>(),
            vec!["Hello friends.", "Today we will go.", "Nice."]
        );
        assert_eq!(cues[0].start, 1.0);
        assert_eq!(cues[0].end, 2.0);
        assert_eq!(cues[1].start, 2.0);
        assert!(cues[1].end <= 5.0);
        assert_eq!(cues[2].start, 12.0);
        assert!(cues[2].end - cues[2].start <= 0.8 + 1e-6);
    }

    #[test]
    fn vtt_timing_accepts_standard_whitespace_and_long_hours() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("captions.vtt");
        fs::write(
            &path,
            "WEBVTT\n\nfirst cue\n00:00:01.000\t-->   00:00:02.500 align:start\nHello\n\n123:04:05.006 --> 123:04:06.007\nWorld\n",
        )
        .unwrap();

        let cues = parse_vtt(&path).unwrap();
        assert_eq!(cues.len(), 2);
        assert_eq!((cues[0].start, cues[0].end), (1.0, 2.5));
        assert_eq!(cues[1].start, 123.0 * 3600.0 + 4.0 * 60.0 + 5.006);
    }

    #[test]
    fn vtt_cleanup_decodes_entities_and_removes_music_markers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("captions.vtt");
        fs::write(
            &path,
            r#"WEBVTT

00:00:01.000 --> 00:00:03.000
&gt;&gt; Looks good. [music] Are you ready?

00:00:03.000 --> 00:00:04.000
[MUSIC]

00:00:04.000 --> 00:00:06.000
Brown noise and chill music.
"#,
        )
        .unwrap();

        let cues = parse_vtt(&path).unwrap();
        assert_eq!(
            cues.iter()
                .map(|cue| cue.source.as_str())
                .collect::<Vec<_>>(),
            vec![
                ">> Looks good. Are you ready?",
                "Brown noise and chill music."
            ]
        );
    }

    #[test]
    fn caption_cleanup_only_removes_music_labels_not_normal_words() {
        assert_eq!(
            sanitize_caption_text("&gt;&gt; 棕色噪音。[音乐] 也许听点舒缓音乐。♪"),
            "棕色噪音。 也许听点舒缓音乐。"
        );
        assert_eq!(
            sanitize_caption_text("A &amp; B &amp;gt; C"),
            "A & B &gt; C"
        );
    }

    #[test]
    fn html_entities_decode_named_decimal_and_hex_references_once() {
        assert_eq!(
            sanitize_caption_text(
                "Tom&nbsp;&amp; Jerry&#39;s &#x201c;show&#x201d;&hellip; &#128512; &amp;gt;"
            ),
            "Tom & Jerry's “show”… 😀 &gt;"
        );
        assert_eq!(
            sanitize_caption_text("unknown &nope; entity"),
            "unknown &nope; entity"
        );
    }

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
    fn rolling_dedup_handles_multibyte_subtitle_text() {
        // `♪`（音乐字幕）和 `’`（智能引号）在人工上传的英文字幕里很常见：
        // 重叠扫描必须按字符边界跳过，而不是在字节切片时 panic。
        assert_eq!(
            strip_rolling_overlap(
                "♪♪♪ music playing softly ♪♪♪",
                "♪ and then he said something else entirely",
                10
            ),
            "♪ and then he said something else entirely"
        );
        // 多字节字符参与的真实重叠仍要被正确裁剪。
        assert_eq!(
            strip_rolling_overlap(
                "that’s what I’m talking about here",
                "that’s what I’m talking about here folks",
                10
            ),
            "folks"
        );
        let mut cues = vec![
            Cue {
                start: 0.0,
                end: 1.0,
                source: "♪ so I’m gonna show you the new brawler".into(),
                translation: None,
            },
            Cue {
                start: 1.1,
                end: 2.0,
                source: "so I’m gonna show you the new brawler today ♪".into(),
                translation: None,
            },
        ];
        dedup_rolling(&mut cues);
        assert_eq!(cues.len(), 2);
        assert_eq!(cues[1].source, "today ♪");
    }

    #[test]
    fn rolling_dedup_removes_complete_short_snapshot_overlap() {
        assert_eq!(strip_rolling_overlap("question match?", "match?", 10), "");
        assert_eq!(
            strip_rolling_overlap("Brown.", "Brown. Brown noise.", 10),
            "Brown noise."
        );
        assert_eq!(
            strip_rolling_overlap("Like", "Like Amazing.", 10),
            "Amazing."
        );
        assert_eq!(
            strip_rolling_overlap("No one expected this", "No, that's wrong.", 10),
            "No, that's wrong."
        );
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
        // 空译文（模型丢弃语气词）也算已翻译，检查点才能收敛。
        apply_translations(&mut c, &[(0, "  ".into())]).unwrap();
        assert_eq!(c[0].translation.as_deref(), Some(""));
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
