//! 将原 Pi extension 的策略/词库注入移入进程内；每个 Pipeline 只加载一次。
use anyhow::{Context, Result};
use regex::{Regex, RegexBuilder};
use serde_json::Value;
use std::{collections::BTreeMap, fs, path::Path};

const SYSTEM_PROMPT: &str = include_str!("../../pi/system-prompt.txt");

struct Term {
    source: String,
    normalized: String,
    translation: String,
}

struct Pattern {
    regex: Regex,
    left_boundary: bool,
    right_boundary: bool,
    translation: String,
}

pub(super) struct PromptPolicy {
    policy: Value,
    terms: Vec<Term>,
    curated: Vec<Term>,
    patterns: Vec<Pattern>,
}

fn normalize(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn boundary_matches(text: &str, start: usize, end: usize, left: bool, right: bool) -> bool {
    (!left || start == 0 || !text.as_bytes()[start - 1].is_ascii_alphanumeric())
        && (!right || end == text.len() || !text.as_bytes()[end].is_ascii_alphanumeric())
}

impl Term {
    fn matches(&self, text: &str) -> bool {
        let left = self.normalized.as_bytes()[0].is_ascii_alphanumeric();
        let right = self.normalized.as_bytes()[self.normalized.len() - 1].is_ascii_alphanumeric();
        text.match_indices(&self.normalized)
            .any(|(start, term)| boundary_matches(text, start, start + term.len(), left, right))
    }
}

fn terms(layer: &Value) -> Vec<Term> {
    layer
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(source, entry)| {
            let translation = entry.as_str().or_else(|| entry["translation"].as_str())?;
            let normalized = normalize(source);
            (!normalized.is_empty()).then(|| Term {
                source: source.clone(),
                normalized,
                translation: translation.to_owned(),
            })
        })
        .collect()
}

impl PromptPolicy {
    pub(super) fn load(path: &Path) -> Result<Self> {
        let policy: Value = serde_json::from_slice(&fs::read(path).context("读取 AI policy")?)
            .context("AI policy JSON 无效")?;
        anyhow::ensure!(policy.is_object(), "AI policy 必须是 JSON 对象");
        let mut merged = BTreeMap::new();
        let mut patterns = Vec::new();
        let glossary_path = path.with_file_name("brawl-stars-glossary.json");
        if path
            .file_name()
            .is_none_or(|name| name != "audit-policy.json")
            && glossary_path.exists()
        {
            let doc: Value =
                serde_json::from_slice(&fs::read(&glossary_path)?).context("官方词库 JSON 无效")?;
            let layers = if doc["version"] == 1 {
                vec![&doc["glossary"]]
            } else {
                vec![&doc["legacy"], &doc["active"]]
            };
            for layer in layers {
                for term in terms(layer) {
                    merged.insert(term.normalized.clone(), term);
                }
            }
            for rule in doc["patterns"].as_array().into_iter().flatten() {
                let (Some(pattern), Some(translation)) =
                    (rule["pattern"].as_str(), rule["translation"].as_str())
                else {
                    continue;
                };
                // Rust regex 不支持 lookaround；边界在匹配后检查，保留现有词库规则语义。
                let left_boundary = pattern.starts_with("(?<![A-Za-z0-9])");
                let right_boundary = pattern.ends_with("(?![A-Za-z0-9])");
                let core = pattern
                    .trim_start_matches("(?<![A-Za-z0-9])")
                    .trim_end_matches("(?![A-Za-z0-9])");
                let flags = rule["flags"].as_str().unwrap_or("i");
                patterns.push(Pattern {
                    regex: RegexBuilder::new(core)
                        .case_insensitive(flags.contains('i'))
                        .multi_line(flags.contains('m'))
                        .dot_matches_new_line(flags.contains('s'))
                        .build()
                        .context("官方词库 pattern 无效")?,
                    left_boundary,
                    right_boundary,
                    translation: translation.into(),
                });
            }
        }
        let curated = terms(&policy["glossary"]);
        Ok(Self {
            policy,
            terms: merged.into_values().collect(),
            curated,
            patterns,
        })
    }

    fn glossary(&self, input: &str) -> Value {
        let normalized = normalize(input);
        let mut selected = BTreeMap::new();
        for term in &self.terms {
            if term.matches(&normalized) {
                selected.insert(
                    term.normalized.clone(),
                    (term.source.clone(), term.translation.clone()),
                );
            }
        }
        for pattern in &self.patterns {
            for captures in pattern.regex.captures_iter(input) {
                let matched = captures.get(0).expect("regex whole match");
                if !boundary_matches(
                    input,
                    matched.start(),
                    matched.end(),
                    pattern.left_boundary,
                    pattern.right_boundary,
                ) {
                    continue;
                }
                let mut translation = pattern.translation.clone();
                for index in 1..captures.len() {
                    translation = translation.replace(
                        &format!("{{{index}}}"),
                        captures.get(index).map_or("", |m| m.as_str()),
                    );
                }
                selected.insert(
                    normalize(matched.as_str()),
                    (matched.as_str().to_owned(), translation),
                );
            }
        }
        for term in &self.curated {
            if term.matches(&normalized) {
                selected.insert(
                    term.normalized.clone(),
                    (term.source.clone(), term.translation.clone()),
                );
            }
        }
        Value::Object(
            selected
                .into_values()
                .map(|(s, t)| (s, Value::String(t)))
                .collect(),
        )
    }

    pub(super) fn system_prompt(&self, input: &str) -> String {
        let mut policy = self.policy.clone();
        policy["glossary"] = self.glossary(input);
        format!(
            "{SYSTEM_PROMPT}{}",
            serde_json::to_string_pretty(&policy).expect("JSON value serializes")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glossary_keeps_layer_precedence_numeric_patterns_and_word_boundaries() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("pi/policy.json");
        let policy = PromptPolicy::load(&path).unwrap();
        let value = policy.glossary(
            "BRAWL   STARS Buzz buzzing 1,200 Gems 200 Coins 50 Power Points 3 Bling Xbox",
        );
        assert_eq!(value["Brawl Stars"], "荒野乱斗");
        assert_eq!(value["Buzz"], "巴兹");
        assert_eq!(value["1,200 Gems"], "1,200宝石");
        assert_eq!(value["200 Coins"], "200金币");
        assert_eq!(value["50 Power Points"], "50战力能量");
        assert_eq!(value["3 Bling"], "3闪闪币");
        assert!(policy.glossary("buzzing").get("Buzz").is_none());
        let audit = PromptPolicy::load(&path.with_file_name("audit-policy.json")).unwrap();
        assert_eq!(audit.glossary("Buzz 50 Gems"), serde_json::json!({}));
    }
}
