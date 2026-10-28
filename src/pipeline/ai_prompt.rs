//! 从外部文件加载提示词、策略和词库；每个 Pipeline 只加载一次。
use anyhow::{Context, Result};
use regex::{Regex, RegexBuilder};
use serde_json::Value;
use std::{collections::BTreeMap, fs, path::Path};

struct Term {
    source: String,
    normalized: String,
    translation: String,
}

struct Pattern {
    regex: Regex,
    right_boundary: bool,
    translation: String,
}

pub(super) struct PromptPolicy {
    system_prompt: String,
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
        let prompt_path = path.with_file_name("system-prompt.txt");
        let system_prompt = fs::read_to_string(&prompt_path)
            .with_context(|| format!("读取 AI 提示词 {}", prompt_path.display()))?;
        anyhow::ensure!(!system_prompt.trim().is_empty(), "AI 提示词不能为空");
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
                // 左边界必须参与搜索，避免先匹配后过滤时跳过逗号后的合法数值。
                // 捕获组 1 保留原始词条，排除为模拟 lookbehind 消耗的前导字符。
                let left_boundary = pattern.starts_with("(?<![A-Za-z0-9])");
                let right_boundary = pattern.ends_with("(?![A-Za-z0-9])");
                let core = pattern
                    .trim_start_matches("(?<![A-Za-z0-9])")
                    .trim_end_matches("(?![A-Za-z0-9])");
                let core = if left_boundary {
                    format!("(?:^|[^A-Za-z0-9])({core})")
                } else {
                    format!("({core})")
                };
                let flags = rule["flags"].as_str().unwrap_or("i");
                patterns.push(Pattern {
                    regex: RegexBuilder::new(&core)
                        .case_insensitive(flags.contains('i'))
                        .multi_line(flags.contains('m'))
                        .dot_matches_new_line(flags.contains('s'))
                        .build()
                        .context("官方词库 pattern 无效")?,
                    right_boundary,
                    translation: translation.into(),
                });
            }
        }
        let curated = terms(&policy["glossary"]);
        Ok(Self {
            system_prompt,
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
                let matched = captures.get(1).expect("regex term capture");
                if !boundary_matches(
                    input,
                    matched.start(),
                    matched.end(),
                    false,
                    pattern.right_boundary,
                ) {
                    continue;
                }
                let mut translation = pattern.translation.clone();
                for index in 2..captures.len() {
                    translation = translation.replace(
                        &format!("{{{}}}", index - 1),
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
            "{}{}",
            self.system_prompt,
            serde_json::to_string_pretty(&policy).expect("JSON value serializes")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_comes_from_runtime_file_and_is_reused_until_reloaded() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("policy.json");
        let prompt_path = temp.path().join("system-prompt.txt");
        fs::write(&path, "{}").unwrap();
        assert!(PromptPolicy::load(&path).is_err());
        for invalid in [b" \n".as_slice(), b"\xff".as_slice()] {
            fs::write(&prompt_path, invalid).unwrap();
            assert!(PromptPolicy::load(&path).is_err());
        }
        fs::write(&prompt_path, "External prompt\n").unwrap();
        let cached = PromptPolicy::load(&path).unwrap();
        fs::write(&prompt_path, "Updated prompt\n").unwrap();
        assert_eq!(
            cached.system_prompt(""),
            "External prompt\n{\n  \"glossary\": {}\n}"
        );
        let reloaded = PromptPolicy::load(&path).unwrap();
        fs::remove_file(&prompt_path).unwrap();
        assert!(reloaded.system_prompt("").starts_with("Updated prompt\n"));
    }

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
        let fallback = policy.glossary("x1,200 Gems x3,500 Coins x1.200 Power Points x1.20 Bling");
        assert_eq!(fallback["200 Gems"], "200宝石");
        assert_eq!(fallback["500 Coins"], "500金币");
        assert_eq!(fallback["200 Power Points"], "200战力能量");
        assert_eq!(fallback["20 Bling"], "20闪闪币");
        assert!(
            policy
                .glossary("x200 Coins 200 Coinsx")
                .get("200 Coins")
                .is_none()
        );
        let audit = PromptPolicy::load(&path.with_file_name("audit-policy.json")).unwrap();
        assert_eq!(audit.glossary("Buzz 50 Gems"), serde_json::json!({}));
    }
}
