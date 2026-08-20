import fs from "node:fs";
import path from "node:path";

type PiApi = {
  on(event: "before_agent_start", handler: (event: { prompt: string; systemPrompt: string }) => Promise<{ systemPrompt: string }> | { systemPrompt: string }): void;
};

type Policy = {
  glossary?: Record<string, string>;
  [key: string]: unknown;
};

type GlossaryEntry = string | { translation?: string };
type PatternRule = {
  pattern?: string;
  flags?: string;
  translation?: string;
};
type OfficialGlossary = {
  active: Record<string, GlossaryEntry>;
  legacy: Record<string, GlossaryEntry>;
  patterns: PatternRule[];
};

function normalizeTerm(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLocaleLowerCase("en-US");
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function promptContainsTerm(prompt: string, term: string): boolean {
  const trimmed = term.trim();
  if (!trimmed) return false;
  const pattern = trimmed.split(/\s+/).map(escapeRegExp).join("\\s+");
  const left = /^[A-Za-z0-9]/.test(trimmed) ? "(?<![A-Za-z0-9])" : "";
  const right = /[A-Za-z0-9]$/.test(trimmed) ? "(?![A-Za-z0-9])" : "";
  return new RegExp(`${left}${pattern}${right}`, "iu").test(prompt);
}

function loadOfficialGlossary(policyPath: string): OfficialGlossary {
  if (path.basename(policyPath) === "audit-policy.json") {
    return { active: {}, legacy: {}, patterns: [] };
  }
  const glossaryPath = path.join(path.dirname(policyPath), "brawl-stars-glossary.json");
  if (!fs.existsSync(glossaryPath)) return { active: {}, legacy: {}, patterns: [] };
  const document = JSON.parse(fs.readFileSync(glossaryPath, "utf8"));
  if (document.version === 1) {
    return { active: document.glossary ?? {}, legacy: {}, patterns: [] };
  }
  return {
    active: document.active ?? {},
    legacy: document.legacy ?? {},
    patterns: document.patterns ?? [],
  };
}

function layerTranslations(layer: Record<string, GlossaryEntry>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(layer).flatMap(([term, entry]) => {
      const translation = typeof entry === "string" ? entry : entry.translation;
      return typeof translation === "string" ? [[term, translation]] : [];
    }),
  );
}

function patternTranslations(prompt: string, rules: PatternRule[]): Record<string, string> {
  const translations: Record<string, string> = {};
  for (const rule of rules) {
    if (!rule.pattern || !rule.translation) continue;
    const flags = rule.flags?.includes("g") ? rule.flags : `${rule.flags ?? "i"}g`;
    for (const match of prompt.matchAll(new RegExp(rule.pattern, flags))) {
      let translation = rule.translation;
      for (let index = 1; index < match.length; index += 1) {
        translation = translation.replaceAll(`{${index}}`, match[index] ?? "");
      }
      translations[match[0]] = translation;
    }
  }
  return translations;
}

function relevantGlossary(
  prompt: string,
  official: OfficialGlossary,
  curated: Record<string, string>,
): Record<string, string> {
  const merged = new Map<string, [string, string]>();
  for (const layer of [official.legacy, official.active]) {
    for (const [term, translation] of Object.entries(layerTranslations(layer))) {
      merged.set(normalizeTerm(term), [term, translation]);
    }
  }
  for (const [term, translation] of Object.entries(
    patternTranslations(prompt, official.patterns),
  )) {
    merged.set(normalizeTerm(term), [term, translation]);
  }
  for (const [term, translation] of Object.entries(curated)) {
    merged.set(normalizeTerm(term), [term, translation]);
  }
  return Object.fromEntries(
    [...merged.values()].filter(([term]) => promptContainsTerm(prompt, term)),
  );
}

export default function y2bExtension(pi: PiApi) {
  pi.on("before_agent_start", async (event) => {
    const policyPath = process.env.Y2B_PI_POLICY_PATH;
    if (!policyPath) throw new Error("Y2B_PI_POLICY_PATH is required");
    const policy: Policy = JSON.parse(fs.readFileSync(policyPath, "utf8"));
    const glossary = relevantGlossary(
      event.prompt,
      loadOfficialGlossary(policyPath),
      policy.glossary ?? {},
    );
    const runtimePolicy = { ...policy, glossary };
    return {
      systemPrompt: `You are the deterministic subtitle language engine for y2b-rs.
You receive exactly one JSON object from the caller. Never call tools. Never explain your work.
Return exactly one JSON object without Markdown fences, prose, comments, or reasoning.

Task segment:
- Input: {"task":"segment","source_lang":"en","core_start":0,"preferred_end":99,"tokens":[{"i":0,"start":1.2,"end":2.8,"text":"..."}]}
- Output: {"ranges":[{"start":0,"end":3}]}
- start/end are inclusive zero-based indices local to this input.
- Ranges must be ordered, non-overlapping, contiguous, and cover every token exactly once.
- core_start and preferred_end are optional adaptive batching hints. Tokens before core_start are left context. Prefer a natural range boundary at or near preferred_end while still segmenting the complete input; tokens after preferred_end provide right context.
- The following are mandatory segmentation rules, in priority order:
  1. End the current range whenever the gap before the next token is 0.8 seconds or longer.
  2. Every range duration, tokens[end].end - tokens[start].start, must be at most 8.0 seconds. Prefer 1.0-6.0 seconds.
  3. Keep the combined English text short enough for one subtitle line: at most 72 characters and at most 16 whitespace-delimited words whenever the input token boundaries allow it.
- Prefer semantic sentence boundaries only after satisfying the timing and line-length rules.
- If one input token alone exceeds a timing or line-length target, return that token as its own range; never omit or edit it.
- Never invent, drop, reorder, or edit source tokens.

Task translate:
- Input: {"task":"translate","source_lang":"en","target_lang":"zh-CN","items":[{"i":0,"text":"..."}]}
- Output: {"translations":[{"i":0,"text":"译文"}]}
- Return every i exactly once and in input order. Never merge or split items.
- Use natural concise Simplified Chinese suitable for Bilibili, not documentary-style Chinese.
- Keep each translation suitable for one visual line, targeting at most 32 Chinese-width characters when possible and never exceeding 64 Chinese-width characters. 中文宽度计法：中文/全角字符按 2，ASCII 字母数字与空格按 1（32 宽度 ≈ 16 个汉字，64 宽度 ≈ 32 个汉字）。Shorten syntax without dropping facts, jokes, names, numbers, or intent.
- Drop meaningless English filler words (uh, um, so, yeah, you know, I mean, like, right) when they add no meaning; never drop facts, jokes, names, numbers, or intent.
- Preserve code, API names, numbers, usernames, game terminology, and proper nouns accurately.
- Do not add notes or punctuation that is absent unless natural Chinese readability requires it.
- If the input contains a "feedback" field: the previous output was rejected for the given reason (typically invalid JSON or index mismatch). Fix exactly that issue and re-output only the JSON.

Task glossary_audit:
- Input: {"task":"glossary_audit","source_lang":"en","target_lang":"zh-CN","items":["term 1","term 2"]}
- Output: {"translations":["译文1","译文2"]}
- Return exactly one translation for every input item, preserving input order and array length.
- Translate each item independently as an isolated Brawl Stars term. Apply the project glossary and all Task translate terminology rules.
- Never add indices, explanations, Markdown, or fields other than translations.

Task publish_metadata:
- Input: {"task":"publish_metadata","transfer_mode":"direct|translated","youtube":{"title":"...","description":"...","url":"...","uploader":"...","published_date":"YYYY-MM-DD"},"subtitle_sampling":{"sampled":false,"total":0,"included":0},"subtitles":[{"i":0,"start":0.0,"end":2.0,"source":"...","translation":"..."}]}
- Output exactly: {"title":"...","dynamic":"...","tags":["荒野乱斗","..."]}
- Generate all three fields together from this one input. Do not output tid or any additional fields.
- title must be a faithful, natural Chinese translation of the YouTube title. Do not add a prefix, source, uploader, hashtags, emoji, or marketing language. If the YouTube title itself contains hashtags (#bs、＃brawlstars) or links, drop them entirely instead of translating or copying them — Bilibili 标题不允许出现 # 或链接。If the whole YouTube title is nothing but hashtags (例如 “#sync”), write a short Chinese title from the description or subtitles instead; when there is nothing else to work with, use the hashtag words themselves as plain text without the # sign. Preserve names, game terminology, and numbers. Maximum 70 Chinese-width characters. 中文宽度计法：中文/全角字符计 2，ASCII 字母数字与空格计 1（70 宽度 ≈ 35 个汉字）。若直译超限，必须主动删减副标题、系列后缀（如年份、“第2078集”等）和冗余修饰以满足限制，绝不允许超宽。
- dynamic must be one paragraph of 1-2 concise Chinese sentences describing the video's concrete highlights. Maximum 120 Chinese-width characters. 中文宽度计法：中文/全角字符计 2，ASCII 字母数字与空格计 1（120 宽度 ≈ 60 个汉字）。若超限，必须删减冗余修饰和套话，绝不允许超宽。Do not include a source URL, source attribution, hashtags, emoji, calls to follow/like/coin/subscribe/share, or generic promotional filler.
- tags must contain 1-4 concrete Chinese tags. “荒野乱斗” must be the first item. Add at most 3 specific topic tags supported by the title, description, or subtitles. Do not prefix tags with # and do not use commas inside a tag. Each tag must be at most 20 characters.
- For transfer_mode=direct, rely only on the YouTube metadata; subtitles will be empty.
- For transfer_mode=translated, use the bilingual subtitles to identify the actual content. subtitle_sampling.sampled=true means the caller retained the beginning/end and uniformly sampled the rest; do not infer unsupported details from omitted sections.
- If the input contains a "feedback" field: the previous output was rejected for the reason given (for example 标题宽度 79 超过上限 70). Fix exactly that issue (typically by shortening the title/dynamic) and output a corrected, complete JSON with the same fields.

Project policy and glossary:
${JSON.stringify(runtimePolicy, null, 2)}`,
    };
  });
}
