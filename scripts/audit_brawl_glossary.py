#!/usr/bin/env python3
"""Audit Pi models against Brawl Stars EN -> Simplified Chinese terminology."""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import dataclasses
import json
import pathlib
import re
import shlex
import subprocess
import time
import unicodedata
import urllib.parse
import urllib.request
from typing import Any


API_ROOT = "https://api.brawlapi.com"
USER_AGENT = "y2b-rs-glossary-audit/1.0"


@dataclasses.dataclass(frozen=True)
class Term:
    source: str
    target: str
    tid: str
    categories: tuple[str, ...]


def fetch_json(url: str, attempts: int = 3) -> dict[str, Any]:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=45) as response:
                return json.load(response)
        except Exception as exc:  # noqa: BLE001 - preserve the final network error
            error = exc
            if attempt + 1 < attempts:
                time.sleep(1 + attempt)
    assert error is not None
    raise error


def raw_csv(path: str) -> dict[str, Any]:
    quoted = urllib.parse.quote(path, safe="/")
    return fetch_json(f"{API_ROOT}/v2/raw/{quoted}")


def normalize(value: str) -> str:
    value = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", value.strip()).casefold()


def is_term_like(source: str, target: str) -> bool:
    if not source or not target or normalize(source) == normalize(target):
        return False
    if any(marker in source or marker in target for marker in ("<", ">", "\\n", "\n")):
        return False
    if len(source) > 80 or len(target) > 48:
        return False
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9+'’&./-]*", source)
    if not 1 <= len(words) <= 8:
        return False
    if re.fullmatch(r"[\d\s.,:+%/-]+", source):
        return False
    if re.search(r"[.!?;:]\s*$", source):
        return False
    return True


def iter_rows(data: Any) -> list[dict[str, Any]]:
    rows = data.values() if isinstance(data, dict) else data
    return [row for row in rows if isinstance(row, dict)]


def scan_tid_references(path: str) -> tuple[str, dict[str, set[str]]]:
    columns: dict[str, set[str]] = collections.defaultdict(set)
    for row in iter_rows(raw_csv(path)["data"]):
        for column, value in row.items():
            values = value if isinstance(value, list) else [value]
            for item in values:
                if isinstance(item, str) and item.startswith("TID_"):
                    columns[column].add(item)
    return path, columns


def is_terminology_reference(path: str, column: str) -> bool:
    sentence_files = {
        "csv_client/hints",
        "csv_client/local_notifications",
        "csv_client/oddity_shop_reactions",
        "csv_client/tutorial",
        "csv_logic/messages",
    }
    if path in sentence_files:
        return False
    lowered = column.casefold()
    if any(
        marker in lowered
        for marker in (
            "desc",
            "description",
            "info",
            "text",
            "tooltip",
            "message",
            "notification",
            "warning",
            "difference",
            "powernumber",
            "target",
            "missing",
            "requires",
            "speech",
            "intro",
        )
    ):
        return False
    if column in {
        "TID",
        "ShopTID",
        "PluralTID",
        "TierTID",
        "StageTID",
        "RewardTID",
    }:
        return True
    return any(
        marker in lowered
        for marker in ("name", "title", "label", "rank", "mode", "section", "item")
    )


def extract_terms(workers: int) -> tuple[list[Term], dict[str, Any]]:
    index = fetch_json(f"{API_ROOT}/game")
    paths = sorted(
        path
        for path in index
        if path.startswith(("csv_logic/", "csv_client/")) and not path.endswith(" 2")
    )

    references: dict[str, set[tuple[str, str]]] = collections.defaultdict(set)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for path, columns in executor.map(scan_tid_references, paths):
            for column, tids in columns.items():
                if not is_terminology_reference(path, column):
                    continue
                for tid in tids:
                    references[tid].add((path, column))

    english_doc = raw_csv("localization/texts")
    chinese_doc = raw_csv("localization/cn")
    patch_doc = raw_csv("localization/texts_patch")
    english = english_doc["data"]
    chinese = chinese_doc["data"]
    patch = patch_doc["data"]

    pairs: dict[str, list[str]] = {
        tid: [row.get("EN", ""), (chinese.get(tid) or {}).get("CN", "")]
        for tid, row in english.items()
    }
    for tid, row in patch.items():
        if row.get("EN"):
            pairs.setdefault(tid, ["", ""])[0] = row["EN"]
        if row.get("CN"):
            pairs.setdefault(tid, ["", ""])[1] = row["CN"]

    grouped: dict[str, list[Term]] = collections.defaultdict(list)
    raw_term_rows = 0
    for tid, sources in references.items():
        source, target = pairs.get(tid, ("", ""))
        source = (source or "").strip()
        target = (target or "").strip()
        if not is_term_like(source, target):
            continue
        raw_term_rows += 1
        categories = tuple(sorted({path for path, _ in sources}))
        grouped[normalize(source)].append(Term(source, target, tid, categories))

    terms: list[Term] = []
    ambiguous: list[dict[str, Any]] = []
    for normalized_source, rows in grouped.items():
        targets = {normalize(row.target) for row in rows}
        if len(targets) != 1:
            ambiguous.append(
                {
                    "source": normalized_source,
                    "variants": sorted({row.target for row in rows}),
                    "tids": sorted({row.tid for row in rows}),
                }
            )
            continue
        representative = sorted(rows, key=lambda row: (row.source.casefold(), row.tid))[0]
        categories = tuple(sorted({category for row in rows for category in row.categories}))
        terms.append(
            Term(
                representative.source,
                representative.target,
                representative.tid,
                categories,
            )
        )

    terms.sort(key=lambda term: (term.source.casefold(), term.tid))
    metadata = {
        "api_root": API_ROOT,
        "localization_generated_at": english_doc["metadata"]["generatedAt"],
        "localization_rows": len(pairs),
        "patch_rows": len(patch),
        "game_csv_paths": len(paths),
        "referenced_tids": len(references),
        "raw_term_rows": raw_term_rows,
        "unique_english": len(grouped),
        "unambiguous_terms": len(terms),
        "ambiguous_terms": len(ambiguous),
        "ambiguous": ambiguous,
    }
    return terms, metadata


def parse_pi_output(stdout: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    final_text: str | None = None
    usage: dict[str, Any] | None = None
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") != "message_end":
            continue
        message = event.get("message") or {}
        if message.get("role") != "assistant":
            continue
        texts = [
            block.get("text", "")
            for block in message.get("content", [])
            if block.get("type") == "text"
        ]
        if texts:
            final_text = texts[-1]
        usage = message.get("usage") or usage
    if final_text is None:
        raise RuntimeError("Pi output did not contain a final assistant text message")
    value = json.loads(final_text)
    if not isinstance(value, dict):
        raise RuntimeError("Pi final response is not a JSON object")
    return value, usage


def call_pi(
    server: str,
    model: str,
    terms: list[Term],
    timeout: int,
    extension: str,
    policy: str,
) -> tuple[list[str], dict[str, Any] | None, float]:
    payload = json.dumps(
        {
            "task": "glossary_audit",
            "source_lang": "en",
            "target_lang": "zh-CN",
            "items": [term.source for term in terms],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    args = [
        "env",
        f"Y2B_PI_POLICY_PATH={policy}",
        "/usr/local/bin/pi",
        "--mode",
        "json",
        "--print",
        "--no-session",
        "--no-tools",
        "--no-skills",
        "--no-context-files",
        "--no-prompt-templates",
        "--no-extensions",
        "--extension",
        extension,
        "--provider",
        "openai-codex",
        "--model",
        model,
        "--thinking",
        "high",
        "--no-approve",
        payload,
    ]
    command = ["ssh", "-o", "BatchMode=yes", server, shlex.join(args)]
    started = time.monotonic()
    process = subprocess.run(
        command,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed = time.monotonic() - started
    if process.returncode != 0:
        raise RuntimeError(
            f"Pi exited {process.returncode}: {process.stderr[-1000:]}"
        )
    value, usage = parse_pi_output(process.stdout)
    translations = value.get("translations")
    if not isinstance(translations, list):
        raise RuntimeError("Pi response has no translations array")
    if len(translations) != len(terms) or not all(
        isinstance(row, str) for row in translations
    ):
        raise RuntimeError(
            f"Pi response length/type mismatch: expected={len(terms)} "
            f"actual={len(translations)}"
        )
    return translations, usage, elapsed


def merge_usage(total: dict[str, float], usage: dict[str, Any] | None) -> None:
    if not usage:
        return
    for key in (
        "input",
        "output",
        "reasoning",
        "cacheRead",
        "cacheWrite",
        "totalTokens",
    ):
        value = usage.get(key)
        if isinstance(value, (int, float)):
            total[key] += value
    cost = usage.get("cost") or {}
    if isinstance(cost.get("total"), (int, float)):
        total["cost"] += cost["total"]


def test_batch(
    server: str,
    model: str,
    terms: list[Term],
    timeout: int,
    extension: str,
    policy: str,
    attempts: int = 2,
) -> tuple[list[str], dict[str, Any] | None, float]:
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            return call_pi(server, model, terms, timeout, extension, policy)
        except Exception as exc:  # noqa: BLE001 - retry protocol/provider failures
            error = exc
            if attempt + 1 < attempts:
                time.sleep(2)
    assert error is not None
    if len(terms) <= 1:
        print(
            f"{model} term failed after retries: {terms[0].source!r}: {error}",
            flush=True,
        )
        return [""], None, float(timeout)
    midpoint = len(terms) // 2
    left, left_usage, left_elapsed = test_batch(
        server, model, terms[:midpoint], timeout, extension, policy, attempts=1
    )
    right, right_usage, right_elapsed = test_batch(
        server, model, terms[midpoint:], timeout, extension, policy, attempts=1
    )
    usage: dict[str, float] = collections.defaultdict(float)
    merge_usage(usage, left_usage)
    merge_usage(usage, right_usage)
    synthetic = {
        "input": usage["input"],
        "output": usage["output"],
        "reasoning": usage["reasoning"],
        "cacheRead": usage["cacheRead"],
        "cacheWrite": usage["cacheWrite"],
        "totalTokens": usage["totalTokens"],
        "cost": {"total": usage["cost"]},
    }
    return left + right, synthetic, left_elapsed + right_elapsed


def write_report(path: pathlib.Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="root@157.230.241.109")
    parser.add_argument(
        "--models",
        default="gpt-5.6-luna,gpt-5.6-sol,gpt-5.6-terra",
        help="Comma-separated Pi model names; empty means extraction only",
    )
    parser.add_argument("--batch-size", type=int, default=300)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--extension", default="/opt/y2b/pi/y2b-extension.ts")
    parser.add_argument("--policy", default="/opt/y2b/pi/audit-policy.json")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--game-version", default="68.250")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument(
        "--terms-file",
        type=pathlib.Path,
        help="Reuse terms and source metadata from a previous extraction report",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=pathlib.Path("/tmp/y2b-brawl-glossary-audit.json"),
    )
    args = parser.parse_args()
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        parser.error("--shard-index must be within [0, --shard-count)")

    if args.terms_file:
        extracted = json.loads(args.terms_file.read_text(encoding="utf-8"))
        terms = [
            Term(
                source=row["source"],
                target=row["target"],
                tid=row["tid"],
                categories=tuple(row["categories"]),
            )
            for row in extracted["terms"]
        ]
        source_metadata = extracted["source"]
        print(f"loaded {len(terms)} extracted terms", flush=True)
    else:
        print("extracting official terminology", flush=True)
        terms, source_metadata = extract_terms(args.workers)
        print(
            f"extracted {len(terms)} unambiguous terms "
            f"({source_metadata['ambiguous_terms']} ambiguous excluded)",
            flush=True,
        )
    if args.shard_count > 1:
        terms = terms[args.shard_index :: args.shard_count]
        source_metadata = {
            **source_metadata,
            "audit_shard_index": args.shard_index,
            "audit_shard_count": args.shard_count,
        }
        print(
            f"using audit shard {args.shard_index + 1}/{args.shard_count}: "
            f"{len(terms)} terms",
            flush=True,
        )
    if args.resume and args.output.exists():
        report = json.loads(args.output.read_text(encoding="utf-8"))
        previous_sources = [row["source"] for row in report.get("terms", [])]
        if previous_sources != [term.source for term in terms]:
            raise RuntimeError("resume report terminology does not match this extraction")
    else:
        report = {
            "game_version": args.game_version,
            "source": source_metadata,
            "terms": [dataclasses.asdict(term) for term in terms],
            "models": {},
            "suggested_glossary": {},
        }
    write_report(args.output, report)

    models = [model.strip() for model in args.models.split(",") if model.strip()]
    suggested: dict[str, str] = dict(report.get("suggested_glossary", {}))
    for model in models:
        existing = report["models"].get(model, {})
        if existing.get("failures") is not None:
            print(f"{model} already complete; skipping", flush=True)
            continue
        outputs: list[str] = list(existing.get("partial_outputs", []))
        usage_total: dict[str, float] = collections.defaultdict(
            float, existing.get("partial_usage", {})
        )
        wall_seconds = float(existing.get("partial_wall_seconds", 0.0))
        if outputs:
            print(f"{model} resuming at {len(outputs)}/{len(terms)}", flush=True)
        batches = (len(terms) + args.batch_size - 1) // args.batch_size
        starts = range(len(outputs), len(terms), args.batch_size)
        for start in starts:
            batch_number = start // args.batch_size + 1
            batch = terms[start : start + args.batch_size]
            translations, usage, elapsed = test_batch(
                args.server,
                model,
                batch,
                args.timeout,
                args.extension,
                args.policy,
            )
            outputs.extend(translations)
            merge_usage(usage_total, usage)
            wall_seconds += elapsed
            current_correct = sum(
                normalize(actual) == normalize(term.target)
                for term, actual in zip(terms[: len(outputs)], outputs, strict=True)
            )
            print(
                f"{model} batch {batch_number}/{batches}: "
                f"{current_correct}/{len(outputs)} exact, {elapsed:.1f}s",
                flush=True,
            )
            report["models"][model] = {
                "partial_outputs": outputs,
                "partial_usage": dict(usage_total),
                "partial_wall_seconds": wall_seconds,
            }
            write_report(args.output, report)

        failures = []
        for term, actual in zip(terms, outputs, strict=True):
            if normalize(actual) == normalize(term.target):
                continue
            failure = {
                "source": term.source,
                "expected": term.target,
                "actual": actual,
                "tid": term.tid,
                "categories": list(term.categories),
            }
            failures.append(failure)
            suggested[term.source] = term.target
        result = {
            "total": len(terms),
            "exact": len(terms) - len(failures),
            "accuracy": (len(terms) - len(failures)) / len(terms),
            "wall_seconds": wall_seconds,
            "usage": dict(usage_total),
            "failures": failures,
        }
        report["models"][model] = result
        report["suggested_glossary"] = dict(
            sorted(suggested.items(), key=lambda item: item[0].casefold())
        )
        write_report(args.output, report)
        print(
            f"{model} complete: {result['exact']}/{result['total']} "
            f"({result['accuracy']:.2%}), failures={len(failures)}",
            flush=True,
        )

    print(
        f"union failures={len(suggested)} report={args.output}", flush=True
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
