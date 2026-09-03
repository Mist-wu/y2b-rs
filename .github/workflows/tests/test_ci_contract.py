import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"
CI = WORKFLOWS / "ci.yml"


class CiContractTests(unittest.TestCase):
    def test_single_workflow_covers_every_required_gate(self) -> None:
        self.assertEqual(sorted(p.name for p in WORKFLOWS.glob("*.y*ml")), ["ci.yml"])
        ci = CI.read_text(encoding="utf-8")
        required_commands = (
            "cargo fmt --check",
            "cargo clippy --all-targets --all-features -- -D warnings",
            "cargo test --all-features",
            "npm run check",
            "python3 -m unittest discover -s scripts",
            "python3 -m unittest discover -s deploy/tests",
            "python3 -m unittest discover -s .github/workflows/tests",
            "python3 -m compileall -q scripts deploy",
            "shellcheck deploy/*.sh",
            "bash -n deploy/*.sh",
            "npm audit --audit-level=high",
            "cargo audit",
            "gitleaks/gitleaks-action@",
        )
        for command in required_commands:
            with self.subTest(command=command):
                self.assertIn(command, ci)
        # 任何门禁都不得降级：不允许 continue-on-error 或 `|| true`。
        self.assertNotIn("continue-on-error", ci)
        self.assertNotIn("|| true", ci)

    def test_triggers_do_not_duplicate_pull_request_runs(self) -> None:
        ci = CI.read_text(encoding="utf-8")
        self.assertRegex(ci, r"(?m)^  push:\n    branches:\n      - main$")
        self.assertIn("  pull_request:", ci)
        self.assertIn("concurrency:", ci)
        self.assertIn("github.head_ref || github.ref", ci)
        self.assertIn("cancel-in-progress: true", ci)

    def test_every_action_is_pinned_to_a_commit(self) -> None:
        action_pattern = re.compile(r"^\s*-?\s*uses:\s*([^\s@]+)@([^\s#]+)", re.MULTILINE)
        actions = action_pattern.findall(CI.read_text(encoding="utf-8"))
        self.assertTrue(actions)
        for action, revision in actions:
            with self.subTest(action=action):
                self.assertRegex(revision, r"^[0-9a-f]{40}$")

    def test_package_check_runs_type_and_parse_checks(self) -> None:
        package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
        scripts = package["scripts"]
        self.assertIn("npm run typecheck", scripts["check"])
        self.assertIn("npm run check:extension", scripts["check"])
        self.assertIn("await import", scripts["check:extension"])
        self.assertIn("pi/y2b-extension.ts", scripts["check:extension"])

    def test_gitleaks_allowlist_is_a_single_exact_fingerprint(self) -> None:
        allowlist = (WORKFLOWS / "gitleaksignore").read_text(encoding="utf-8")
        fingerprint = (
            "a9fb21bab9f0cd23f5711773a3c903276dda2195:"
            "deploy/tests/test_y2b_set_deepseek_key.py:generic-api-key:80"
        )
        entries = [line for line in allowlist.splitlines() if line and not line.startswith("#")]
        self.assertEqual(entries, [fingerprint])
        self.assertIn("cp .github/workflows/gitleaksignore .gitleaksignore", CI.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
