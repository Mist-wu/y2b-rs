import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"


class CiContractTests(unittest.TestCase):
    def test_quality_workflow_covers_every_required_gate(self) -> None:
        quality = (WORKFLOWS / "quality-gates.yml").read_text(encoding="utf-8")
        secret_scan = (WORKFLOWS / "secret-scan.yml").read_text(encoding="utf-8")
        required_commands = (
            "cargo fmt --check",
            "cargo clippy --all-targets --all-features -- -D warnings",
            "cargo test",
            "npm run check",
            "python3 -m unittest discover -s scripts",
            "python3 -m unittest discover -s deploy/tests",
            "python3 -m compileall -q scripts deploy",
            "shellcheck deploy/*.sh",
            "bash -n deploy/*.sh",
            "npm audit",
            "cargo audit",
        )
        for command in required_commands:
            with self.subTest(command=command):
                self.assertIn(command, quality)
        self.assertIn("gitleaks/gitleaks-action@", secret_scan)

    def test_every_action_is_pinned_to_a_commit(self) -> None:
        action_pattern = re.compile(r"^\s*uses:\s*([^\s@]+)@([^\s#]+)", re.MULTILINE)
        workflow_files = sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml"))
        self.assertTrue(workflow_files)
        for workflow in workflow_files:
            for action, revision in action_pattern.findall(workflow.read_text(encoding="utf-8")):
                with self.subTest(workflow=workflow.name, action=action):
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
        secret_scan = (WORKFLOWS / "secret-scan.yml").read_text(encoding="utf-8")
        self.assertIn("cp .github/workflows/gitleaksignore .gitleaksignore", secret_scan)


if __name__ == "__main__":
    unittest.main()
