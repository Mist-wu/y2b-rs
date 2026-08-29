import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class RepositoryHygieneTests(unittest.TestCase):
    def test_python_cache_patterns_are_ignored(self) -> None:
        patterns = set((ROOT / ".gitignore").read_text(encoding="utf-8").splitlines())
        self.assertIn("__pycache__/", patterns)
        self.assertIn("*.py[cod]", patterns)

    def test_no_python_bytecode_is_tracked(self) -> None:
        tracked = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        ).stdout.decode().split("\0")
        bytecode = [path for path in tracked if path.endswith((".pyc", ".pyo"))]
        self.assertEqual(bytecode, [])


if __name__ == "__main__":
    unittest.main()
