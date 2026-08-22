from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "y2b-set-deepseek-key.py"
SPEC = importlib.util.spec_from_file_location("y2b_set_deepseek_key", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CredentialTopologyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.auth_dir = self.root / "dedicated"
        self.auth_file = self.auth_dir / "auth.json"
        self.global_auth = self.root / "global-auth.json"
        self.env_file = self.root / "y2b.env"
        self.uid = os.getuid()
        self.gid = os.getgid()
        self.global_auth.write_text(
            json.dumps(
                {
                    "openai": {"type": "oauth", "access": "preserve-me"},
                    "deepseek": {"type": "api_key", "key": "sk-old-key-123456789012345"},
                }
            ),
            encoding="utf-8",
        )
        self.global_auth.chmod(0o600)
        self.env_file.write_text(
            "YOUTUBE_API_KEY=preserve-me\nDEEPSEEK_API_KEY=sk-old-key-123456789012345\n",
            encoding="utf-8",
        )
        self.env_file.chmod(0o600)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def install(self, key: str = "sk-new-key-123456789012345") -> str:
        return MODULE.install_key(
            key,
            self.auth_dir,
            self.auth_file,
            self.global_auth,
            self.env_file,
            self.uid,
            self.gid,
        )

    def test_install_creates_single_deepseek_path_and_preserves_other_credentials(self) -> None:
        digest = self.install()

        self.assertEqual(len(digest), 16)
        self.assertEqual(
            json.loads(self.auth_file.read_text(encoding="utf-8")),
            {"deepseek": {"type": "api_key", "key": "sk-new-key-123456789012345"}},
        )
        self.assertEqual(
            json.loads(self.global_auth.read_text(encoding="utf-8")),
            {"openai": {"type": "oauth", "access": "preserve-me"}},
        )
        self.assertEqual(
            self.env_file.read_text(encoding="utf-8"),
            "YOUTUBE_API_KEY=preserve-me\n",
        )
        self.assertEqual(self.auth_dir.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.auth_file.stat().st_mode & 0o777, 0o600)

    def test_check_rejects_legacy_environment_copy(self) -> None:
        self.install()
        with self.env_file.open("a", encoding="utf-8") as handle:
            handle.write("export DEEPSEEK_API_KEY=sk-duplicate-123456789012345\n")

        with self.assertRaisesRegex(MODULE.TopologyError, "environment file"):
            MODULE.check_topology(
                self.auth_dir,
                self.auth_file,
                self.global_auth,
                self.env_file,
                self.uid,
                self.gid,
            )

    def test_invalid_key_changes_nothing(self) -> None:
        before_global = self.global_auth.read_bytes()
        before_env = self.env_file.read_bytes()

        with self.assertRaisesRegex(MODULE.TopologyError, "invalid"):
            self.install("not-a-key")

        self.assertFalse(self.auth_file.exists())
        self.assertEqual(self.global_auth.read_bytes(), before_global)
        self.assertEqual(self.env_file.read_bytes(), before_env)

    def test_dedicated_auth_rejects_additional_provider(self) -> None:
        self.install()
        document = json.loads(self.auth_file.read_text(encoding="utf-8"))
        document["openai"] = {"type": "api_key", "key": "not-relevant"}
        self.auth_file.write_text(json.dumps(document), encoding="utf-8")

        with self.assertRaisesRegex(MODULE.TopologyError, "only the deepseek"):
            MODULE.check_topology(
                self.auth_dir,
                self.auth_file,
                self.global_auth,
                self.env_file,
                self.uid,
                self.gid,
            )


if __name__ == "__main__":
    unittest.main()
