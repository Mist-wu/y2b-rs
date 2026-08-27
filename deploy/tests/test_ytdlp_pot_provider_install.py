from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy" / "install-ytdlp-pot-provider.sh"


class YtDlpPotProviderInstallTests(unittest.TestCase):
    def test_installer_has_valid_shell_syntax(self) -> None:
        subprocess.run(["bash", "-n", str(INSTALLER)], check=True)

    def test_upstream_artifacts_are_versioned_and_digest_pinned(self) -> None:
        source = INSTALLER.read_text()
        version = re.search(r"^provider_version=(\S+)$", source, re.MULTILINE)
        self.assertIsNotNone(version)
        self.assertIn(f"/download/$provider_version/", source)
        self.assertIn("/refs/tags/$provider_version.tar.gz", source)
        digests = re.findall(r"^(?:plugin|source)_sha256=([0-9a-f]{64})$", source, re.MULTILINE)
        self.assertEqual(len(digests), 2)
        self.assertIn("sha256sum -c -", source)

    def test_install_is_portless_and_does_not_restart_y2b(self) -> None:
        source = INSTALLER.read_text()
        self.assertIn("server/build/generate_once.js", source)
        self.assertIn("mode=script-node (no listening port; y2b restart not required)", source)
        self.assertNotIn("systemctl", source)
        self.assertIn("/etc/yt-dlp/plugins", source)
        self.assertIn("/root/bgutil-ytdlp-pot-provider", source)

    def test_bootstrap_and_deploy_preserve_the_installer(self) -> None:
        bootstrap = (ROOT / "deploy" / "bootstrap-server.sh").read_text()
        deploy_app = (ROOT / "deploy" / "deploy-app.sh").read_text()
        self.assertIn("bash \"$pot_installer\"", bootstrap)
        self.assertIn("install-ytdlp-pot-provider.sh", deploy_app)


if __name__ == "__main__":
    unittest.main()
