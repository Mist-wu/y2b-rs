import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class DocumentationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.readme = (ROOT / "README.md").read_text(encoding="utf-8")
        cls.discovery = (ROOT / "DISCOVERY_REARCHITECTURE.md").read_text(
            encoding="utf-8"
        )

    def test_readme_describes_current_operational_safety_contract(self) -> None:
        required = (
            "## 强失败质量门禁",
            "maintenance hold",
            "原子 release",
            "schema v19",
            "弱证据",
            "不是“翻译压制”",
            "npm audit --audit-level=high",
            "cargo audit --no-yanked",
            "unmaintained、yanked、unsound 等 warning",
        )
        for phrase in required:
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.readme)
        self.assertIn("应用 release 当前不是原子切换", self.readme)
        self.assertIn("版本化 `releases/<version>`", self.readme)
        self.assertIn("恢复旧数据库及原 service 状态并返回非零", self.readme)

    def test_recovery_installs_a_complete_release_before_restoring_data(self) -> None:
        recovery = self.readme.split("## 备份与恢复", 1)[1]
        deploy_position = recovery.index("先用 `deploy-app.sh` 安装")
        restore_position = recovery.index("`deploy/restore.sh BACKUP.db`")
        self.assertLess(deploy_position, restore_position)
        self.assertIn("不能只替换数据库或二进制", recovery)

    def test_discovery_document_is_clearly_historical_and_points_to_v19(self) -> None:
        self.assertIn("（历史设计文档）", self.discovery.splitlines()[0])
        self.assertIn("当前 schema 为 **v19**", self.discovery)
        self.assertNotIn("确认部署提交位于 `feat/discovery-rearchitecture`", self.discovery)
        self.assertNotIn("确认 schema version 为 15", self.discovery)

    def test_repository_contains_the_full_mit_license(self) -> None:
        license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")
        self.assertTrue(license_text.startswith("MIT License\n\n"))
        self.assertIn("Copyright (c) 2026 Mist-wu", license_text)
        self.assertIn("Permission is hereby granted, free of charge", license_text)
        self.assertIn('THE SOFTWARE IS PROVIDED "AS IS"', license_text)
        self.assertIn(
            "[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)",
            self.readme,
        )


if __name__ == "__main__":
    unittest.main()
