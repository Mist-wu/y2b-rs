import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PI_DIR = ROOT / "pi"


class PiJsonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.documents = {
            path.name: json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(PI_DIR.glob("*.json"))
        }

    def assert_non_empty_string(self, value: object) -> None:
        self.assertIsInstance(value, str)
        self.assertTrue(value.strip())

    def test_every_pi_json_file_has_an_explicit_schema_check(self) -> None:
        self.assertEqual(
            set(self.documents),
            {"audit-policy.json", "brawl-stars-glossary.json", "policy.json"},
        )

    def test_translation_policies_have_valid_structure(self) -> None:
        for filename in ("policy.json", "audit-policy.json"):
            with self.subTest(filename=filename):
                policy = self.documents[filename]
                self.assertEqual(policy["version"], 1)
                self.assert_non_empty_string(policy["target_audience"])
                self.assertIsInstance(policy["style"], list)
                self.assertTrue(policy["style"])
                for rule in policy["style"]:
                    self.assert_non_empty_string(rule)
                self.assertIsInstance(policy["glossary"], dict)
                for source, translation in policy["glossary"].items():
                    self.assert_non_empty_string(source)
                    self.assert_non_empty_string(translation)

    def test_official_glossary_counts_match_its_layers(self) -> None:
        glossary = self.documents["brawl-stars-glossary.json"]
        self.assertEqual(glossary["version"], 2)
        self.assertEqual(glossary["language_pair"], "en-CN")
        self.assert_non_empty_string(glossary["game_version"])

        selection = glossary["selection"]
        self.assertEqual(selection["active_entries"], len(glossary["active"]))
        self.assertEqual(selection["legacy_entries"], len(glossary["legacy"]))
        self.assertEqual(
            selection["pattern_collapsed_entries"]
            + selection["excluded_or_unreferenced_failures"],
            len(glossary["omitted"]),
        )
        self.assertEqual(
            selection["model_failure_inputs"],
            len(glossary["active"])
            + len(glossary["legacy"])
            + len(glossary["omitted"]),
        )
        ignored_paths = selection["ignored_reference_paths"]
        self.assertIsInstance(ignored_paths, list)
        self.assertTrue(ignored_paths)
        self.assertTrue(all(isinstance(path, str) and path for path in ignored_paths))

    def test_official_glossary_layers_have_valid_entries(self) -> None:
        glossary = self.documents["brawl-stars-glossary.json"]
        active_terms = set(glossary["active"])
        legacy_terms = set(glossary["legacy"])
        omitted_terms = set(glossary["omitted"])
        self.assertFalse(active_terms & legacy_terms)
        self.assertFalse(active_terms & omitted_terms)
        self.assertFalse(legacy_terms & omitted_terms)

        for layer_name in ("active", "legacy"):
            for term, entry in glossary[layer_name].items():
                with self.subTest(layer=layer_name, term=term):
                    self.assert_non_empty_string(term)
                    self.assertIsInstance(entry, dict)
                    self.assert_non_empty_string(entry["translation"])
                    self.assert_non_empty_string(entry["representative_tid"])
                    self.assertIsInstance(entry["sources"], list)
                    self.assertTrue(entry["sources"])
                    self.assertTrue(
                        all(isinstance(source, str) and source for source in entry["sources"])
                    )
                    self.assertIsInstance(entry["active_references"], int)
                    self.assertIsInstance(entry["disabled_references"], int)
                    self.assertGreaterEqual(entry["active_references"], 0)
                    self.assertGreaterEqual(entry["disabled_references"], 0)
                    if layer_name == "active":
                        self.assertGreater(entry["active_references"], 0)
                    else:
                        self.assertEqual(entry["active_references"], 0)
                        self.assertGreater(entry["disabled_references"], 0)

        for term, entry in glossary["omitted"].items():
            with self.subTest(layer="omitted", term=term):
                self.assert_non_empty_string(term)
                self.assert_non_empty_string(entry["translation"])
                self.assertIn(entry["reason"], {"pattern", "ignored_or_unreferenced"})

    def test_glossary_patterns_are_unique_and_compile(self) -> None:
        patterns = self.documents["brawl-stars-glossary.json"]["patterns"]
        self.assertIsInstance(patterns, list)
        self.assertTrue(patterns)
        identifiers = [rule["id"] for rule in patterns]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        for rule in patterns:
            with self.subTest(pattern=rule["id"]):
                self.assert_non_empty_string(rule["id"])
                self.assert_non_empty_string(rule["pattern"])
                self.assert_non_empty_string(rule["translation"])
                self.assertRegex(rule["flags"], r"^[gimsuy]+$")
                compiled = re.compile(
                    rule["pattern"],
                    re.IGNORECASE if "i" in rule["flags"] else 0,
                )
                placeholders = {int(index) for index in re.findall(r"\{(\d+)\}", rule["translation"])}
                self.assertTrue(all(0 < index <= compiled.groups for index in placeholders))

    def test_audit_cascade_totals_are_internally_consistent(self) -> None:
        audit = self.documents["brawl-stars-glossary.json"]["audit"]
        stages = [audit[name] for name in ("gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.6-sol")]
        for stage in stages:
            self.assertEqual(stage["tested"], stage["exact"] + stage["failures"])
        self.assertEqual(stages[1]["tested"], stages[0]["exact"])
        self.assertEqual(stages[2]["tested"], stages[1]["exact"])
        self.assertEqual(audit["passed_all_three"], stages[2]["exact"])
        self.assertEqual(audit["union_failures"], sum(stage["failures"] for stage in stages))


if __name__ == "__main__":
    unittest.main()
