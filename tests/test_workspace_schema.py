import copy
import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]


class WorkspaceSchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(
            (ROOT / "schemas/workspace.schema.json").read_text(encoding="utf-8")
        )
        cls.fixture = json.loads(
            (ROOT / "tests/fixtures/workspace/valid-v1.expected-v2.json").read_text(
                encoding="utf-8"
            )
        )
        cls.validator = Draft202012Validator(cls.schema)

    def test_shared_expected_v2_fixture_satisfies_the_complete_schema(self):
        self.validator.validate(self.fixture)

    def test_v1_and_unknown_node_types_fail_the_v2_schema(self):
        v1 = copy.deepcopy(self.fixture)
        v1["version"] = 1
        self.assertTrue(list(self.validator.iter_errors(v1)))

        unknown = copy.deepcopy(self.fixture)
        unknown["nodes"][0]["type"] = "future-plugin-node"
        self.assertTrue(list(self.validator.iter_errors(unknown)))

    def test_invalid_lifecycle_state_fails_the_schema(self):
        invalid = copy.deepcopy(self.fixture)
        lifecycle_node = next(
            node for node in invalid["nodes"] if "lifecycle" in node.get("data", {})
        )
        lifecycle_node["data"]["lifecycle"]["freshness"] = "fresh-enough"
        self.assertTrue(list(self.validator.iter_errors(invalid)))


if __name__ == "__main__":
    unittest.main()
