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

    def test_shared_inverse_design_fixtures_satisfy_the_complete_schema(self):
        for name in ("inverse-design-v2.json", "inverse-design-v2.expected-restored.json"):
            with self.subTest(fixture=name):
                fixture = json.loads(
                    (ROOT / "tests/fixtures/workspace" / name).read_text(encoding="utf-8")
                )
                self.validator.validate(fixture)
                self.assertEqual(
                    [node["type"] for node in fixture["nodes"]],
                    ["inverse-design-target", "gradient-design", "designed-network", "model-builder"],
                )

    def test_inverse_design_schema_rejects_invalid_result_lifecycle(self):
        invalid = json.loads(
            (ROOT / "tests/fixtures/workspace/inverse-design-v2.expected-restored.json")
            .read_text(encoding="utf-8")
        )
        gradient = next(node for node in invalid["nodes"] if node["type"] == "gradient-design")
        gradient["data"]["lifecycle"]["state"] = "restored-current"
        self.assertTrue(list(self.validator.iter_errors(invalid)))

    def test_multidimensional_target_design_shared_fixtures_preserve_the_new_contract(self):
        target_schema = json.loads(
            (ROOT / "schemas/design-target.schema.json").read_text(encoding="utf-8")
        )
        target_validator = Draft202012Validator(target_schema)
        fixtures = []
        for name in (
            "target-design-multidimensional-v2.json",
            "target-design-multidimensional-v2.expected-restored.json",
        ):
            with self.subTest(fixture=name):
                fixture = json.loads(
                    (ROOT / "tests/fixtures/workspace" / name).read_text(encoding="utf-8")
                )
                self.validator.validate(fixture)
                fixtures.append(fixture)
                nodes = {node["id"]: node["data"] for node in fixture["nodes"]}
                target = json.loads(nodes["target"]["inverseTargetJSON"])
                target_validator.validate(target)
                self.assertEqual(target, nodes["target"]["inverseDesignRequest"]["target"])
                self.assertEqual((len(target["inputs"]), len(target["outputs"])), (2, 2))
                self.assertEqual(len(target["samples"]), 3)
                self.assertEqual(len(target["validation_samples"]), 2)
                self.assertEqual(target["samples"][1]["weight"], 2)
                chemistry = json.loads(nodes["target"]["inverseChemistryJSON"])
                self.assertEqual(chemistry["max_copies"]["Y"], 1)
                self.assertEqual(chemistry["forbidden_complexes"], ["A_B"])
                self.assertEqual(chemistry["binding_gates"][0]["requires"], {"X": 1})
                result = nodes["gradient"]["inverseDesignResult"]
                target_validator.validate(result["target"])
                self.assertEqual(result["selected_network"]["targets"], [row["outputs"] for row in target["samples"]])
                self.assertEqual(result["validation"]["targets"], [row["outputs"] for row in target["validation_samples"]])
                network = nodes["output"]["designedNetwork"]
                target_validator.validate(network["target"])
                self.assertEqual(network["totals"], result["selected_network"]["totals"])
                self.assertNotEqual(network["totals"]["A"], 1)
                self.assertNotEqual(network["totals"]["B"], 1)
                self.assertEqual(network["outputs"][0]["transform"], "log10")
                self.assertEqual(network["outputs"][0]["offset"], 0.24430986707351177)
                self.assertEqual(network["outputs"][1]["offset"], -0.1)

        source, restored = fixtures
        self.assertEqual(source["nodes"][0]["data"]["inverseTargetJSON"], restored["nodes"][0]["data"]["inverseTargetJSON"])
        self.assertEqual(source["nodes"][0]["data"]["inverseChemistryJSON"], restored["nodes"][0]["data"]["inverseChemistryJSON"])
        for node in restored["nodes"][1:3]:
            self.assertEqual(node["data"]["lifecycle"]["state"], "historical")
            self.assertEqual(node["data"]["lifecycle"]["freshness"], "historical")
            self.assertEqual(node["data"]["lifecycle"]["evidence"], node["data"]["evidence"])
        self.assertNotIn("transient-", json.dumps(restored))

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
