#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("query_path_ids.py")
SPEC = importlib.util.spec_from_file_location("query_path_ids", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
query_path_ids = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = query_path_ids
SPEC.loader.exec_module(query_path_ids)


CFG = "scope=feasible;min_volume_mean=0.0;deduplicate=true;keep_singular=true;keep_nonasymptotic=false;compute_volume=false;motif_zero_tol=1.0e-6"
CFG_CUSTOM = "scope=all;min_volume_mean=0.25;deduplicate=false;keep_singular=true;keep_nonasymptotic=true;compute_volume=true;motif_zero_tol=1.0e-4"


class StablePathIdTests(unittest.TestCase):
    def test_input_selector_roundtrip(self) -> None:
        path_id = query_path_ids.encode_path_id(
            network_id="[1]+[2]<->[1,2]",
            selector_kind="input",
            selector_value="tA",
            output_symbol="A",
            cfg_signature=CFG,
            path_idx=2,
        )
        self.assertTrue(path_id.startswith("p3."))
        decoded = query_path_ids.decode_path_id(path_id)
        self.assertEqual(decoded["network_id"], "[1]+[2]<->[1,2]")
        self.assertEqual(decoded["selector_kind"], "input")
        self.assertEqual(decoded["selector_value"], "tA")
        self.assertEqual(decoded["output_symbol"], "A")
        self.assertEqual(decoded["cfg_signature"], CFG)
        self.assertEqual(decoded["path_idx"], 2)

    def test_change_selector_roundtrip(self) -> None:
        path_id = query_path_ids.encode_path_id(
            network_id="[1]+[1]<->[1,1]|[1,1]+[2]<->[1,1,2]",
            selector_kind="change",
            selector_value="orthant(+tA,-tB)",
            output_symbol="C_A_A_B",
            cfg_signature=CFG_CUSTOM,
            path_idx=17,
        )
        decoded = query_path_ids.decode_path_id(path_id)
        self.assertEqual(decoded["network_id"], "[1]+[1]<->[1,1]|[1,1]+[2]<->[1,1,2]")
        self.assertEqual(decoded["selector_kind"], "change")
        self.assertEqual(decoded["selector_value"], "orthant(+tA,-tB)")
        self.assertEqual(decoded["output_symbol"], "C_A_A_B")
        self.assertEqual(decoded["cfg_signature"], CFG_CUSTOM)
        self.assertEqual(decoded["path_idx"], 17)

    def test_behavior_code_roundtrip(self) -> None:
        behavior_code = query_path_ids.encode_behavior_code(["+1", "+Inf", "+1"])
        decoded = query_path_ids.decode_behavior_code(behavior_code)
        self.assertEqual(decoded["output_order_tokens"], ["+1", "+Inf", "+1"])
        self.assertEqual(decoded["path_length"], 3)
        self.assertEqual(decoded["exact_label"], "+1 -> +Inf -> +1")
        self.assertEqual(decoded["motif_label"], "multistage_activation_with_singular_transition")

    def test_fractional_behavior_code_roundtrip(self) -> None:
        behavior_code = query_path_ids.encode_behavior_code(["+0.5", "-1.25", "0"])
        decoded = query_path_ids.decode_behavior_code(behavior_code)
        self.assertEqual(decoded["output_order_tokens"], ["+0.5", "-1.25", "0"])
        self.assertEqual(decoded["motif_profile"], ["+", "-", "0"])
        self.assertEqual(decoded["motif_label"], "biphasic_peak")

    def test_vector_behavior_code_roundtrip(self) -> None:
        behavior_code = query_path_ids.encode_behavior_code(["(+0.5,0)", "(+1,-1.25)"])
        decoded = query_path_ids.decode_behavior_code(behavior_code)
        self.assertEqual(decoded["output_order_tokens"], ["(+0.5,0)", "(+1,-1.25)"])
        self.assertEqual(decoded["motif_profile"], ["(+,0)", "(+,-)"])
        self.assertEqual(decoded["motif_label"], "vector_motif::(+,0) -> (+,-)")

    def test_text_behavior_code_roundtrip(self) -> None:
        behavior_code = query_path_ids.encode_behavior_code(["high_nullity(n=2)"])
        decoded = query_path_ids.decode_behavior_code(behavior_code)
        self.assertEqual(decoded["output_order_tokens"], ["high_nullity(n=2)"])


if __name__ == "__main__":
    unittest.main()
