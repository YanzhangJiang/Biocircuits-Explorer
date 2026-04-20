#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("atlas_id_codec.py")
SPEC = importlib.util.spec_from_file_location("atlas_id_codec", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
atlas_id_codec = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = atlas_id_codec
SPEC.loader.exec_module(atlas_id_codec)


CFG = "scope=feasible;min_volume_mean=0.0;deduplicate=true;keep_singular=true;keep_nonasymptotic=false;compute_volume=false;motif_zero_tol=1.0e-6"
NETWORK = "[1,1]+[2]<->[1,1,2]|[1]+[1]<->[1,1]"
GRAPH_INPUT = f"{NETWORK}::graph_input=tA::graphcfg=siso_v0"
GRAPH_CHANGE = f"{NETWORK}::graph_change=orthant(+tA,+tB)::graphcfg=orthant_v0"
SLICE_INPUT = f"{NETWORK}::input=tA::output=A::cfg={CFG}"
SLICE_CHANGE = f"{NETWORK}::change=orthant(+tA,+tB)::output=C_A_A::cfg={CFG}"
REGIME_ID = f"{SLICE_CHANGE}::regime::17"
TRANSITION_ID = f"{SLICE_CHANGE}::transition::17->24"
FAMILY_EXACT_ID = f"{SLICE_CHANGE}::exact::3"
FAMILY_MOTIF_ID = f"{SLICE_CHANGE}::motif::2"
PATH_ID = f"{SLICE_CHANGE}::path::9"


class AtlasIdCodecTests(unittest.TestCase):
    def test_cfg_parse_roundtrip(self) -> None:
        cfg = atlas_id_codec.ClassifierConfig.parse(CFG)
        self.assertEqual(cfg.render(), CFG)
        self.assertIs(cfg.typed_fields["deduplicate"], True)
        self.assertIs(cfg.typed_fields["keep_nonasymptotic"], False)
        self.assertAlmostEqual(cfg.typed_fields["motif_zero_tol"], 1.0e-6)

    def test_graph_slice_roundtrip(self) -> None:
        for raw_id in (GRAPH_INPUT, GRAPH_CHANGE):
            parsed = atlas_id_codec.GraphSliceId.parse(raw_id)
            self.assertEqual(parsed.render(), raw_id)

    def test_behavior_slice_roundtrip(self) -> None:
        for raw_id in (SLICE_INPUT, SLICE_CHANGE):
            parsed = atlas_id_codec.BehaviorSliceId.parse(raw_id)
            self.assertEqual(parsed.render(), raw_id)

    def test_child_identifier_roundtrip(self) -> None:
        cases = [
            ("regime", REGIME_ID),
            ("transition", TRANSITION_ID),
            ("family", FAMILY_EXACT_ID),
            ("family", FAMILY_MOTIF_ID),
            ("path", PATH_ID),
        ]
        for kind, raw_id in cases:
            parsed = atlas_id_codec.parse_identifier(kind, raw_id)
            self.assertEqual(atlas_id_codec.render_identifier(kind, parsed), raw_id)

    def test_compact_expand_roundtrip(self) -> None:
        dictionaries = atlas_id_codec.AtlasDictionaries()
        cases = [
            ("graph_slice", GRAPH_INPUT),
            ("graph_slice", GRAPH_CHANGE),
            ("slice", SLICE_INPUT),
            ("slice", SLICE_CHANGE),
            ("regime", REGIME_ID),
            ("transition", TRANSITION_ID),
            ("family", FAMILY_EXACT_ID),
            ("family", FAMILY_MOTIF_ID),
            ("path", PATH_ID),
        ]
        for kind, raw_id in cases:
            parsed = atlas_id_codec.parse_identifier(kind, raw_id)
            compact = atlas_id_codec.KIND_TO_COMPACT[kind](parsed, dictionaries)
            expanded = atlas_id_codec.expand_compact_identifier(kind, compact, dictionaries)
            self.assertEqual(atlas_id_codec.render_identifier(kind, expanded), raw_id)

    def test_cfg_compact_expand_roundtrip(self) -> None:
        dictionaries = atlas_id_codec.AtlasDictionaries()
        cfg = atlas_id_codec.ClassifierConfig.parse(CFG)
        compact = {"cfg_id": dictionaries.cfg_id(cfg.render())}
        expanded = atlas_id_codec.expand_compact_identifier("cfg", compact, dictionaries)
        self.assertEqual(expanded.render(), CFG)


if __name__ == "__main__":
    unittest.main()
