"""build_indexes.py — one-command Stage-2 atlas_root build (S2.1 → S2.5).

    python3 build_indexes.py --packets <spike-curve-packet-corpus> --out <atlas_root>
    python3 build_indexes.py <atlas_root>          # already migrated; (re)build indexes only

Runs: build_atlas_store (Parquet+Zarr) → build_function_index (HNSW+kNN graph) →
build_diffusion (diffusion coords) → build_mapper (Mapper graph) → build_concepts
(FCA lattice + redescriptions). Idempotent; safe to re-run.
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _run(script, *args):
    print(f"\n=== {script} {' '.join(map(str, args))} ===")
    subprocess.check_call([sys.executable, os.path.join(HERE, script), *map(str, args)])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?")
    ap.add_argument("--packets")
    ap.add_argument("--out")
    a = ap.parse_args()
    root = a.out or a.root
    if not root:
        ap.error("give an atlas_root (positional) or --out with --packets")
    here = lambda *p: os.path.join(root, *p)
    if a.packets and not os.path.exists(here("metadata", "records.parquet")):
        _run("build_atlas_store.py", "--in", a.packets, "--out", root)
    elif a.packets:
        print("skip migrate (metadata/records.parquet exists)")
    # idempotent: skip a step whose primary output already exists (resume after a stall)
    for script, out in (("build_function_index.py", ("indexes", "hnsw_function.bin")),
                        ("build_diffusion.py", ("indexes", "diffusion_coords.parquet")),
                        ("build_mapper.py", ("indexes", "mapper_graph.json")),
                        ("build_concepts.py", ("indexes", "concept_lattice.json"))):
        if os.path.exists(here(*out)):
            print(f"skip {script} ({out[-1]} exists)")
        else:
            _run(script, root)
    print(f"\nStage-2 atlas_root ready at {root}  (metadata/ arrays/ rop/ indexes/).")


if __name__ == "__main__":
    main()
