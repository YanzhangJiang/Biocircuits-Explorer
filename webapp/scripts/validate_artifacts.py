#!/usr/bin/env python3
# Phase-0 closeout: validate PRODUCED artifacts against their hand-authored JSON
# Schemas (the struct-generated network-ir/design-spec schemas are already guarded
# by gen_schemas.jl + `git diff --exit-code` in CI; this covers the rest, which
# have no struct to drift-check against and no instance validation otherwise):
#   - datasets/*/manifest.json        -> schemas/latent-atlas-manifest.schema.json
#   - benchmarks/specs/*.behavior.json -> schemas/behavior-spec.schema.json
#
# Self-contained (NO jsonschema dependency) draft-07 subset validator covering the
# features these schemas actually use: type, properties, required, enum, const,
# $ref(#/$defs/..), items, additionalProperties:false, min/maxItems, number/integer.
# Exits non-zero on any violation so it can gate CI.
#   python3 webapp/scripts/validate_artifacts.py            # validate all known artifacts
#   python3 webapp/scripts/validate_artifacts.py --schema X --instance Y
import sys, os, json, glob, argparse

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
TYPES = {"object": dict, "array": list, "string": str, "boolean": bool,
         "number": (int, float), "integer": int, "null": type(None)}

def resolve(ref, root):
    assert ref.startswith("#/"), f"only local refs supported: {ref}"
    node = root
    for part in ref[2:].split("/"):
        node = node[part]
    return node

def check(inst, schema, root, path, errs):
    if "$ref" in schema:
        schema = resolve(schema["$ref"], root)
    t = schema.get("type")
    if t:
        ts = t if isinstance(t, list) else [t]
        ok = any(isinstance(inst, TYPES[x]) and not (x == "number" and isinstance(inst, bool))
                 and not (x == "integer" and isinstance(inst, bool)) for x in ts)
        # bool is a subclass of int — exclude it from number/integer
        if isinstance(inst, bool) and "boolean" not in ts:
            ok = False
        if not ok:
            errs.append(f"{path}: expected type {t}, got {type(inst).__name__}"); return
    if "const" in schema and inst != schema["const"]:
        errs.append(f"{path}: const mismatch (want {schema['const']!r}, got {inst!r})")
    if "enum" in schema and inst not in schema["enum"]:
        errs.append(f"{path}: {inst!r} not in enum {schema['enum']}")
    if isinstance(inst, dict):
        props = schema.get("properties", {})
        for req in schema.get("required", []):
            if req not in inst:
                errs.append(f"{path}: missing required property '{req}'")
        if schema.get("additionalProperties", True) is False:
            for k in inst:
                if k not in props:
                    errs.append(f"{path}: unexpected property '{k}' (additionalProperties:false)")
        for k, v in inst.items():
            if k in props:
                check(v, props[k], root, f"{path}.{k}", errs)
    if isinstance(inst, list):
        if "items" in schema:
            for i, el in enumerate(inst):
                check(el, schema["items"], root, f"{path}[{i}]", errs)
        if "minItems" in schema and len(inst) < schema["minItems"]:
            errs.append(f"{path}: minItems {schema['minItems']} (got {len(inst)})")
        if "maxItems" in schema and len(inst) > schema["maxItems"]:
            errs.append(f"{path}: maxItems {schema['maxItems']} (got {len(inst)})")

def validate(instance_path, schema_path):
    schema = json.load(open(schema_path))
    inst = json.load(open(instance_path))
    errs = []
    check(inst, schema, schema, os.path.basename(instance_path), errs)
    return errs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema"); ap.add_argument("--instance")
    a = ap.parse_args()
    pairs = []
    if a.schema and a.instance:
        pairs.append((a.instance, a.schema))
    else:
        bs = os.path.join(ROOT, "schemas", "behavior-spec.schema.json")
        mf = os.path.join(ROOT, "schemas", "latent-atlas-manifest.schema.json")
        for f in glob.glob(os.path.join(ROOT, "datasets", "*", "manifest.json")):
            pairs.append((f, mf))
        for f in glob.glob(os.path.join(ROOT, "benchmarks", "specs", "*.behavior.json")):
            pairs.append((f, bs))
    if not pairs:
        print("no artifacts found to validate (datasets/*/manifest.json, benchmarks/specs/*.behavior.json)")
        return 0
    bad = 0
    for inst, sch in pairs:
        errs = validate(inst, sch)
        rel = os.path.relpath(inst, ROOT)
        if errs:
            bad += 1
            print(f"FAIL {rel}  (vs {os.path.basename(sch)})")
            for e in errs:
                print(f"     - {e}")
        else:
            print(f"ok   {rel}  (vs {os.path.basename(sch)})")
    if bad:
        print(f"\n{bad} artifact(s) failed schema validation")
        return 1
    print(f"\nall {len(pairs)} artifact(s) valid")
    return 0

if __name__ == "__main__":
    sys.exit(main())
