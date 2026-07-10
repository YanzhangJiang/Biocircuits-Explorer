---
title: Schema contract and coverage
status: verified
verified_against: 1177a3d
---

# Schema contract and coverage

A schema tells a client which fields may appear. It does not prove that a
computation converged, that every producer was validated, or that an old stored
file has current meaning. Shape and evidence are separate contracts.

The exact schema path, `$id`, identity field, version, contract owner, and
declared coverage are generated in the
[contract reference](../generated/reference.md#json-schemas). Values there come
from the schema and catalogs, so this page does not copy the full inventory.

## What coverage statements mean

The contract catalog distinguishes four kinds of evidence:

- generated drift checks show that a derived file matches its runtime source;
- runtime contract tests show that selected positive and negative inputs behave
  as promised;
- tracked-instance validation shows that committed examples satisfy a schema;
- semantic tests protect meaning that JSON Schema alone cannot express, such as
  keeping proxy candidates separate from verified recommendations and rejecting
  incomplete numerical evidence.

“Partial,” “conditional,” or “not established” means `unknown`, not “known
invalid.” Upgrade such a claim only after adding an identified producer fixture
and a validator that actually loads it.

## Current Design Screen contract

The current response identity is `bne-design-screen/v0.3.0`. Version 0.3 makes
synchronous evaluation coverage visible:

- `eligible_count` is the number of deduplicated catalogue records eligible
  before the card budget is applied;
- `evaluated_count` is the number of records actually evaluated;
- `screened_count` remains the compatibility count and equals
  `evaluated_count`;
- `truncated` is true exactly when `eligible_count > evaluated_count`;
- the same three coverage fields are repeated in `screen_summary` for display
  consumers.

This is not a pagination promise. A truncated response says that the synchronous
screen stopped at its bounded prefix; unevaluated records are neither failures
nor recommendations. The synchronous request cap is 64 screened cards and 8
exact placements. A requested budget above those values is a 422 budget failure,
not silent clipping.

### Migration from v0.2

Version 0.2 consumers that read only `screened_count` can continue to display the
evaluated prefix, but they cannot tell whether candidates were omitted. Current
consumers must require the v0.3 identity and use `eligible_count`,
`evaluated_count`, and `truncated` together. The browser renderer keeps a v0.2
display fallback for historical artifacts; that fallback does not change the
current producer schema.

`screened_candidates` remain proxy-only. `verified_recommendations` remain the
only recommendation list. The transitional `recommended` alias must equal the
verified list, never the screened prefix.

## Numerical validity fields

Several scan responses are handler contracts rather than members of a single
hand-authored schema family. Their additive validity fields still have fixed
meaning:

| Response kind | Validity fields | Meaning |
|---|---|---|
| 1D scan, placer curve, qK-space ROP cloud | `valid`, `partial` | one Boolean per sample; `partial` is true when any sample is invalid |
| 2D scan, atlas landscape, FRET heatmap | `validity_grid`, `partial` | one Boolean per cell; `partial` is true when any cell is invalid |
| Placement threshold/program verification | `verification_validity`, `verification_partial` | only complete valid verification may pass |
| Inverse refinement | `refinement_status`, `valid_sample_count`, `sample_count`, `partial` | partial numerical scans are not eligible for best-candidate selection |

Non-converged or non-finite samples are invalid gaps, not zero-valued data. JSON
serialization may encode non-finite numeric placeholders safely, but a consumer
must use the validity mask and must not fill a gap to manufacture a pass, shape,
or ranking score.

## Ownership rules

1. `NetworkIR` and `DesignSpec` fields and version constants are owned by Julia
   source. Change the structs/parser and regenerate their schemas; do not make a
   generated JSON file the primary edit.
2. A hand-authored schema changes with its runtime producer or consumer and a
   contract test in the same change.
3. Each schema has one `$id`, one identity-field `const`, one contract owner, and
   one artifact-catalog owner. The schema owns its version value; catalogs point
   to it instead of copying it.
4. A schema family and a concrete version are different. The NetworkIR parser's
   family-level acceptance is forward-compatibility behavior, not proof that an
   unknown future payload has the intended meaning. The DesignabilitySpec
   normalizer instead requires its exact current version.
5. `additionalProperties: true` is an extension policy, not permission for a
   client to rely on arbitrary fields forever.
6. JSON Schema `$id` values identify schemas. Repository links remain relative,
   so private checkout locations never enter the public contract.

## Identity boundary schemas cannot express

Canonical topology relabeling is exact only for binding networks with at most
seven free species. Above seven, `NetworkIR` and inverse-support identities use a
deterministic positional content hash instead of factorial relabeling. The
payload remains schema-valid, but two payloads that differ only by species
renaming are not promised to deduplicate. Direct exact atlas canonicalization
rejects that unsupported size. Even within seven, the full NetworkIR content
hash retains declared content beyond the topology code, so graph isomorphism
alone is not a promise of equal full hashes. This is an identity algorithm
boundary, not a JSON Schema error.

## Required change sequence

For a schema-bearing interface:

1. change the runtime owner;
2. add or update positive and negative contract fixtures;
3. update or regenerate the schema;
4. run producer and consumer tests;
5. regenerate the reference with `python3 scripts/verify_repository.py --write`;
6. run the read-only gate with `python3 scripts/verify_repository.py --check`;
7. document migration and sunset behavior when compatibility is not additive.

`webapp/scripts/validate_artifacts.py` implements only the draft-07 subset used
by its configured artifact families. A pass must not be generalized to schemas
or instances the script did not load.

## Verified against

- Current source commit: `1177a3d`.
- Historical baseline: schema ownership and drift rules were audited at
  `f9c65a5`; that audit predates Design Screen v0.3 and the explicit numerical
  validity fields above.
