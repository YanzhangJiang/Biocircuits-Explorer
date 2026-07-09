# Schema contract and coverage

A schema file documents a shape. It does not, by itself, prove that every
producer emits that shape or that every stored instance was validated.

The exact schema path, `$id`, identity field, version, contract owner, and
declared coverage are generated in the
[contract reference](../generated/reference.md#json-schemas). Values there come
from the schema and catalogs, so this page does not repeat them.

## What the coverage statements mean

The contract catalog distinguishes four kinds of evidence:

- generated drift checks show that a derived file matches its runtime source;
- runtime contract tests show that selected positive and negative inputs behave
  as promised;
- tracked-instance validation shows that committed examples satisfy the schema;
- semantic tests protect meaning that JSON Schema alone cannot express, such as
  keeping proxy candidates separate from verified recommendations.

“Partial,” “conditional,” or “not established” means `unknown`, not “known
invalid.” Upgrade such a claim only after adding an identified producer fixture
and a validator that actually loads it.

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

## Required change sequence

For a schema-bearing interface:

1. change the runtime owner;
2. add or update positive and negative contract fixtures;
3. update or regenerate the schema;
4. run the producer and consumer tests;
5. regenerate the reference with `python3 scripts/verify_repository.py --write`;
6. run the read-only gate with `python3 scripts/verify_repository.py --check`;
7. document migration and sunset behavior when compatibility is not additive.

`webapp/scripts/validate_artifacts.py` implements only the draft-07 subset used
by its configured artifact families. A pass must not be generalized to schemas
or instances the script did not load.
