# Project knowledge base

Biocircuits Explorer has three kinds of material that used to look equally current:
executable product code, research snapshots, and dated project narratives. They are
not interchangeable. This directory makes the distinction explicit. A statement is
current only when its named owner and evidence still support it; an old document is
useful history, not an automatic source of truth.

The plain-language contrast is **verified current contract versus narrative
snapshot**. The two technical terms used on this page are:

- **contract** — a behavior or data shape that code and tests are expected to keep;
- **evidence** — the source, test, command, dataset identity, or result that lets
  another person check a statement.

Terms such as reaction order, designability, witness, certificate, support, and
feasible region are postponed to [the glossary](glossary.md) and the relevant
module or contract page.

## Start here

For a new contributor or a compacted agent context, read in this order:

1. [`PROJECT_SUMMARY.md`](../PROJECT_SUMMARY.md) for the smallest project map.
2. [`status/current.md`](status/current.md) for the verified snapshot and open
   unknowns.
3. The architecture overview selected by [`manifest.yaml`](manifest.yaml).
4. [`catalogs/modules.yaml`](catalogs/modules.yaml) and the module card for the
   code being changed.
5. [`catalogs/contracts.yaml`](catalogs/contracts.yaml) and the applicable
   contract page.
6. A task-specific context pack, if one exists.
7. The cited source and tests before changing a scientific or compatibility claim.

`manifest.yaml` is the machine-readable navigation index. The catalogs answer
three different questions: which code owns a capability, which interfaces must
remain compatible, and which data products are safe to use.

## Authority order

When two statements disagree, use the first applicable item below and record the
conflict instead of silently merging the wording:

1. observed runtime behavior and immutable artifact bytes;
2. executable source plus tests at the declared revision;
3. versioned schemas and generated files whose drift check passes;
4. this knowledge base's contracts, catalogs, and decisions;
5. architecture and module summaries;
6. root entry-point summaries;
7. historical reports, ignored `doc/` or `wiki/` material, and copied research
   snapshots.

Higher-level pages are **verifiable compression**: they route readers to the
owner and evidence, but do not become a second owner of the same fact. Each
mutable fact should have one canonical owner. If no owner can be identified, mark
the value `unknown`.

## Evidence states

- `verified` — checked against the revision declared by the document and backed
  by a direct source/test/artifact reference;
- `provisional` — implemented or proposed, but its full gate has not been run;
- `unknown` — evidence is missing, conflicting, or semantically ambiguous;
- `historical` — valid only as a dated record;
- `generated` — derived from another owner and never edited as the primary source.

Dates alone do not make prose current. A verified revision and evidence path do.

## Documentation policy

- Put system-wide orientation here; put implementation detail in a module card.
- Put compatibility rules in `contracts/`; put machine routing in `catalogs/`.
- Put a durable choice and its trade-offs in `decisions/`.
- Put task-local read order and guardrails in `context-packs/`; do not copy whole
  module cards into a pack.
- Keep paths repository-relative. Do not publish private checkout locations,
  workstation paths, credentials, private feedback, or private repository URLs.
- Do not turn a retrieval score, atlas label, or screened proxy into a verified
  design. Follow [`contracts/scientific-evidence.md`](contracts/scientific-evidence.md).
- Do not copy a research number into product or manuscript prose without an
  artifact identity and an explicit claim owner.

The unified gate applies a conservative lexical safety check to maintained
public-facing files. Workstation roots such as `/home` and `/tmp`, SSH-style
repository addresses, credential-bearing URLs, private-key headers, and common
token shapes are rejected even when an example might be harmless. This bounded
guard is not a full secret scanner; release workflows still need their normal
repository and hosting checks.

The legacy `doc/`, `wiki/`, and incident-specific agent notes can still explain
why something was attempted. Unless a current page explicitly promotes a file
with fresh evidence, those locations are non-authoritative.

## Updating the knowledge base

For a code change that affects behavior:

1. update and run the owning test first;
2. update the owning schema or contract;
3. update its catalog entry and module card;
4. add a decision only when the choice is durable and non-obvious;
5. change summaries last;
6. for a snapshot document, set `verified_against` to the revision actually
   inspected or mark it provisional; executable catalogs instead retain their
   historical `baseline_evidence_revision` and must pass their declared
   `verification_command` on the current tree.

For a data or paper claim, also follow the repository hand-off in
[`research/repositories.md`](research/repositories.md). Conflicting counts stay
`unknown` until their semantics and lineage are reconciled.
