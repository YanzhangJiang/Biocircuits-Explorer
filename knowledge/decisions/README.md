# Architecture decision records

Use a decision record when the project makes a durable, non-obvious choice whose
trade-offs future contributors would otherwise have to rediscover. Examples are
identity semantics, compatibility sunsets, evidence policy, repository ownership,
or a storage format. Routine implementation notes and temporary task state belong
elsewhere.

## Format

Copy `template.md` to a file named `NNNN-short-kebab-title.md`. Use the next
available four-digit number. Paths and evidence references must be
repository-relative; public records must not contain private infrastructure or
repository locations.

Status values:

- `proposed` — under review, not yet binding;
- `accepted` — current decision;
- `superseded` — replaced by a named later record;
- `rejected` — considered but not adopted;
- `historical` — describes a past state and has no current force.

## Review rules

1. State the user/system problem before naming the technical choice.
2. Separate observed constraints from preferences.
3. Name alternatives that were genuinely viable and why they lost.
4. Give source/test/artifact evidence and a `verified_against` revision.
5. Record consequences, including migration and rollback implications.
6. Do not silently rewrite an accepted decision. Make a new record and mark the
   old one superseded, preserving the reasoning chain.
7. Update the relevant contract/catalog/module card when a decision changes
   executable behavior. A decision record alone does not implement anything.

The index of accepted decisions belongs in `knowledge/manifest.yaml`. An empty
index means no decision has yet been promoted under this policy, not that the
system has no history.
