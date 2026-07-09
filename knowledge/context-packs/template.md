# Context pack: task name

- Status: active
- Verified against: `unknown`
- Branch/worktree label: `unknown`
- Primary module IDs: `[]`
- Primary contract IDs: `[]`

## Objective

One observable outcome. State the user/system problem in plain language.

## Non-goals

- Boundary that must not expand silently.

## Read order

1. `PROJECT_SUMMARY.md`
2. `knowledge/status/current.md`
3. `knowledge/modules/<module-id>.md`
4. `knowledge/contracts/<contract>.md`
5. Source and tests listed below

## Source and evidence map

| Question | Canonical relative path | Focus |
|---|---|---|
| Behavior owner | `path/to/source` | symbol or section |
| Regression owner | `path/to/test` | test set or invariant |
| Data owner | `path/to/manifest` | release/hash/semantics |

## Invariants

- Invariant that must remain true and how it is observed.
- Evidence wording that must not be upgraded.

## Path scope

May change:

- `relative/path`

Must not change:

- `relative/path`

## Verification

```bash
focused command from repository root
```

Expected result: describe the invariant, not a remembered test count.

## Current checkpoint

- Completed: none
- In progress: none
- Next safe action: inspect current state

## Unknowns and risks

- `unknown`: missing or conflicting evidence and the check needed to resolve it.

## Handoff rule

Before stopping, record the current revision, dirty paths, commands actually run,
results, and the next safe action. Do not claim the task complete from a narrow
test when the objective requires a broader gate.
