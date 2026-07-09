# Context packs

A context pack is a small routing document for one bounded task. It helps a human
or agent resume after context compaction without treating a stale chat summary as
truth. It points to owners and evidence; it must not duplicate whole architecture,
module, or contract pages.

## Re-entry sequence

1. Inspect `git status` and the current revision. Existing changes belong to the
   current workspace owner until proven otherwise.
2. Read `PROJECT_SUMMARY.md` and `knowledge/status/current.md`.
3. Read the task's context pack.
4. Follow its module and contract IDs through the machine-readable catalogs.
5. Inspect the cited source/tests/artifacts at the pack's `verified_against`
   revision.
6. Re-verify any fact that would change the implementation or scientific wording.
7. Mark conflicts `unknown`; do not fill gaps from memory.

## Creating a pack

Copy `template.md` to a short task name under this directory. Keep it small enough
to read before code, and include:

- objective and explicit non-goals;
- verified revision and worktree/branch identity without private absolute paths;
- read order by module/contract ID;
- invariants and evidence boundaries;
- allowed and forbidden path scopes;
- focused verification commands;
- current checkpoint and unresolved unknowns.

Do not include secrets, API keys, private URLs, workstation paths, large logs, or
unreviewed manuscript claims. A temporary local handoff may contain more tactical
detail in an ignored workspace location, but the tracked pack remains public-safe.

Update or delete a pack when its task ends. Durable facts discovered during the
task belong in the owning contract, module card, catalog, or decision—not in an
ever-growing task diary.
