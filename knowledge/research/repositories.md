# Research repository roles

This public repository owns product and computation code. Manuscripts, private
feedback, and paper-side data belong in separately access-controlled workspaces.
A shared topic or copied filename does not transfer authority.

## Authority matrix

| Workspace | Canonical role | Owns | Does not own |
|---|---|---|---|
| `Biocircuits-Explorer` | computation and product | engine/backend source, schemas, tests, artifact production, product behavior | final manuscript wording or private feedback history |
| Private manuscript workspace | manuscript and reproducibility package | paper text, publication figures/tables, claim ledger, paper-side artifact lock, reproduction entry points | live product behavior or unpinned Explorer output |
| Private feedback archive | dated working record | reports, feedback, and historical decision context | current code contract, current dataset count, or final manuscript claim |

Repository names are used as logical identifiers. Public documentation must not
contain private repository URLs, local checkout paths, workstation mount points,
credentials, or raw private feedback.

## Public-boundary rule

Do not copy a manuscript directory, paper figures, notebook outputs, private
research export, or workstation script into Explorer. A byte-identical copy is
still a public disclosure. The repository verifier rejects these paths and
common manuscript file types even when a force-add bypasses `.gitignore`.

## Required hand-off

The safe direction is explicit and versioned:

1. Explorer produces an artifact release with a stable release ID, code revision,
   config, hashes, semantic counts, and reproduction command.
2. The private manuscript workspace records that release in a paper-side artifact lock.
3. A claim ledger maps each sentence/figure/table to the locked artifact, declared
   assumptions, and allowed wording.
4. Paper checks reject missing files, hash drift, or claims that refer to an
   unlocked artifact.
5. Private feedback may summarize the state at a date, but does not become an input to
   either lock.

Reverse transfer is also explicit: a paper-discovered bug or new requirement
becomes an Explorer issue/test/code change; it is not patched only in a copied
script and treated as product behavior.

## Data reconciliation

The authoritative publication count and relationships among research
populations are **unknown** in this checkout unless a reviewed artifact release
declares their lineage and semantics. Do not infer them from copied filenames or
approximate counts.

## Working-copy discipline

- Audit a repository before making cross-repository edits.
- Use an isolated branch/worktree when the manuscript checkout contains unrelated
  local work.
- Do not bulk-add untracked figures or data; first map each output to its generator
  and input.
- Do not use a private feedback archive as a way to rewrite history. Add a new
  dated record if a historical correction is genuinely required.
- Keep public provenance sanitized: commit/release/hash identifiers are useful;
  private locations and access details are not.

Explorer research output may support development and regression, but must not be
described as a canonical manuscript dataset without the private release gate.
