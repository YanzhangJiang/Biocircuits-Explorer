# Research repository roles

Three repositories participate in the project, but each owns a different kind of
truth. A shared topic or copied filename does not transfer authority.

## Authority matrix

| Repository | Canonical role | Owns | Does not own |
|---|---|---|---|
| `Biocircuits-Explorer` | computation and product | engine/backend source, schemas, tests, artifact production, product behavior | final manuscript wording or private feedback history |
| `paper_rop_periodic_table` | standalone manuscript and reproducibility package | paper text, publication figures/tables, claim ledger, paper-side artifact lock, reproduction entry points | live product behavior or unpinned Explorer output |
| `bmac-weekly-reports` | dated weekly record | reports, feedback, and historical decision context as of their dates | current code contract, current dataset count, or final manuscript claim |

Repository names are used as logical identifiers. Public documentation must not
contain private repository URLs, local checkout paths, workstation mount points,
credentials, or raw private feedback.

## Name collision inside Explorer

`Biocircuits-Explorer/paper_rop_periodic_table/` is a tracked embedded research
snapshot. It is useful for product-side regression and offline examples, but it is
not the standalone manuscript repository. Likewise,
`webapp/scripts/rop_periodic_table/` contains duplicated research exports and
scripts. A byte-identical copy is still a copy; the standalone paper repository
owns publication wording and release consumption.

Never infer that editing the embedded directory updates the manuscript, or that a
working manuscript file has been released back to Explorer.

## Required hand-off

The safe direction is explicit and versioned:

1. Explorer produces an artifact release with a stable release ID, code revision,
   config, hashes, semantic counts, and reproduction command.
2. The paper repository records that release in a paper-side artifact lock.
3. A claim ledger maps each sentence/figure/table to the locked artifact, declared
   assumptions, and allowed wording.
4. Paper checks reject missing files, hash drift, or claims that refer to an
   unlocked artifact.
5. Weekly reports may summarize the state at a date, but do not become an input to
   either lock.

Reverse transfer is also explicit: a paper-discovered bug or new requirement
becomes an Explorer issue/test/code change; it is not patched only in a copied
script and treated as product behavior.

## Current data reconciliation

At Explorer revision `f9c65a5`, direct inspection found several differently
labelled slice and network populations in the embedded snapshot. The standalone
paper repository now owns its exact committed baseline and candidate comparison
under `provenance/` at commit
`bbe91d958df29a98b6462cbdf319002e7fdac8e6`. Explorer's machine catalog records
only the cross-repository routing and status; it does not duplicate the paper's
exact values.

The authoritative publication count and the relationships among those
populations are **unknown** until a release explains the lineage and semantics.
Do not “fix” one number by guessing that another measures the same set, even when
some arithmetic happens to agree. Preserve the source labels and reconcile them
through the artifact lock and claim ledger. The paper's CI verifier currently
passes the committed consumer snapshot, while its stricter release mode fails on
the intentionally recorded producer-lineage, claim, count-semantics, and figure
gaps.

## Working-copy discipline

- Audit a repository before making cross-repository edits.
- Use an isolated branch/worktree when the manuscript checkout contains unrelated
  local work.
- Do not bulk-add untracked figures or data; first map each output to its generator
  and input.
- Do not modify the weekly archive as a way to rewrite history. Add a new dated
  record if a historical correction is genuinely required.
- Keep public provenance sanitized: commit/release/hash identifiers are useful;
  private locations and access details are not.

Until the paper's release gate passes, Explorer research snapshots may support
development and regression, but they must not be described as the canonical
manuscript dataset.
