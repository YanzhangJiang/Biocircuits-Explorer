---
title: Frontend design consistency contract
status: active
verified_against: working tree on 4b07626 (frontend consistency + node geometry passes, uncommitted)
---

# Frontend design consistency contract

The browser frontend under `webapp/public/` has one design system. This page
names the owner file for each design concern and states the rule that future
changes must keep. It routes to owners; it does not become a second owner of
any literal value. When a rule here conflicts with the code, the code is wrong
or this page is stale — fix one of them in the same change (see "Maintaining
this contract").

The rules below exist because each one was once violated and produced a real
defect: an undefined-variable declaration that silently dropped error styling,
a fullscreen overlay rendered under the header, dead Run buttons on restored
legacy nodes, wires and sockets of the same port rendering in different
colors, and a hardcoded path index that would have exported silently wrong
figures.

## Stylesheet layering

`index-node.html` loads stylesheets in a fixed order, and each file has one
role:

1. `style-node-base.css` — all layout, spacing, typography, and structure. It
   begins with `@import url("target-design.css")`. Base references every
   color through `var(--*)`; hardcoded color literals are allowed only inside
   its small `:root` block and inside `var()` fallback positions.
2. Base's `:root` block holds the **intentionally theme-independent tokens**:
   category identity colors (`--header-*-bg`), port colors (`--port-*`),
   socket colors, the terminal-style debug console palette, and dark toast
   bubbles. These are defined once here and are not overridden per theme.
3. `style-node.css` — the dark theme variable set, always loaded.
4. `style-node-light.css` — pure variable overrides scoped to
   `:root[data-effective-theme="light"]`, injected at runtime by
   `js/theme.js`. It contains no layout rules.
5. `style-node-agent.css` — the Design Agent surface variant. It owns the
   `--ax-*` palette, but shared concepts (status colors, port colors) must
   reference the main tokens with a matching literal fallback, e.g.
   `var(--status-ok, #46c46b)`, so a standalone load keeps working.

## Color and token rules

- **Theme manifest parity.** `style-node.css` and `style-node-light.css`
  define exactly the same variable names. Check with
  `diff <(grep -o '\--[a-z0-9-]*:' style-node.css | sort -u) <(grep -o '\--[a-z0-9-]*:' style-node-light.css | sort -u)`.
  A variable that is genuinely theme-independent moves to base's `:root` with
  a comment saying so; it does not become a one-theme-only definition.
- **Status colors have one family:** `--status-ok`, `--status-warn`,
  `--status-error`, plus their `--status-*-soft` companions. CSS rules and JS
  inline styles (for example `js/node-types/placer.js`) consume only these.
  Introducing a new hardcoded status hue is a contract violation.
- **Selection colors** are `--selection-color` and `--selection-bg`, defined
  per theme. A `var()` fallback literal must equal the defined value; a
  fallback that is the real value in disguise hides missing definitions.
- **Category identity colors** (`--header-input-bg`, `--header-parameter-bg`,
  `--header-process-bg`, `--header-result-bg`, `--header-viewer-bg`,
  `--header-note-bg`) are theme-independent and live in base's `:root`. They
  drive both node headers (`headerClass` in the node type definition) and the
  Add Node menu chips (`.menu-color.color-*`). Menu chips reference these
  variables, never literals, so the two surfaces cannot drift.
- **Node-interior wells** are `--node-inset-bg` / `--node-inset-border`,
  defined per theme (dark in `style-node.css`, light in
  `style-node-light.css`). Everything recessed inside a node — `.node-panel`,
  metric cards, empty-state regions — consumes only these two.
- **z-index scale** is a small ordered set: canvas affordances (5–200),
  cheatsheet-style fullscreen overlays must sit above `#header` (2000) and
  debug console (1900) — currently 2500 — below dropdown menus (3000), with
  toasts at 5000. Pick from this scale; do not invent ad-hoc values.

## Port color pairing

Port colors have two owners that must cover the same port names with the same
grouping:

- `js/state.js` `PORT_COLOR_GROUPS` + `getPortColor` own the wire stroke;
- `style-node-base.css` `[data-port]` rules own the socket fill, referencing
  the same group's `var(--port-*)`.

Adding a port name means three edits in one change: declare the artifact type
in `js/port-types.js`, assign exactly one color group in `state.js`, and add
the matching socket rule. The fallbacks (`#888` wire, default socket fill)
must never trigger for a registered port; if they do, one table is stale.

## Node geometry

- A node's `category` decides its color identity; `headerClass` is derived
  from `category`, never chosen independently. `sbml-export` is `process` with
  `header-process`; config-role nodes such as `inverse-design-target` are
  `parameter` with `header-parameter`.
- The `.viewer` CSS class applies only to `viewer` and `result` categories
  and is a styling hook only; it carries no size floor. Parameter nodes
  render at their declared `defaultWidth` (320px).
- **Minimum sizes are a per-type contract.** `js/node-sizes.js`
  (`NODE_MIN_SIZES`) is the single authority — every one of the 43 types has
  an explicit entry. `createNode` pins it as inline
  `style.minWidth/minHeight`, so the browser itself clamps every sizing path
  (default render, workspace restore, undo, paste) including old workspaces
  saved below the floor; the `.node-resize` drag in `js/canvas.js` clamps to
  the same values so the serialized size never goes under. There is no
  blanket category floor in CSS (the former `.viewer` 380×300 rule is
  removed); the base `.node` 240×100 rule is only a backstop for unknown
  types, which get `FALLBACK_MIN_NODE_SIZE` (240×140).
- **Deriving a minimum for a new type.** Height = 60px chrome (header 36 +
  body padding 24) + 36px per socket row (inputs + outputs) + the smallest
  content block that stays usable: plot area ≥ ~280×180 (Plotly's own
  min-heights force scrolling below that), one form row ~43px, a button row
  ~38px, tab-nav ~46px. Width must fit the widest non-wrapping control row;
  grids with `minmax(220px, …)` columns and tables that scroll horizontally
  only need their first column/rows visible. `applyNodeContracts` throws at
  module construction when the table drifts from the type inventory or a
  declared `defaultWidth/defaultHeight` is below the minimum, and
  `webapp/test/node-sizes-inventory.test.mjs` pins the same invariants plus
  the clamp math.
- **Node DOM structure pins every port to the node frame.** `createNode`
  emits: `.node-header`, then `.node-body` (flex column, `overflow:hidden`)
  containing input socket rows pinned at top, a `.node-content` wrapper
  (the only scrolling layer: `flex:1; overflow-y:auto; overflow-x:hidden;
  overscroll-behavior:contain`) holding all `createBody` markup, and output
  socket rows pinned at bottom. Ports never live inside the scrollable flow,
  so connection endpoints always land on the visible node edge; tabbed nodes
  keep `.node-content` at `overflow:hidden` and scroll inside the tab pane.
  Appending ports inside scrolling content is the historical "floating wire"
  defect and is a contract violation.
- **Declared and planned sizes are one value.** A type's
  `defaultWidth`/`defaultHeight` (rendered) must equal its
  `DEFAULT_NODE_SIZES` entry in `js/graph-patch.js` (planning). Set them from
  the measured natural content height so the node initially renders without
  an internal scrollbar; when content legitimately exceeds the viewport, the
  pinned-port structure above is the intended fallback, not hidden ports.
- Placement reservations in `js/graph-patch.js` (`DEFAULT_PARAMETER_SIZE`
  320px, `DEFAULT_RESULT_SIZE` 420px, and the `DEFAULT_NODE_SIZES` table)
  must equal the rendered widths; the Quick Add planner relies on this to
  produce the declared 60px gap, which `webapp/e2e/workspace.spec.mjs`
  measures against real DOM boxes.

## Node panels and output regions

Node interiors are modular, not a scrolling webpage — Blender is the
reference: distinct framed modules, and resizing the node stretches the
output region instead of reflowing or crushing content.

- **`.node-panel` is the module unit.** Structure: `.node-panel` >
  `.node-panel-header` (title in `.node-panel-title`, uppercase 10px
  letterspaced; toolbar buttons right-aligned, wrapping allowed) +
  `.node-panel-body`. Panels are wells painted with `--node-inset-bg` /
  `--node-inset-border` (registered in `style-node.css` and
  `style-node-light.css`; hardcoding a panel background or reusing
  `--panel-bg` for the well is a violation). Config sections of a node
  SHOULD be panels; primary action buttons and trailing hints stay outside
  panels as the node footer. `inverse-design-target` and `gradient-design`
  (in `js/node-types/inverse-design.js` + `js/design-target-editor.js`)
  are the reference implementations.
- **Charts and live visualizations MUST live in a chart panel**
  (`.node-panel--chart`, tighter body padding), never inline in the text
  flow. Chart SVGs scale with the node: `viewBox` set, CSS `width:100%;
  height:auto`, and no fixed pixel width/height attributes —
  `webapp/test/node-panels.test.mjs` enforces both rules.
- **Tables fill their panel flush**: `.node-panel-body--flush` (zero
  padding) with the scrolling wrapper (`.inverse-table-scroll`,
  max-height + internal scroll) directly inside.
- **Exactly one flexible output region per node.** `.node-content` is a
  flex column whose direct children are `flex-shrink:0`, so overflowing
  content scrolls between modules instead of crushing them. A node with a
  results/viewer area gives that region `flex:1 0 <basis>px` (the
  `.inverse-design-viewer` pattern): config panels keep natural height at
  the top, the output region absorbs extra node height and scrolls
  internally. Two flexible regions, or a flexible config panel, are
  contract violations. The region's flex-basis counts as its content
  height in the minimum-size formula.
- Re-measure natural heights after re-panelizing a node (panel chrome adds
  ~30px header plus borders/padding per panel) and sync the type's
  `defaultHeight`, `DEFAULT_NODE_SIZES`, and `NODE_MIN_SIZES` in the same
  change — `applyNodeContracts` and the e2e geometry checks catch drift.

## Node type JavaScript conventions

- `js/node-contracts.js` is the exhaustive node inventory and fails module
  construction on drift. Restore-only legacy types (`siso-analysis`,
  `rop-cloud`, `fret-heatmap`, `parameter-scan-1d/2d`, `rop-polyhedron`)
  carry no `execute`/`prepare`; their recompute paths call standalone
  functions (`runSISOAnalysis`, `runROPCloud`, `runFRETHeatmap`,
  `runParameterScan1D/2D`, `runROPPolyhedron`), never `NODE_TYPES[type].execute`.
- `createBody` DOM ids and `data-field` names are serialization contracts and
  stay byte-stable. A new-style params node and its restore-only twin share
  one body template function (`scan1DParameterBody`, `ropCloudSamplingBody`,
  `ropPolyAxesBody` in `js/node-types/`); the twin keeps only its real
  differences (Run button, auto-update behavior).
- Static controls use `data-action` delegation (`js/main.js` +
  `js/action-events.js`); dynamically created controls bind at the creation
  point. The loading state has one system: `js/node-loading.js`
  `setNodeLoading` with the `.loading` class — not per-feature copies.
- **Dragging a node works from its body, not only its header.** A left-button
  press anywhere inside `.node` starts a node drag, except on interactive
  surfaces: form controls, links, `[data-action]`, sockets, the resize
  handle, Plotly panes, drawing canvases, and scrollable regions' scrollbar
  gutters (the whitelist lives in `js/canvas.js`). Node resizing has one
  mechanism — the custom `.node-resize` handle, clamped per type to
  `NODE_MIN_SIZES`; the browser-native CSS `resize` property on `.node` is
  removed so there is no second grip.
- `escapeHtml` is imported from `js/api.js`; local copies are a contract
  violation.
- `CONFIG_PORT_TYPES` in `js/connections.js` is the complete list of config
  artifact types (it includes `DesignabilitySpec`, `InverseDesignRequest`,
  and `ROPShapeReferenceArtifact`); a new config-role type is added there so
  wiring triggers downstream `prepare` uniformly.
- `js/node-schema.js` field types mirror DOM input semantics: an optional
  numeric input that may be empty stays `type: 'string'` with a
  `parseOptional*` reader, so empty restores to absent instead of `0`
  (pinned by `webapp/test/design-spec-node-contract.test.mjs`).

## Page conventions

- **Relative resource paths only** (`vendor/…`, `./js/…`), matching
  `index-node.html`; absolute `/js/…` paths break prefixed deployments.
  Every page carries `<!DOCTYPE html>`, `lang="en"`, and
  `<link rel="icon" href="data:,">`.
- **i18n status:** `js/i18n.js` + `locales/zh.json` cover the landing pages
  only. The node editor is intentionally English-only until a product
  decision says otherwise; do not half-annotate `data-i18n` attributes.
- **Figure-export pages** (`webapp/public/figure-export-*.html`) serve the
  out-of-repo screenshot pipeline. They share `js/figure-export.js` and
  `figure-export.css`; page-specific code keeps only the network, titles, and
  plot calls. Rules that keep exported figures trustworthy:
  - the completion contract is `window.__figureExport`, with each page's
    payload keys preserved and `done` set last;
    `reportFigureExportDone`/`reportFigureExportError` are the only writers;
  - theme goes through `applyThemeMode('<mode>', { persist: false })` like
    every other consumer of `js/theme.js`;
  - mock node cards use the `export-node-*` classes and real theme tokens —
    never classes that collide with the editor's `.node`/`.node-header`;
  - selecting a behavior family or path by index is forbidden; select by
    label and **throw when missing** so the pipeline fails loudly instead of
    exporting a silently wrong figure;
  - optional indices use `??`, never `||` (index `0` is valid).

## Live charts and scientific visualization

Charts rendered inside nodes (for example the gradient-design learning
panel, owned by `js/inverse-design-render.js`) follow one grammar:

- **Minimal axes.** An L-shaped axis in `var(--panel-border)`, no gridlines,
  tick labels at min/max or a small adaptive set, transparent background.
  Charts must read in both themes and keep the UI font stack — no
  chart-specific or serif "math" fonts.
- **Semantic colors.** A user-supplied target curve is `var(--status-warn)`
  (yellow); a computed network response is `var(--port-result)` (cyan);
  training-error lines are `var(--status-warn)`. Legends are small line
  swatches plus `var(--text-dim)` text, never color-only prose.
- **Smoothing without invention.** Ordered curves with at least four points
  may render as monotone cubic (Fritsch–Carlson) paths, which cannot
  overshoot the sampled values; fewer points or non-monotonic x fall back to
  polylines. Scatter and field views stay unsmoothed.
- **Honesty of series.** A chart shows only data that actually arrived:
  training RMSD is plotted, validation is plotted only when reported, and a
  log axis omits non-positive values instead of placing them at fake
  positions. `aria-label`s state what the series is (for example "across N
  evaluated updates").
- **Live-update discipline.** Streamed charts accumulate samples in the
  session closure (never in the DOM), with a capacity cap and decimation;
  each progress event re-renders from that accumulated state. Existing metric
  text and structure that tests match stay byte-stable — the chart is added,
  not substituted. Restart and pruning events are dashed vertical markers,
  and the current value is displayed as one large number beside the title.
- **Live and final views agree.** When the final result carries the same
  series (for example `optimization_history`), the result panel renders the
  same chart component, not a second design.

## Verification gates

Before considering a frontend change done:

1. `cd webapp && npm run lint` — eslint over `public/js` with
   `--max-warnings 0`. Inline `<script>` in HTML pages is outside eslint
   coverage; re-read it manually.
2. `cd webapp && npm run test:js` — the full contract-test chain.
3. `cd webapp && npx playwright test` — e2e. The static test server
   (`python3 -m http.server`) can reset connections under parallel workers
   and leave a page unstyled, producing bogus geometry failures; confirm any
   surprising failure with `--workers=1` before believing it.
4. `python3 -m pytest tests/` — repository-level checks, including portable
   paths and deployment contract.

## Maintaining this contract

This page is owned by the `web-workspace` module and registered in
`knowledge/catalogs/contracts.yaml` and `knowledge/manifest.yaml`.

- Any change that alters a convention stated here updates this page (and the
  catalog entry, when owners or tests change) **in the same commit** — the
  standard and the code move together or not at all.
- New design tokens, port names, node geometry constants, or page types are
  added to the owner file first, then reflected here as a rule, not as a
  copied value.
- Deliberate exceptions are recorded where they live, with a comment (for
  example theme-independent tokens in base's `:root`), so the next audit can
  tell "intentional" from "drift" without archaeology.
- Known open items, deliberately not yet standardized: full convergence of
  the `--ax-*` palette onto main tokens, a font/radius/spacing token layer,
  the manual plot-refresh registry in `js/theme.js`, the unused
  `public/js/dist/main.js` bundle, and the editor i18n decision. When one of
  these lands, this page gains the rule in the same change.
