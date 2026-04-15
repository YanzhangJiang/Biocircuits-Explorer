import { setupAutoUpdate, setupTabNavigation } from '../nodes.js';
import { executeAtlasQueryResult, executeAtlasInverseDesignResult, addAtlasBuilderRow, refreshAtlasQueryDesigner } from '../atlas.js';

export const ATLAS_TYPES = {
  'atlas-spec': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Atlas Spec',
    inputs: [],
    outputs: [{ port: 'atlas-spec', label: 'Spec' }],
    defaultWidth: 420,
    defaultHeight: 620,
    createBody(nodeId) {
      return `
        <div class="tab-nav">
          <button class="tab-btn active" data-tab="basic">Basic</button>
          <button class="tab-btn" data-tab="behavior">Behavior</button>
          <button class="tab-btn" data-tab="enumeration">Enumeration</button>
          <button class="tab-btn" data-tab="explicit">Explicit</button>
        </div>

        <div class="tab-content active" data-tab="basic">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Persistence</div>
            <div class="param-row">
              <label>Source label:</label>
              <input type="text" id="${nodeId}-source-label" value="" class="auto-update" placeholder="atlas_run_001">
            </div>
            <div class="param-row">
              <label>Library label:</label>
              <input type="text" id="${nodeId}-library-label" value="" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>SQLite path:</label>
            </div>
            <textarea
              id="${nodeId}-sqlite-path"
              class="auto-update atlas-textarea atlas-textarea-compact atlas-textarea-singleline"
              placeholder="/absolute/path/to/atlas.sqlite"
            ></textarea>
            <div class="param-row">
              <label>Skip existing:</label>
              <input type="checkbox" id="${nodeId}-skip-existing" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Persist to SQLite:</label>
              <input type="checkbox" id="${nodeId}-persist-sqlite" class="auto-update">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Search Profile</div>
            <div class="param-row">
              <label>Profile name:</label>
              <input type="text" id="${nodeId}-profile-name" value="binding_small_v0" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max base species:</label>
              <input type="number" id="${nodeId}-max-base-species" value="4" min="1" max="12" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max reactions:</label>
              <input type="number" id="${nodeId}-max-reactions" value="5" min="1" max="24" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max support:</label>
              <input type="number" id="${nodeId}-max-support" value="3" min="1" max="12" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="behavior">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Behavior Config</div>
            <div class="param-row">
              <label>Path scope:</label>
              <select id="${nodeId}-path-scope" class="auto-update">
                <option value="feasible">feasible</option>
                <option value="robust">robust</option>
                <option value="all">all graph paths</option>
              </select>
            </div>
            <div class="param-row">
              <label>Min volume:</label>
              <input type="number" id="${nodeId}-min-volume" value="0.01" min="0" step="0.01" class="auto-update">
            </div>
            <div class="param-row">
              <label>Keep singular:</label>
              <input type="checkbox" id="${nodeId}-keep-singular" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Keep non-asym:</label>
              <input type="checkbox" id="${nodeId}-keep-nonasym" class="auto-update">
            </div>
            <div class="param-row">
              <label>Store path records eagerly:</label>
              <input type="checkbox" id="${nodeId}-include-path-records" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="enumeration">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Enumeration</div>
            <div class="param-row">
              <label>Enable:</label>
              <input type="checkbox" id="${nodeId}-enable-enumeration" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Mode:</label>
              <select id="${nodeId}-enum-mode" class="auto-update">
                <option value="pairwise_binding">pairwise_binding</option>
              </select>
            </div>
            <div class="param-row">
              <label>Base species counts:</label>
              <input type="text" id="${nodeId}-base-species-counts" value="2,3" class="auto-update" placeholder="2,3">
            </div>
            <div class="param-row">
              <label>Min reactions:</label>
              <input type="number" id="${nodeId}-min-enum-reactions" value="1" min="1" max="12" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max reactions:</label>
              <input type="number" id="${nodeId}-max-enum-reactions" value="2" min="1" max="12" class="auto-update">
            </div>
            <div class="param-row">
              <label>Limit:</label>
              <input type="number" id="${nodeId}-enum-limit" value="0" min="0" step="1" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="explicit">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Explicit Networks (JSON)</div>
            <textarea
              id="${nodeId}-explicit-networks"
              class="auto-update atlas-textarea"
              placeholder='[
  {
    "label": "monomer_dimer",
    "reactions": ["A + B <-> AB"],
    "input_symbols": ["tA"],
    "output_symbols": ["AB"]
  }
]'
            ></textarea>
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'atlas-spec');
      setupTabNavigation(nodeId);
    },
  },
  'atlas-query-config': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Atlas Query Config',
    inputs: [],
    outputs: [{ port: 'atlas-query', label: 'Query' }],
    defaultWidth: 420,
    defaultHeight: 700,
    createBody(nodeId) {
      return `
        <div class="tab-nav">
          <button class="tab-btn active" data-tab="basic">Basic</button>
          <button class="tab-btn" data-tab="behavior">Behavior</button>
          <button class="tab-btn" data-tab="structure">Structure</button>
          <button class="tab-btn" data-tab="advanced">Advanced</button>
          <button class="tab-btn" data-tab="pipeline">Design</button>
        </div>

        <div class="tab-content active" data-tab="basic">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Atlas Source</div>
            <div class="param-row">
              <label>Prefer persisted atlas:</label>
              <input type="checkbox" id="${nodeId}-prefer-persisted-atlas" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>SQLite override:</label>
            </div>
            <textarea
              id="${nodeId}-query-sqlite-path"
              class="auto-update atlas-textarea atlas-textarea-compact atlas-textarea-singleline"
              placeholder="/absolute/path/to/atlas.sqlite"
            ></textarea>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Search Goal</div>
            <div class="param-row">
              <label>IO pair:</label>
              <input type="text" id="${nodeId}-goal-io" class="auto-update" placeholder="tA -> AB">
            </div>
            <div class="param-row">
              <label>Target motif:</label>
              <input type="text" id="${nodeId}-goal-motif" class="auto-update" placeholder="activation_with_saturation">
            </div>
            <div class="param-row">
              <label>Target exact:</label>
              <input type="text" id="${nodeId}-goal-exact" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Witness path:</label>
              <input type="text" id="${nodeId}-goal-witness" class="auto-update" placeholder="source:0 -> +1 -> sink:+1">
            </div>
            <div class="param-row">
              <label>Must transitions:</label>
              <input type="text" id="${nodeId}-goal-transitions" class="auto-update" placeholder="0->+1,+1->0">
            </div>
            <div class="param-row">
              <label>Forbid regimes:</label>
              <input type="text" id="${nodeId}-goal-forbid-regimes" class="auto-update" placeholder="singular">
            </div>
            <div class="param-row">
              <label>Require robust:</label>
              <input type="checkbox" id="${nodeId}-goal-robust" class="auto-update">
            </div>
            <div class="param-row">
              <label>Require feasible:</label>
              <input type="checkbox" id="${nodeId}-goal-feasible" class="auto-update">
            </div>
            <div class="param-row">
              <label>Min witness volume:</label>
              <input type="number" id="${nodeId}-goal-min-volume" value="" step="0.001" min="0" class="auto-update" placeholder="optional">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Ranking</div>
            <div class="param-row">
              <label>Ranking mode:</label>
              <select id="${nodeId}-ranking-mode" class="auto-update">
                <option value="minimal_first">minimal_first</option>
                <option value="robustness_first">robustness_first</option>
              </select>
            </div>
            <div class="param-row">
              <label>Collapse by network:</label>
              <input type="checkbox" id="${nodeId}-collapse-by-network" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Pareto only:</label>
              <input type="checkbox" id="${nodeId}-pareto-only" class="auto-update">
            </div>
            <div class="param-row">
              <label>Limit:</label>
              <input type="number" id="${nodeId}-query-limit" value="20" min="1" step="1" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="behavior">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Behavior Filters</div>
            <div class="param-row">
              <label>Motif labels:</label>
              <input type="text" id="${nodeId}-motif-labels" class="auto-update" placeholder="activation_with_saturation,biphasic_peak">
            </div>
            <div class="param-row">
              <label>Motif mode:</label>
              <select id="${nodeId}-motif-match-mode" class="auto-update">
                <option value="any">any</option>
                <option value="all">all</option>
              </select>
            </div>
            <div class="param-row">
              <label>Exact labels:</label>
              <input type="text" id="${nodeId}-exact-labels" class="auto-update" placeholder="up-up-down">
            </div>
            <div class="param-row">
              <label>Exact mode:</label>
              <select id="${nodeId}-exact-match-mode" class="auto-update">
                <option value="any">any</option>
                <option value="all">all</option>
              </select>
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">IO + Robustness</div>
            <div class="param-row">
              <label>Inputs:</label>
              <input type="text" id="${nodeId}-input-symbols" class="auto-update" placeholder="tA,tB">
            </div>
            <div class="param-row">
              <label>Outputs:</label>
              <input type="text" id="${nodeId}-output-symbols" class="auto-update" placeholder="AB,ABC">
            </div>
            <div class="param-row">
              <label>Require robust:</label>
              <input type="checkbox" id="${nodeId}-require-robust" class="auto-update">
            </div>
            <div class="param-row">
              <label>Min robust paths:</label>
              <input type="number" id="${nodeId}-min-robust-path-count" value="0" min="0" step="1" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="structure">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Structural Bounds</div>
            <div class="param-row">
              <label>Max base species:</label>
              <input type="number" id="${nodeId}-query-max-base-species" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Max reactions:</label>
              <input type="number" id="${nodeId}-query-max-reactions" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Max support:</label>
              <input type="number" id="${nodeId}-query-max-support" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Max support mass:</label>
              <input type="number" id="${nodeId}-query-max-support-mass" value="" min="0" step="1" class="auto-update" placeholder="optional">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Graph Spec</div>
            <div class="param-row">
              <label>Required regimes:</label>
            </div>
            <textarea
              id="${nodeId}-required-regimes"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"role": "source", "output_order_token": "+1"},
  {"role": "sink", "output_order_token": "0"}
]'
            ></textarea>
            <div class="param-row">
              <label>Forbidden regimes:</label>
            </div>
            <textarea
              id="${nodeId}-forbidden-regimes"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"singular": true}
]'
            ></textarea>
            <div class="param-row">
              <label>Required transitions:</label>
            </div>
            <textarea
              id="${nodeId}-required-transitions"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"transition_token": "+1->0"}
]'
            ></textarea>
            <div class="param-row">
              <label>Forbidden transitions:</label>
            </div>
            <textarea
              id="${nodeId}-forbidden-transitions"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"transition_token": "+1->-1"}
]'
            ></textarea>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Path Spec</div>
            <div class="param-row">
              <label>Required sequences:</label>
            </div>
            <textarea
              id="${nodeId}-required-path-sequences"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  [
    {"role": "source", "output_order_token": "+1"},
    {"role": "sink", "output_order_token": "0"}
  ]
]'
            ></textarea>
            <div class="param-row">
              <label>Forbid singular:</label>
              <input type="checkbox" id="${nodeId}-forbid-singular-on-witness" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max path length:</label>
              <input type="number" id="${nodeId}-max-witness-path-length" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Polytope Spec</div>
            <div class="param-row">
              <label>Require feasible:</label>
              <input type="checkbox" id="${nodeId}-require-witness-feasible" class="auto-update">
            </div>
            <div class="param-row">
              <label>Require robust:</label>
              <input type="checkbox" id="${nodeId}-require-witness-robust" class="auto-update">
            </div>
            <div class="param-row">
              <label>Min volume mean:</label>
              <input type="number" id="${nodeId}-min-witness-volume-mean" value="" step="0.001" min="0" class="auto-update" placeholder="optional">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="advanced">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Condition Builder</div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Required regimes</span>
                <button type="button" class="btn btn-small" data-action="addAtlasBuilderRow" data-node="${nodeId}" data-container="builder-required-regimes" data-kind="regime">+ Add</button>
              </div>
              <div id="${nodeId}-builder-required-regimes" class="atlas-builder-list"></div>
            </div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Forbidden regimes</span>
                <button type="button" class="btn btn-small" data-action="addAtlasBuilderRow" data-node="${nodeId}" data-container="builder-forbidden-regimes" data-kind="regime">+ Add</button>
              </div>
              <div id="${nodeId}-builder-forbidden-regimes" class="atlas-builder-list"></div>
            </div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Required transitions</span>
                <button type="button" class="btn btn-small" data-action="addAtlasBuilderRow" data-node="${nodeId}" data-container="builder-required-transitions" data-kind="transition">+ Add</button>
              </div>
              <div id="${nodeId}-builder-required-transitions" class="atlas-builder-list"></div>
            </div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Witness stages</span>
                <button type="button" class="btn btn-small" data-action="addAtlasBuilderRow" data-node="${nodeId}" data-container="builder-witness-sequence" data-kind="regime">+ Add</button>
              </div>
              <div id="${nodeId}-builder-witness-sequence" class="atlas-builder-list"></div>
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Preview</div>
            <div id="${nodeId}-behavior-sketch"></div>
            <pre id="${nodeId}-query-preview" class="atlas-query-preview"></pre>
          </div>
        </div>

        <div class="tab-content" data-tab="pipeline">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Inverse Design</div>
            <div class="text-dim">These settings are optional. Atlas Preview and Atlas Search ignore them; Atlas Inverse Design uses them.</div>
            <div class="param-row">
              <label>Source label:</label>
              <input type="text" id="${nodeId}-inverse-source-label" value="inverse_design_run" class="auto-update">
            </div>
            <div class="param-row">
              <label>Skip existing:</label>
              <input type="checkbox" id="${nodeId}-inverse-skip-existing" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Build library if missing:</label>
              <input type="checkbox" id="${nodeId}-inverse-build-library-if-missing" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Allow duplicate atlas:</label>
              <input type="checkbox" id="${nodeId}-allow-duplicate-atlas" class="auto-update">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Refinement</div>
            <div class="param-row">
              <label>Enable refinement:</label>
              <input type="checkbox" id="${nodeId}-refinement-enabled" class="auto-update">
            </div>
            <div class="param-row">
              <label>Top k:</label>
              <input type="number" id="${nodeId}-refinement-top-k" value="3" min="1" step="1" class="auto-update">
            </div>
            <div class="param-row">
              <label>Trials:</label>
              <input type="number" id="${nodeId}-refinement-trials" value="6" min="1" step="1" class="auto-update">
            </div>
            <div class="param-row">
              <label>Points per trace:</label>
              <input type="number" id="${nodeId}-refinement-n-points" value="200" min="20" step="10" class="auto-update">
            </div>
            <div class="param-row">
              <label>Include traces:</label>
              <input type="checkbox" id="${nodeId}-refinement-include-traces" class="auto-update">
            </div>
            <div class="param-row">
              <label>Rerank by refinement:</label>
              <input type="checkbox" id="${nodeId}-refinement-rerank" checked class="auto-update">
            </div>
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'atlas-query-config');
      setupTabNavigation(nodeId);
      refreshAtlasQueryDesigner(nodeId);
    },
  },
  'atlas-query-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Atlas Search Result',
    inputs: [{ port: 'atlas', label: 'Atlas' }, { port: 'atlas-query', label: 'Query' }],
    outputs: [],
    defaultWidth: 640,
    defaultHeight: 540,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeAtlasQueryResult" data-node="${nodeId}">Search Atlas</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect an Atlas Preview Builder and an Atlas Query Config node, or provide a SQLite atlas path in the query config. For pure preview, use Atlas Preview alone.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeAtlasQueryResult(nodeId);
    },
  },
  'atlas-inverse-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Atlas Inverse Design',
    inputs: [
      { port: 'atlas-spec', label: 'Spec' },
      { port: 'atlas', label: 'Atlas' },
      { port: 'atlas-query', label: 'Query' },
    ],
    outputs: [],
    defaultWidth: 700,
    defaultHeight: 620,
    createBody(nodeId) {
      return `
        <button class="btn btn-primary" data-action="executeAtlasInverseDesignResult" data-node="${nodeId}">Run Inverse Design</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect an Atlas Spec and Atlas Query Config to run the support-first inverse-design pipeline. You can also connect an Atlas Preview Builder or provide a SQLite path in the query config for reuse.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeAtlasInverseDesignResult(nodeId);
    },
  },
};
