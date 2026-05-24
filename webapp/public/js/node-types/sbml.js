// SBML import/export node types.
//
// sbml-import is a reaction *source* (it owns a reactions-list exactly like
// reaction-network) with an import panel bolted on top: paste/upload SBML,
// click Import, and the parsed reactions populate the list. Reusing the
// reaction-row structure means getReactionsFromNode / serialization / the
// whole downstream pipeline work unchanged.
//
// sbml-export consumes a `reactions` input and downloads an SBML L3 file.

export const SBML_TYPES = {
  'sbml-import': {
    category: 'input',
    headerClass: 'header-input',
    title: 'SBML Import',
    inputs: [],
    outputs: [{ port: 'reactions', label: 'Reactions' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="sbml-import-panel">
          <textarea id="${nodeId}-sbml-input" class="sbml-textarea"
                    rows="4" placeholder="Paste SBML XML here, or choose a file…"></textarea>
          <div class="sbml-import-controls">
            <input type="file" id="${nodeId}-sbml-file" class="sbml-file-input"
                   accept=".xml,.sbml" data-action="loadSbmlFile" data-node="${nodeId}">
            <button class="btn btn-small" data-action="importSbml" data-node="${nodeId}">Import SBML</button>
          </div>
          <div class="sbml-warnings" id="${nodeId}-sbml-warnings" style="display:none;"></div>
        </div>
        <div class="reaction-header">
          <span class="reaction-header-label">Reaction</span>
          <span class="reaction-header-label reaction-header-kd">Kd</span>
          <span class="reaction-header-spacer"></span>
        </div>
        <div id="${nodeId}-reactions-list"></div>
        <button class="btn btn-small" data-action="addReactionRow" data-node="${nodeId}">+ Add Reaction</button>
      `;
    },
    // No seeded rows — the list is filled by Import (or manual + Add Reaction).
    onInit() {},
  },

  'sbml-export': {
    category: 'process',
    headerClass: 'header-viewer',
    title: 'SBML Export',
    inputs: [{ port: 'reactions', label: 'Reactions' }],
    outputs: [],
    defaultWidth: 280,
    createBody(nodeId) {
      return `
        <div class="sbml-export-panel">
          <p class="node-hint">Connect a reaction source, then export to SBML Level 3.</p>
          <button class="btn btn-small" data-action="exportSbml" data-node="${nodeId}">Export SBML ↓</button>
          <div class="sbml-export-status" id="${nodeId}-sbml-export-status"></div>
        </div>
      `;
    },
    onInit() {},
  },
};
