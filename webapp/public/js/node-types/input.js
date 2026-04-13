import { addReactionRow } from '../model.js';

export const INPUT_TYPES = {
  'reaction-network': {
    category: 'input',
    headerClass: 'header-input',
    title: 'Reaction Network',
    inputs: [],
    outputs: [{ port: 'reactions', label: 'Reactions' }],
    defaultWidth: 280,
    createBody(nodeId) {
      return `
        <div class="reaction-header">
          <span class="reaction-header-label">Reaction</span>
          <span class="reaction-header-label reaction-header-kd">Kd</span>
          <span class="reaction-header-spacer"></span>
        </div>
        <div id="${nodeId}-reactions-list"></div>
        <button class="btn btn-small" data-action="addReactionRow" data-node="${nodeId}">+ Add Reaction</button>
      `;
    },
    onInit(nodeId) {
      addReactionRow(nodeId, 'E + S <-> C_ES', 1e-3);
      addReactionRow(nodeId, 'E + P <-> C_EP', 1e-3);
    },
  },
};
