import { setupAutoUpdate } from '../nodes.js';
import { addReactionRow } from '../model.js';
import { renderAtlasNetworkDefinitionPreview } from '../atlas.js';

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
  'network-id-definition': {
    category: 'input',
    headerClass: 'header-input',
    title: 'Network ID',
    inputs: [],
    outputs: [
      { port: 'reactions', label: 'Reactions' },
      { port: 'atlas-network', label: 'Atlas Net' },
    ],
    defaultWidth: 280,
    createBody(nodeId) {
      return `
        <div class="reaction-header">
          <span class="reaction-header-label">Compressed Network ID</span>
        </div>
        <textarea
          id="${nodeId}-network-id"
          class="auto-update atlas-textarea-singleline"
          placeholder="p3.AQIBAQECAQIBAg.iAnRB.AUE.AAMAAA.1"
        ></textarea>
        <div class="node-info" id="${nodeId}-network-preview"></div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'network-id-definition');
      document.getElementById(nodeId)?.querySelectorAll('.auto-update').forEach(input => {
        const eventType = input.tagName === 'SELECT' || input.type === 'checkbox' ? 'change' : 'input';
        input.addEventListener(eventType, () => renderAtlasNetworkDefinitionPreview(nodeId));
      });
      renderAtlasNetworkDefinitionPreview(nodeId);
    },
  },
};
