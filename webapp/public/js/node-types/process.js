import { setupAutoModelBuild } from '../nodes.js';
import { buildModel } from '../model.js';
import { executeAtlasBuilder } from '../atlas.js';

export const PROCESS_TYPES = {
  'model-builder': {
    category: 'process',
    headerClass: 'header-process',
    title: 'Model Builder',
    inputs: [{ port: 'reactions', label: 'Reactions' }],
    outputs: [{ port: 'model', label: 'Model' }],
    defaultWidth: 260,
    createBody(nodeId) {
      return `
        <div class="node-info" id="${nodeId}-model-info" style="display:none;">
          <pre id="${nodeId}-model-info-text"></pre>
        </div>
        <button class="btn btn-run" data-action="buildModel" data-node="${nodeId}">Run</button>
      `;
    },
    onInit(nodeId) {
      setupAutoModelBuild(nodeId);
    },
    async execute(nodeId) {
      await buildModel(nodeId, { triggerDownstream: false });
    },
  },
  'atlas-builder': {
    category: 'process',
    headerClass: 'header-process',
    title: 'Atlas Builder',
    inputs: [{ port: 'atlas-spec', label: 'Spec' }],
    outputs: [{ port: 'atlas', label: 'Atlas' }],
    defaultWidth: 460,
    defaultHeight: 480,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeAtlasBuilder" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect an Atlas Spec node and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeAtlasBuilder(nodeId);
    },
  },
};
