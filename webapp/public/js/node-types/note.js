import { nodeRegistry } from '../state.js';
import { renderSafeMarkdown } from '../safe-markdown.js';

// ===== Markdown-note helpers (module-private) =====

function renderMarkdown(nodeId) {
  const textarea = document.getElementById(`${nodeId}-markdown`);
  const preview = document.getElementById(`${nodeId}-preview`);

  if (!textarea || !preview) return;

  renderSafeMarkdown(preview, textarea.value);
}

export function switchNoteTab(nodeId, tab) {
  const editArea = document.getElementById(`${nodeId}-edit-area`);
  const previewArea = document.getElementById(`${nodeId}-preview-area`);
  const node = document.getElementById(nodeId);

  if (!editArea || !previewArea || !node) return;

  // Update tab buttons
  node.querySelectorAll('.note-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });

  if (tab === 'edit') {
    editArea.style.display = '';
    previewArea.style.display = 'none';
  } else {
    editArea.style.display = 'none';
    previewArea.style.display = '';
    renderMarkdown(nodeId);
  }
}

export const NOTE_TYPES = {
  'markdown-note': {
    category: 'note',
    headerClass: 'header-note',
    title: 'Markdown Note',
    inputs: [],
    outputs: [],
    defaultWidth: 400,
    defaultHeight: 300,
    createBody(nodeId) {
      return `
        <div class="note-tabs">
          <button class="note-tab active" data-tab="edit" data-action="switchNoteTab" data-node="${nodeId}">Edit</button>
          <button class="note-tab" data-tab="preview" data-action="switchNoteTab" data-node="${nodeId}">Preview</button>
        </div>
        <div class="note-edit-area" id="${nodeId}-edit-area">
          <textarea id="${nodeId}-markdown" class="markdown-editor" placeholder="Write your markdown notes here...

# Example
- Bullet point
- **Bold text**
- *Italic text*
- [Link](https://example.com)
"></textarea>
        </div>
        <div class="note-preview-area" id="${nodeId}-preview-area" style="display:none;">
          <div id="${nodeId}-preview" class="markdown-preview"></div>
        </div>
      `;
    },
    onInit(nodeId) {
      const textarea = document.getElementById(`${nodeId}-markdown`);
      if (textarea) {
        // Auto-save on input
        textarea.addEventListener('input', () => {
          const info = nodeRegistry[nodeId];
          if (info) {
            info.data = info.data || {};
            info.data.markdown = textarea.value;
            // Update preview if in preview mode
            const previewArea = document.getElementById(`${nodeId}-preview-area`);
            if (previewArea && previewArea.style.display !== 'none') {
              renderMarkdown(nodeId);
            }
          }
        });
      }
    },
  },
};
