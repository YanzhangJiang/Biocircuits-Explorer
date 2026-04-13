import { nodeRegistry } from '../state.js';

// ===== Markdown-note helpers (module-private) =====

function renderMarkdown(nodeId) {
  const textarea = document.getElementById(`${nodeId}-markdown`);
  const preview = document.getElementById(`${nodeId}-preview`);

  if (!textarea || !preview) return;

  const markdown = textarea.value;
  preview.innerHTML = simpleMarkdownToHTML(markdown);
}

function simpleMarkdownToHTML(markdown) {
  if (!markdown) return '<p class="text-dim">No content yet.</p>';

  let html = markdown;

  // Escape HTML
  html = html.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Headers
  html = html.replace(/^### (.*$)/gim, '<h3>$1</h3>');
  html = html.replace(/^## (.*$)/gim, '<h2>$1</h2>');
  html = html.replace(/^# (.*$)/gim, '<h1>$1</h1>');

  // Bold
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/__(.+?)__/g, '<strong>$1</strong>');

  // Italic
  html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
  html = html.replace(/_(.+?)_/g, '<em>$1</em>');

  // Code inline
  html = html.replace(/`(.+?)`/g, '<code>$1</code>');

  // Links
  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');

  // Lists
  html = html.replace(/^\* (.+)$/gim, '<li>$1</li>');
  html = html.replace(/^- (.+)$/gim, '<li>$1</li>');
  html = html.replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>');

  // Line breaks
  html = html.replace(/\n\n/g, '</p><p>');
  html = html.replace(/\n/g, '<br>');

  // Wrap in paragraphs
  if (!html.startsWith('<h') && !html.startsWith('<ul>')) {
    html = '<p>' + html + '</p>';
  }

  return html;
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
