// A deliberately small Markdown renderer for workspace notes.
//
// The note format only needs headings, paragraphs, lists and a few inline
// constructs.  Keep that subset explicit and build DOM nodes directly: user
// input is never parsed as HTML and therefore cannot create event-handler
// attributes or executable elements.

const INLINE_TOKEN = /(`[^`\n]+`|!\[[^\]\n]*\]\([^)\n]*\)|\[[^\]\n]+\]\([^)\n]*\)|\*\*[^*\n]+\*\*|__[^_\n]+__|\*[^*\n]+\*|_[^_\n]+_)/g;
const CONTROL_CHARACTER = /[\u0000-\u001f\u007f]/;
const ABSOLUTE_SCHEME = /^[a-z][a-z0-9+.-]*:/i;
const SAFE_DATA_IMAGE = /^data:image\/(?:png|jpeg|gif|webp|avif);base64,[a-z0-9+/=\s]+$/i;
const RENDERABLE_TAGS = new Set([
  'p', 'h1', 'h2', 'h3', 'ul', 'li', 'strong', 'em', 'code', 'a', 'img', 'br', 'span',
]);
const RENDERABLE_ATTRIBUTES = {
  p: new Set(['class']),
  span: new Set(['class']),
  a: new Set(['href', 'target', 'rel']),
  img: new Set(['alt', 'src', 'loading', 'referrerpolicy']),
};

function text(value) {
  return { type: 'text', value: String(value ?? '') };
}

function element(tag, children = [], attrs = {}) {
  return { type: 'element', tag, attrs, children };
}

function cleanUrlInput(rawValue) {
  const value = String(rawValue ?? '').trim();
  if (!value || CONTROL_CHARACTER.test(value)) return null;
  // Protocol-relative and backslash-relative URLs can silently change hosts.
  if (/^[\\/]{2}/.test(value)) return null;
  return value;
}

/**
 * Resolve a Markdown link to an explicitly allowed navigation target.
 * Relative and same-page links are allowed; executable/data/file schemes are
 * not. The returned value is used with setAttribute(), never HTML interpolation.
 */
export function resolveMarkdownLinkHref(rawHref) {
  const href = cleanUrlInput(rawHref);
  if (!href) return null;

  if (!ABSOLUTE_SCHEME.test(href)) return href;

  try {
    const parsed = new URL(href);
    return ['http:', 'https:', 'mailto:'].includes(parsed.protocol) ? parsed.href : null;
  } catch {
    return null;
  }
}

/**
 * Resolve an image source. Raster data URLs are retained for backwards
 * compatibility, while SVG and every non-image data URL are rejected.
 * Absolute local paths continue to use the backend's local-image endpoint.
 */
export function resolveMarkdownImageSrc(rawSrc) {
  const src = cleanUrlInput(rawSrc);
  if (!src) return null;

  if (/^data:/i.test(src)) return SAFE_DATA_IMAGE.test(src) ? src : null;

  if (/^https?:/i.test(src)) {
    try {
      const parsed = new URL(src);
      return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : null;
    } catch {
      return null;
    }
  }

  let localPath = null;
  if (/^file:/i.test(src)) {
    try {
      const parsed = new URL(src);
      if (parsed.protocol !== 'file:' || (parsed.hostname && parsed.hostname !== 'localhost')) return null;
      localPath = decodeURIComponent(parsed.pathname);
    } catch {
      return null;
    }
  } else if (src.startsWith('/') || src.startsWith('~')) {
    localPath = src;
  }

  if (localPath !== null) {
    return `/api/local-image?path=${encodeURIComponent(localPath)}`;
  }

  // Unknown absolute schemes include javascript:, blob:, ftp:, and custom
  // application schemes. Plain relative paths remain valid note attachments.
  if (ABSOLUTE_SCHEME.test(src)) return null;
  return src;
}

function parseInline(source) {
  const nodes = [];
  let offset = 0;
  // Each recursive call needs its own matcher; sharing lastIndex would let
  // nested emphasis/link labels disturb the caller's scan position.
  const matcher = new RegExp(INLINE_TOKEN.source, INLINE_TOKEN.flags);

  for (let match = matcher.exec(source); match; match = matcher.exec(source)) {
    if (match.index > offset) nodes.push(text(source.slice(offset, match.index)));
    const token = match[0];

    if (token.startsWith('`')) {
      nodes.push(element('code', [text(token.slice(1, -1))]));
    } else if (token.startsWith('![')) {
      const parts = /^!\[([^\]]*)\]\(([^)]*)\)$/.exec(token);
      const alt = parts?.[1] ?? '';
      const src = resolveMarkdownImageSrc(parts?.[2] ?? '');
      if (src) {
        nodes.push(element('img', [], {
          alt,
          src,
          loading: 'lazy',
          referrerpolicy: 'no-referrer',
        }));
      } else {
        nodes.push(element('span', [text(alt ? `[blocked image: ${alt}]` : '[blocked image]')], {
          class: 'text-dim',
        }));
      }
    } else if (token.startsWith('[')) {
      const parts = /^\[([^\]]+)\]\(([^)]*)\)$/.exec(token);
      const label = parts?.[1] ?? token;
      const href = resolveMarkdownLinkHref(parts?.[2] ?? '');
      if (href) {
        nodes.push(element('a', parseInline(label), {
          href,
          target: '_blank',
          rel: 'noopener noreferrer',
        }));
      } else {
        nodes.push(...parseInline(label));
      }
    } else if (token.startsWith('**') || token.startsWith('__')) {
      nodes.push(element('strong', parseInline(token.slice(2, -2))));
    } else {
      nodes.push(element('em', parseInline(token.slice(1, -1))));
    }

    offset = match.index + token.length;
  }

  if (offset < source.length) nodes.push(text(source.slice(offset)));
  return nodes;
}

function paragraphFromLines(lines) {
  const children = [];
  lines.forEach((line, index) => {
    if (index > 0) children.push(element('br'));
    children.push(...parseInline(line));
  });
  return element('p', children);
}

/** Return the small allow-listed render tree used by the DOM renderer. */
export function markdownToSafeTree(markdown) {
  const source = String(markdown ?? '').replace(/\r\n?/g, '\n');
  if (!source) {
    return [element('p', [text('No content yet.')], { class: 'text-dim' })];
  }

  const lines = source.split('\n');
  const blocks = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index];
    if (!line.trim()) {
      index += 1;
      continue;
    }

    const heading = /^(#{1,3})[ \t]+(.*)$/.exec(line);
    if (heading) {
      blocks.push(element(`h${heading[1].length}`, parseInline(heading[2])));
      index += 1;
      continue;
    }

    const firstListItem = /^[ \t]*[-*][ \t]+(.+)$/.exec(line);
    if (firstListItem) {
      const items = [];
      while (index < lines.length) {
        const item = /^[ \t]*[-*][ \t]+(.+)$/.exec(lines[index]);
        if (!item) break;
        items.push(element('li', parseInline(item[1])));
        index += 1;
      }
      blocks.push(element('ul', items));
      continue;
    }

    const paragraphLines = [line];
    index += 1;
    while (index < lines.length && lines[index].trim()) {
      if (/^(?:#{1,3})[ \t]+/.test(lines[index])) break;
      if (/^[ \t]*[-*][ \t]+/.test(lines[index])) break;
      paragraphLines.push(lines[index]);
      index += 1;
    }
    blocks.push(paragraphFromLines(paragraphLines));
  }

  return blocks.length
    ? blocks
    : [element('p', [text('No content yet.')], { class: 'text-dim' })];
}

function renderTreeNode(node, documentRef) {
  if (node.type === 'text') return documentRef.createTextNode(node.value);
  if (node.type !== 'element' || !RENDERABLE_TAGS.has(node.tag)) {
    return documentRef.createTextNode('');
  }

  const rendered = documentRef.createElement(node.tag);
  const allowedAttributes = RENDERABLE_ATTRIBUTES[node.tag] || new Set();
  for (const [name, value] of Object.entries(node.attrs || {})) {
    if (allowedAttributes.has(name)) rendered.setAttribute(name, String(value));
  }
  for (const child of node.children || []) {
    rendered.appendChild(renderTreeNode(child, documentRef));
  }

  if (node.tag === 'img') {
    rendered.style.maxWidth = '100%';
    rendered.style.height = 'auto';
  }
  return rendered;
}

/** Replace a note preview with safe, programmatically-created DOM nodes. */
export function renderSafeMarkdown(container, markdown) {
  const documentRef = container?.ownerDocument || globalThis.document;
  if (!container || !documentRef) return;
  const blocks = markdownToSafeTree(markdown)
    .map(node => renderTreeNode(node, documentRef));
  container.replaceChildren(...blocks);
}
