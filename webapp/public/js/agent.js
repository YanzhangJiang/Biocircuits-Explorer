// Biocircuits Explorer — AI Import Agent.
//
// All requests go through Anthropic's Messages API shape. Two providers
// are routed:
//   - 'anthropic'  → https://api.anthropic.com/v1/messages
//   - 'deepseek'   → https://api.deepseek.com/anthropic/v1/messages (V4 Pro / V4 Flash
//                    speak the Anthropic-compatible Messages API; PDFs and
//                    document content blocks work the same way)
//
// Requests are issued directly from the browser. The host app never stores
// API keys or proxies the call; the user supplies key + model at run time
// inside the AI Import node body.

const ANTHROPIC_ENDPOINT = 'https://api.anthropic.com/v1/messages';
const ANTHROPIC_VERSION = '2023-06-01';
// DeepSeek exposes an Anthropic-compatible Messages API. Update this if
// DeepSeek moves the endpoint; full URL becomes BASE + '/v1/messages'.
const DEEPSEEK_ANTHROPIC_BASE = 'https://api.deepseek.com/anthropic';
const DEFAULT_MAX_TOKENS = 16000;

// Allowed downstream analysis identifiers the agent may recommend. The
// auto-spawn layer expands these into the appropriate node chain. Keep
// this list in sync with `chainMap` in agent-node.js.
export const ALLOWED_ANALYSES = [
  'model-summary',       // tabular summary of N / L / species
  'vertices-table',      // ROP vertex enumeration
  'regime-graph',        // adjacency graph of regimes
  'siso-analysis',       // single-input single-output paths
  'parameter-scan-1d',   // 1D parameter sweep
  'parameter-scan-2d',   // 2D parameter sweep
  'rop-cloud',           // point-cloud sampling of ROP
  'rop-polyhedron',      // explicit polyhedron geometry
  'fret-heatmap',        // FRET observable heatmap
];

const SYSTEM_PROMPT = `You are an extraction agent for Biocircuits Explorer, a tool that analyses EQUILIBRIUM binding networks (Reaction Order Polyhedra). Read the user-provided paper / notebook / text and extract every distinct binding network it describes. A single paper often contains multiple worked examples (different figures, different supplementary appendices) — emit each as a separate network entry.

OUTPUT FORMAT — your entire reply MUST be a single JSON object, starting with '{' as the very first character and ending with '}' as the very last character. NO markdown fences, NO prose preamble ("Here is the extracted…"), NO trailing commentary, NO chain-of-thought, NO explanations outside the JSON. If you have nothing to say, put it inside the JSON "summary" or "warnings" fields. Anything outside the JSON braces will cause the host application to fail.

Schema:
{
  "networks": [
    {
      "name": "Short label, e.g. 'Figure 2A: two-step TF binding'",
      "reactions": [
        {"rule": "A + B <-> AB", "kd": 1.0e-6, "kd_units": "M", "source": "short quote or section reference", "confidence": "high|medium|low"}
      ],
      "recommended_analyses": [
        "model-summary",
        "regime-graph",
        {"name": "siso-analysis"},
        {"name": "parameter-scan-1d", "output_expression": "D_1_2", "scan_param": "q_M_1"},
        {"name": "parameter-scan-2d", "output_expression": "D_1_2 + D_2_2", "scan_param": "q_M_1", "scan_param_2": "q_M_2"}
      ],
      "notes": "One short paragraph: what this network represents, what the paper does with it, what the user should double-check."
    }
  ],
  "warnings": ["one short string per item that was discarded, approximated, or unit-converted"],
  "summary": "One short paragraph describing the overall paper, what fraction translates cleanly, and any caveats."
}

REACTION SYNTAX:
- ASCII only. Use the literal token "<->" between left and right side.
- Left side: one or more reactant species joined by " + ". Right side: a single complex.
- Species names: alphanumeric + underscore, no whitespace. Examples: A, B, C_ES, P_DNA, TF_DNA_Co.
- Each reaction must be a reversible binding step (two or more species combining into one complex, or its dissociation).

Kd:
- Positive float in molar (M). If the paper uses nM, uM, mM — convert and add a warning noting the conversion.
- If only Ka (association constant) is given, return Kd = 1 / Ka and note in warnings.
- If only k_on and k_off are given, return Kd = k_off / k_on and note in warnings.
- If the reaction is clearly an equilibrium binding step but the paper does NOT report any Kd / Ka / k_on/k_off value, emit kd: 0.001 as a placeholder, mark confidence as "low", AND add a warning that names the reaction (e.g. "Kd not reported for A + B <-> AB; defaulted to 1e-3 M"). The user will edit the placeholder by hand — but we still emit the reaction so the topology survives.

WHAT YOU CAN TRANSLATE:
- Explicit equilibrium binding reactions with Kd, Ka, or k_on/k_off.
- Multi-step binding cascades when each step is written out (e.g. P + DNA <-> P_DNA, then P_DNA + Co <-> P_DNA_Co).
- PARAMETRIC REACTION GENERATORS (code libraries, factory functions, loops that emit reactions like \`for i,j in combinations: reactions.append(f"M_{i+1} + M_{j+1} <=> D_{i+1}_{j+1}")\`). When the input is a code library rather than a worked example, mentally execute the generator for the smallest reasonable parameter that yields 2–5 reactions (e.g. m=2 or m=3 for n-monomer dimerisation systems) and emit the EXPANDED concrete reactions. Mark every reaction confidence "low", use kd=0.001 placeholders, and add a warning naming the parameter choice (e.g. "Expanded make_nXn_dimer_reactions for m=2; library supports arbitrary m. Kd values are placeholders."). Each distinct sensible parameter value can become its own network entry (e.g. one network for m=2, another for m=3) if that helps the user. The principle: returning an instantiated network the user can edit is more useful than returning nothing.

WHAT YOU CANNOT TRANSLATE — discard with a warning, do NOT fabricate:
- Hill functions or phenomenological dose-response curves with no underlying binding scheme.
- Kinetic ODEs with no equilibrium interpretation and no rate constants.
- Catalytic / enzymatic turnover steps (only the equilibrium pre-binding step can be kept; the catalytic step must be discarded with a warning).

RECOMMENDED ANALYSES — pick from this closed set ONLY, no others:
  model-summary       Tabular summary of species, totals, binding constants. Cheap, recommend for every network.
  vertices-table      Enumeration of ROP vertices. Recommend when network has >= 2 binding steps.
  regime-graph        Adjacency graph of structural regimes. Recommend when the paper discusses qualitative behaviour modes.
  siso-analysis       Single-input single-output paths. Recommend when paper studies dose-response, transfer function, or input-output mapping.
  parameter-scan-1d   1D parameter sweep. Recommend when paper varies a single Kd or total concentration.
  parameter-scan-2d   2D parameter sweep. Recommend when paper has a 2D phase diagram or two-variable exploration.
  rop-cloud           Point-cloud sampling of the polyhedron. Recommend when paper does Monte-Carlo or scatter exploration.
  rop-polyhedron      Explicit polyhedron geometry. Recommend when paper discusses geometric / structural properties of the response surface.
  fret-heatmap        FRET observable heatmap. Recommend only when paper explicitly uses FRET / fluorescence readout.

Each network MUST include at least "model-summary". Add others only when the paper genuinely motivates them — do not pad.

ANALYSIS CONFIGURATION — for the analyses below, the entry MUST be an object (not a bare string) and MUST include the configuration field listed. Without it, the spawned chain renders blank or falls back to an arbitrary default.

  parameter-scan-1d   { "name": "parameter-scan-1d", "output_expression": "<species or formula>", "scan_param": "<q_... or K_... symbol>" }
  parameter-scan-2d   { "name": "parameter-scan-2d", "output_expression": "<species or formula>", "scan_param": "<q_... or K_... symbol>", "scan_param_2": "<another q_... or K_... symbol>" }
  rop-cloud           { "name": "rop-cloud", "output_expression": "<target species>" }

  - output_expression for scan analyses: a literal species name from the reactions you emitted (e.g. "D_1_2"), or a simple sum/product formula over those species (e.g. "D_1_1 + D_1_2"). The species names MUST appear in your "reactions" list.
  - output_expression for rop-cloud: a single literal target species from the reactions you emitted, not a formula.
  - scan_param / scan_param_2: a total-concentration symbol like q_M_1 (one per monomer species) or a binding constant K_<i>_<j>. Pick one that's meaningful given the paper's analysis — usually a monomer total.
  - For other analyses (model-summary, regime-graph, siso-analysis, vertices-table, rop-polyhedron, fret-heatmap) a bare string is acceptable, but the object form { "name": "..." } also works.

If you recommend a scan or rop-cloud analysis but cannot guess the required configuration from the paper, OMIT that analysis entirely rather than emit one with a blank field.

HONESTY:
- Every approximation, unit conversion, discarded model, or generator-expansion choice goes in "warnings".
- If the input genuinely contains no binding reactions and no parametric generator (e.g., it is a pure plotting / data-loading notebook), return networks: [] and explain why in summary. Returning zero is better than fabricating in that case.
- BUT: a code library that defines a reaction generator IS extractable — instantiate it (see WHAT YOU CAN TRANSLATE above). Do not return empty just because the library lacks specific worked examples.
- Confidence "low" means you found something binding-shaped but had to guess about either the species identities or the Kd, OR you expanded a parametric generator with placeholder values.`;


// ===== File helpers =====

// Cap how much we'll send per text-like file so a single huge notebook
// can't blow the request budget. Truncation is announced in warnings.
const PER_TEXT_FILE_LIMIT = 200_000;       // ~50K tokens
const MAX_TEXT_FILE_BYTES = 5_000_000;     // read at most this much from one text-like file
const MAX_PDF_FILE_BYTES = 20_000_000;     // direct PDF uploads get expensive quickly
const MAX_ZIP_ARCHIVE_BYTES = 50_000_000;
const MAX_ZIP_DEPTH = 2;
const MAX_ZIP_ENTRIES = 200;
const MAX_ZIP_UNCOMPRESSED_BYTES = 25_000_000;
const TOTAL_INPUT_SOFT_LIMIT = 5_000_000;  // warn past this
const TOTAL_INPUT_HARD_LIMIT = 25_000_000; // skip material beyond this request budget

const TEXT_EXTENSIONS = new Set([
  'txt', 'md', 'markdown', 'rst', 'tex',
  'py', 'ipynb', 'jl', 'm', 'r', 'rb', 'pl', 'lua',
  'js', 'mjs', 'ts', 'tsx', 'jsx',
  'c', 'cc', 'cpp', 'h', 'hpp', 'cs', 'java', 'go', 'rs', 'swift', 'kt',
  'json', 'yaml', 'yml', 'toml', 'ini', 'cfg', 'env',
  'csv', 'tsv', 'xml', 'html', 'htm', 'css', 'scss',
  'sh', 'bash', 'zsh', 'fish', 'ps1',
  'log', 'lock', 'gitignore', 'dockerfile',
]);

function fileExt(name) {
  const m = /\.([^.\\/]+)$/.exec(name);
  return m ? m[1].toLowerCase() : '';
}

function isText(file) { return TEXT_EXTENSIONS.has(fileExt(file.name)); }
function isPdf(file)  { return file?.type === 'application/pdf' || fileExt(file.name) === 'pdf'; }
function isIpynb(file){ return fileExt(file.name) === 'ipynb'; }
function isZip(file)  { return /\.(zip)$/i.test(file.name) || file?.type === 'application/zip'; }

function normalizePathForFilter(path) {
  return String(path || '').replace(/\\/g, '/');
}

function isSystemTrashPath(path) {
  const p = normalizePathForFilter(path);
  return /(^|\/)__MACOSX\//.test(p) || /(^|\/)\.DS_Store$/.test(p);
}

function isSensitivePath(path) {
  const p = normalizePathForFilter(path).toLowerCase();
  const base = p.split('/').pop() || p;
  if (/(^|\/)(\.git|\.hg|\.svn|node_modules|__pycache__|\.venv|venv)(\/|$)/.test(p)) return true;
  if (/^\.env($|[.\-_])/.test(base) || base === '.envrc' || base.endsWith('.env')) return true;
  if (/^(aws-)?credentials(\..*)?$/.test(base)) return true;
  if (/^known_hosts$/.test(base)) return true;
  if (/^id_(rsa|ed25519|ecdsa)([.\-_].*)?$/.test(base)) return true;
  if (/(client[_-]?secret|secret|access[_-]?token|refresh[_-]?token|jwt)/.test(base)) return true;
  if (/\.(pem|key|p12|pfx|jks|keystore|crt|cer|cert|ppk)$/.test(base)) return true;
  if (base === 'package-lock.json' || base === 'yarn.lock' || base === 'pnpm-lock.yaml' || base.endsWith('.lock')) return true;
  return false;
}

function skipFile(label, reason, meta) {
  meta.warnings.push(`Skipped ${label}: ${reason}.`);
  meta.skipped.push(label);
}

function reserveRequestBytes(label, bytes, meta) {
  const n = Number(bytes) || 0;
  if (meta.sentBytes + n > TOTAL_INPUT_HARD_LIMIT) {
    skipFile(label, `request input budget would exceed ${(TOTAL_INPUT_HARD_LIMIT / 1024 / 1024).toFixed(0)} MB`, meta);
    return false;
  }
  meta.sentBytes += n;
  return true;
}

async function readTextPrefix(file, label, meta) {
  if (file.size > MAX_TEXT_FILE_BYTES) {
    meta.warnings.push(`${label} truncated before reading to first ${(MAX_TEXT_FILE_BYTES / 1024 / 1024).toFixed(1)} MB (full file ${(file.size / 1024 / 1024).toFixed(1)} MB).`);
    return await file.slice(0, MAX_TEXT_FILE_BYTES).text();
  }
  return await file.text();
}

async function readFileAsBase64(file) {
  const buf = await file.arrayBuffer();
  const bytes = new Uint8Array(buf);
  let binary = '';
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

function extractIpynbText(rawJson) {
  let nb;
  try { nb = JSON.parse(rawJson); }
  catch { throw new Error('Notebook is not valid JSON'); }
  const cells = Array.isArray(nb.cells) ? nb.cells : [];
  const parts = [];
  cells.forEach((cell, i) => {
    const src = Array.isArray(cell.source) ? cell.source.join('') : (cell.source || '');
    if (!src.trim()) return;
    if (cell.cell_type === 'markdown') parts.push(`# [markdown cell ${i}]\n${src}`);
    else if (cell.cell_type === 'code') parts.push(`# [code cell ${i}]\n${src}`);
  });
  return parts.join('\n\n');
}

function truncatedTextBlock(label, raw, limit, meta) {
  if (raw.length <= limit) return { type: 'text', text: `[File: ${label}]\n\n${raw}` };
  meta.warnings.push(`${label} truncated to first ${Math.round(limit/1000)}K chars (full file ${Math.round(raw.length/1000)}K).`);
  return { type: 'text', text: `[File: ${label} — TRUNCATED to first ${limit} chars]\n\n${raw.slice(0, limit)}` };
}

// Walk one file. For zips, recurses through entries.
// `displayPath` is the user-visible name (may include zip-internal path).
async function dispatchFile(file, displayPath, blocks, meta, zipState = { entries: 0, bytes: 0 }, zipDepth = 0) {
  const label = displayPath || file.name;
  const sizeNote = `${(file.size / 1024).toFixed(1)} KB`;
  meta.totalBytes += file.size;

  if (isSystemTrashPath(label)) return;
  if (isSensitivePath(label)) {
    skipFile(label, 'sensitive or irrelevant local/project file name', meta);
    return;
  }

  if (isZip(file)) {
    if (zipDepth >= MAX_ZIP_DEPTH) {
      skipFile(label, `zip nesting exceeds ${MAX_ZIP_DEPTH} levels`, meta);
      return;
    }
    if (file.size > MAX_ZIP_ARCHIVE_BYTES) {
      skipFile(label, `zip archive exceeds ${(MAX_ZIP_ARCHIVE_BYTES / 1024 / 1024).toFixed(0)} MB`, meta);
      return;
    }
    if (typeof window === 'undefined' || !window.JSZip) {
      skipFile(label, 'JSZip library not loaded', meta);
      return;
    }
    let zip;
    try {
      zip = await window.JSZip.loadAsync(await file.arrayBuffer());
    } catch (e) {
      skipFile(label, `not a valid zip (${e.message || e})`, meta);
      return;
    }
    const entries = Object.entries(zip.files)
      .filter(([_, entry]) => !entry.dir)
      .sort(([a], [b]) => a.localeCompare(b));
    if (entries.length > MAX_ZIP_ENTRIES) {
      meta.warnings.push(`${label} has ${entries.length} zip entries; only the first ${MAX_ZIP_ENTRIES} safe entries will be considered.`);
    }
    meta.included.push(`${label} (zip with ${entries.length} entries)`);
    for (const [entryPath, entry] of entries) {
      if (zipState.entries >= MAX_ZIP_ENTRIES) break;
      if (isSystemTrashPath(entryPath)) continue;
      if (isSensitivePath(entryPath)) {
        skipFile(`${label}/${entryPath}`, 'sensitive or irrelevant local/project file name', meta);
        continue;
      }
      const knownSize = Number(entry?._data?.uncompressedSize || 0);
      if (knownSize && zipState.bytes + knownSize > MAX_ZIP_UNCOMPRESSED_BYTES) {
        skipFile(`${label}/${entryPath}`, `zip expanded content would exceed ${(MAX_ZIP_UNCOMPRESSED_BYTES / 1024 / 1024).toFixed(0)} MB`, meta);
        continue;
      }
      const blob = await entry.async('blob');
      if (zipState.bytes + blob.size > MAX_ZIP_UNCOMPRESSED_BYTES) {
        skipFile(`${label}/${entryPath}`, `zip expanded content would exceed ${(MAX_ZIP_UNCOMPRESSED_BYTES / 1024 / 1024).toFixed(0)} MB`, meta);
        continue;
      }
      zipState.entries += 1;
      zipState.bytes += blob.size;
      const innerFile = new File([blob], entryPath, { type: '' });
      await dispatchFile(innerFile, `${label}/${entryPath}`, blocks, meta, zipState, zipDepth + 1);
    }
    return;
  }

  if (isPdf(file)) {
    if (file.size > MAX_PDF_FILE_BYTES) {
      skipFile(label, `PDF exceeds ${(MAX_PDF_FILE_BYTES / 1024 / 1024).toFixed(0)} MB`, meta);
      return;
    }
    if (!reserveRequestBytes(label, file.size, meta)) return;
    const data = await readFileAsBase64(file);
    blocks.push({
      type: 'document',
      source: { type: 'base64', media_type: 'application/pdf', data },
    });
    meta.included.push(`${label} (PDF, ${sizeNote})`);
    return;
  }

  if (isIpynb(file)) {
    if (file.size > MAX_TEXT_FILE_BYTES) {
      skipFile(label, `notebook exceeds ${(MAX_TEXT_FILE_BYTES / 1024 / 1024).toFixed(1)} MB; export a smaller notebook or paste the relevant cells`, meta);
      return;
    }
    const raw = await readTextPrefix(file, label, meta);
    let extracted;
    try { extracted = extractIpynbText(raw); }
    catch (e) {
      skipFile(label, e.message, meta);
      return;
    }
    const block = truncatedTextBlock(`${label} (Jupyter notebook source)`, extracted, PER_TEXT_FILE_LIMIT, meta);
    if (!reserveRequestBytes(label, block.text.length, meta)) return;
    blocks.push(block);
    meta.included.push(`${label} (notebook, ${sizeNote})`);
    return;
  }

  if (isText(file) || file.type.startsWith('text/')) {
    const raw = await readTextPrefix(file, label, meta);
    const block = truncatedTextBlock(label, raw, PER_TEXT_FILE_LIMIT, meta);
    if (!reserveRequestBytes(label, block.text.length, meta)) return;
    blocks.push(block);
    meta.included.push(`${label} (text, ${sizeNote})`);
    return;
  }

  skipFile(label, `unsupported type "${file.type || fileExt(file.name) || 'unknown'}"`, meta);
}

// ===== Provider: Anthropic =====

async function buildAnthropicContent({ text, files }) {
  const blocks = [];
  const meta = { included: [], skipped: [], warnings: [], totalBytes: 0, sentBytes: 0 };
  const zipState = { entries: 0, bytes: 0 };
  for (const f of (files || [])) {
    await dispatchFile(f, f.name, blocks, meta, zipState, 0);
  }
  if (text && text.trim()) {
    const pasted = text.trim();
    const sentText = pasted.length > PER_TEXT_FILE_LIMIT ? pasted.slice(0, PER_TEXT_FILE_LIMIT) : pasted;
    if (pasted.length > PER_TEXT_FILE_LIMIT) {
      meta.warnings.push(`Pasted text truncated to first ${Math.round(PER_TEXT_FILE_LIMIT / 1000)}K chars (full text ${Math.round(pasted.length / 1000)}K).`);
    }
    const block = { type: 'text', text: `[Pasted text]\n\n${sentText}` };
    meta.totalBytes += pasted.length;
    if (reserveRequestBytes('pasted text', block.text.length, meta)) {
      blocks.push(block);
    }
  }
  if (blocks.length === 0) throw new Error('Provide at least one supported, non-sensitive file or some pasted text');
  if (meta.sentBytes > TOTAL_INPUT_SOFT_LIMIT) {
    meta.warnings.push(`Input is large (${(meta.sentBytes / 1024 / 1024).toFixed(1)} MB sent); the API may reject or truncate.`);
  }
  blocks.push({
    type: 'text',
    text: 'Extract every distinct equilibrium binding network from ALL of the material above. If the input spans multiple files, treat them as one cohesive project — examples may be split across files (e.g., a generator library + a notebook that instantiates it). Respond with the JSON object only — no prose, no fences.',
  });
  return { blocks, meta };
}

async function callAnthropic({ apiKey, model, baseUrl, text, files, signal }) {
  const { blocks: content, meta: inputMeta } = await buildAnthropicContent({ text, files });
  const endpoint = baseUrl || ANTHROPIC_ENDPOINT;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': ANTHROPIC_VERSION,
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify({
      model,
      max_tokens: DEFAULT_MAX_TOKENS,
      system: SYSTEM_PROMPT,
      messages: [{ role: 'user', content }],
    }),
    signal,
  });
  if (!response.ok) {
    const detail = await readErrorBody(response);
    throw new Error(`Anthropic API ${response.status}: ${detail}`);
  }
  const data = await response.json();
  const respBlocks = Array.isArray(data?.content) ? data.content.filter(b => b?.type === 'text') : [];
  const raw = respBlocks.map(b => b.text || '').join('\n').trim();
  return {
    raw,
    meta: {
      id: data.id, model: data.model, stop_reason: data.stop_reason, usage: data.usage,
      input: inputMeta,
    },
  };
}

// ===== Shared parsing =====

async function readErrorBody(response) {
  try {
    const errBody = await response.json();
    return errBody?.error?.message || JSON.stringify(errBody);
  } catch {
    try { return await response.text(); } catch { return response.statusText || ''; }
  }
}

// Find the largest balanced {...} substring starting from the first '{'.
// Skips braces that appear inside JSON string literals (handles escapes).
function extractBalancedJson(s) {
  const start = s.indexOf('{');
  if (start < 0) return null;
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < s.length; i++) {
    const ch = s[i];
    if (inString) {
      if (escape) { escape = false; continue; }
      if (ch === '\\') { escape = true; continue; }
      if (ch === '"') { inString = false; }
      continue;
    }
    if (ch === '"') { inString = true; continue; }
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) return s.slice(start, i + 1);
    }
  }
  // Unterminated — return as much as we have so error surface is informative.
  return s.slice(start);
}

function tryParseJson(raw) {
  let s = raw.trim();
  // Strip any markdown code fence (```json, ```, ```python, etc).
  const fence = s.match(/```[a-zA-Z0-9_-]*\s*([\s\S]*?)```/);
  if (fence) s = fence[1].trim();
  // Some models smart-quote keys/values; normalise the common offenders.
  s = s.replace(/[“”]/g, '"').replace(/[‘’]/g, "'");
  // First attempt: parse as-is (post-fence-strip).
  try { return JSON.parse(s); } catch { /* fall through */ }
  // Second attempt: pull the largest balanced top-level object.
  const balanced = extractBalancedJson(s);
  if (balanced) {
    try { return JSON.parse(balanced); } catch { /* fall through */ }
  }
  // Final throw includes a parseable error.
  return JSON.parse(s);
}

function normalizeReaction(r) {
  const rule = typeof r?.rule === 'string' ? r.rule.trim() : '';
  const kd = Number(r?.kd);
  if (!rule || !Number.isFinite(kd) || kd <= 0) return null;
  return {
    rule,
    kd,
    kd_units: r?.kd_units || 'M',
    source: r?.source || '',
    confidence: r?.confidence || 'medium',
  };
}

const REQUIRED_ANALYSIS_FIELDS = {
  'parameter-scan-1d': ['output_expression', 'scan_param'],
  'parameter-scan-2d': ['output_expression', 'scan_param', 'scan_param_2'],
  'rop-cloud': ['output_expression'],
};

function normalizeAnalysisEntry(a, warnings, networkName) {
  const label = networkName || 'network';
  if (typeof a === 'string') {
    if (!ALLOWED_ANALYSES.includes(a)) {
      warnings.push(`Dropped unsupported analysis "${a}" for ${label}.`);
      return null;
    }
    if (REQUIRED_ANALYSIS_FIELDS[a]) {
      warnings.push(`Dropped ${a} for ${label}: missing required configuration.`);
      return null;
    }
    return { name: a };
  }
  if (a && typeof a === 'object' && typeof a.name === 'string') {
    if (!ALLOWED_ANALYSES.includes(a.name)) {
      warnings.push(`Dropped unsupported analysis "${a.name}" for ${label}.`);
      return null;
    }
    const required = REQUIRED_ANALYSIS_FIELDS[a.name] || [];
    const missing = required.filter(field => !(typeof a[field] === 'string' && a[field].trim()));
    if (missing.length) {
      warnings.push(`Dropped ${a.name} for ${label}: missing ${missing.join(', ')}.`);
      return null;
    }
    const out = { name: a.name };
    if (typeof a.output_expression === 'string' && a.output_expression.trim()) {
      out.output_expression = a.output_expression.trim();
    }
    if (typeof a.scan_param === 'string' && a.scan_param.trim()) {
      out.scan_param = a.scan_param.trim();
    }
    if (typeof a.scan_param_2 === 'string' && a.scan_param_2.trim()) {
      out.scan_param_2 = a.scan_param_2.trim();
    }
    return out;
  }
  warnings.push(`Dropped malformed analysis entry for ${label}.`);
  return null;
}

function normalizeNetwork(n, i, warnings) {
  const name = (typeof n?.name === 'string' && n.name.trim()) ? n.name.trim() : `Network ${i + 1}`;
  const reactions = Array.isArray(n?.reactions) ? n.reactions.map(normalizeReaction).filter(Boolean) : [];
  const requested = Array.isArray(n?.recommended_analyses) ? n.recommended_analyses : [];
  const recommended = requested.map(a => normalizeAnalysisEntry(a, warnings, name)).filter(Boolean);
  if (reactions.length > 0 && !recommended.some(a => a.name === 'model-summary')) {
    recommended.unshift({ name: 'model-summary' });
  }
  return {
    name,
    reactions,
    recommended_analyses: recommended,
    notes: typeof n?.notes === 'string' ? n.notes : '',
  };
}

function normalizeResult(parsed) {
  // Support both old single-network shape and new multi-network shape.
  let networksRaw;
  if (Array.isArray(parsed?.networks)) {
    networksRaw = parsed.networks;
  } else if (Array.isArray(parsed?.reactions)) {
    networksRaw = [{ name: 'Network 1', reactions: parsed.reactions, recommended_analyses: ['model-summary'] }];
  } else {
    networksRaw = [];
  }
  const normalizationWarnings = [];
  const networks = networksRaw.map((n, i) => normalizeNetwork(n, i, normalizationWarnings)).filter(n => n.reactions.length > 0);
  return {
    networks,
    warnings: [
      ...(Array.isArray(parsed?.warnings) ? parsed.warnings.map(String) : []),
      ...normalizationWarnings,
    ],
    summary: typeof parsed?.summary === 'string' ? parsed.summary : '',
    raw: parsed,
  };
}

// ===== Entry point =====

export function detectProvider(model) {
  const m = String(model || '').toLowerCase();
  if (m.startsWith('deepseek')) return 'deepseek';
  if (m.startsWith('claude')) return 'anthropic';
  return 'anthropic';
}

export async function runAgent({ provider, apiKey, model, baseUrl, text, files, signal }) {
  if (!apiKey) throw new Error('API key is required');
  if (!model) throw new Error('Model name is required');
  const chosen = provider || detectProvider(model);
  // DeepSeek V4 models speak the Anthropic Messages API — we just point at
  // their base URL. PDFs and document blocks work the same way.
  const resolvedBase = baseUrl
    || (chosen === 'deepseek' ? `${DEEPSEEK_ANTHROPIC_BASE}/v1/messages` : ANTHROPIC_ENDPOINT);

  let raw, meta;
  try {
    ({ raw, meta } = await callAnthropic({ apiKey, model, baseUrl: resolvedBase, text, files, signal }));
  } catch (e) {
    if (e?.name === 'AbortError') throw e;
    if (e instanceof TypeError) throw new Error(`Network error contacting ${chosen}: ${e.message || e}`);
    throw e;
  }

  if (!raw) throw new Error('Empty response from model');

  let parsed;
  try {
    parsed = tryParseJson(raw);
  } catch (e) {
    const truncated = meta?.stop_reason === 'max_tokens';
    const hint = truncated
      ? ' (response was cut off because the model hit the max-token limit — try a paper with fewer examples, or paste only the supplementary section)'
      : '';
    const err = new Error(`Model did not return valid JSON${hint}`);
    err.rawText = raw;
    err.parseError = e?.message || String(e);
    err.stopReason = meta?.stop_reason;
    throw err;
  }
  const result = normalizeResult(parsed);
  result.modelMeta = { ...meta, provider: chosen };
  // Surface input-side warnings (skipped files, truncations, oversized input).
  if (meta?.input?.warnings?.length) {
    result.warnings = [...(meta.input.warnings), ...(result.warnings || [])];
  }
  if (meta?.stop_reason === 'max_tokens') {
    result.warnings = [
      ...(result.warnings || []),
      'Response was truncated by max_tokens; some networks or warnings may be missing.',
    ];
  }
  return result;
}
