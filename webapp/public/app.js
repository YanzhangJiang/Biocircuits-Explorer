// Biocircuits Explorer — Frontend Application
const API = '';  // same origin

// ─── State ───
const state = {
  sessionId: null,
  model: null,
  vertices: null,
  graph: null,
  siso: {},
  qK_syms: [],
};

// ─── API helpers ───
async function api(endpoint, data) {
  setStatus('working', 'Computing...');
  try {
    const resp = await fetch(`${API}/api/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    if (!resp.ok) {
      const ct = resp.headers.get('content-type') || '';
      if (ct.includes('application/json')) {
        const errJson = await resp.json();
        throw new Error(errJson.error || `Server error (${resp.status})`);
      }
      throw new Error(`Server error (${resp.status})`);
    }
    const json = await resp.json();
    if (json.error) throw new Error(json.error);
    setStatus('done', 'Done');
    return json;
  } catch (e) {
    setStatus('error', e.message);
    throw e;
  }
}

function setStatus(cls, text) {
  const badge = document.getElementById('status-badge');
  badge.className = `badge ${cls}`;
  badge.textContent = text;
  if (cls === 'done') setTimeout(() => {
    badge.className = 'badge idle';
    badge.textContent = 'Ready';
  }, 3000);
}

// ─── Reaction Editor ───
function getReactions() {
  const rows = document.querySelectorAll('.reaction-row');
  const reactions = [];
  const kds = [];
  rows.forEach(row => {
    const rule = row.querySelector('.reaction-input').value.trim();
    const kd = parseFloat(row.querySelector('.kd-input').value);
    if (rule && !isNaN(kd)) {
      reactions.push(rule);
      kds.push(kd);
    }
  });
  return { reactions, kds };
}

function addReactionRow(rule = '', kd = 1e-3) {
  const list = document.getElementById('reactions-list');
  const row = document.createElement('div');
  row.className = 'reaction-row';
  row.innerHTML = `
    <div style="flex:1">
      <span class="reaction-label">Reaction (e.g. E + S &lt;-&gt; C_ES)</span>
      <input type="text" class="reaction-input" placeholder="A + B <-> C">
    </div>
    <div>
      <span class="reaction-label">Kd</span>
      <input type="number" class="kd-input" step="any" min="1e-12">
    </div>
    <button class="btn-remove" title="Remove">&times;</button>
  `;
  row.querySelector('.reaction-input').value = rule;
  row.querySelector('.kd-input').value = kd;
  row.querySelector('.btn-remove').onclick = () => row.remove();
  list.appendChild(row);
}

// ─── Tab switching ───
function switchTab(tabId) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.toggle('active', t.id === tabId));
}

// ─── Build Model ───
async function buildModel() {
  const { reactions, kds } = getReactions();
  if (reactions.length === 0) { alert('Add at least one reaction'); return; }

  // Validate Kd values
  if (kds.some(kd => !Number.isFinite(kd) || kd <= 0)) {
    alert('All Kd values must be finite positive numbers (> 0)');
    return;
  }

  const data = await api('build_model', { reactions, kd: kds });
  state.sessionId = data.session_id;
  state.model = data;
  state.vertices = null;
  state.graph = null;
  state.siso = {};

  // Update UI
  document.getElementById('btn-enumerate').disabled = false;
  document.getElementById('btn-graph').disabled = true;

  const info = `Species (n=${data.n}): ${data.x_sym.join(', ')}
Totals (d=${data.d}): ${data.q_sym.join(', ')}
Constants (r=${data.r}): ${data.K_sym.join(', ')}
N = ${JSON.stringify(data.N)}
L = ${JSON.stringify(data.L)}`;
  document.getElementById('model-info').style.display = '';
  document.getElementById('model-info-text').textContent = info;

  // Show cloud/heatmap sections
  document.getElementById('cloud-section').style.display = '';
  if (data.d === 2) {
    document.getElementById('heatmap-section').style.display = '';
  } else {
    document.getElementById('heatmap-section').style.display = 'none';
  }
}

// ─── Find Vertices ───
async function findVertices() {
  const data = await api('find_vertices', { session_id: state.sessionId });
  state.vertices = data;

  document.getElementById('btn-graph').disabled = false;
  renderVerticesTable(data.vertices);
  switchTab('tab-vertices');
}

function renderVerticesTable(vertices) {
  const wrap = document.getElementById('vertices-table-wrap');
  let html = `<table><thead><tr>
    <th>#</th><th>Perm</th><th>Type</th><th>Nullity</th>
  </tr></thead><tbody>`;
  vertices.forEach(v => {
    const typeTag = v.asymptotic
      ? '<span class="tag tag-asym">Asymptotic</span>'
      : '<span class="tag tag-nonasym">Non-Asymp</span>';
    const singTag = v.singular
      ? ' <span class="tag tag-singular">Singular</span>'
      : ' <span class="tag tag-invertible">Invertible</span>';
    html += `<tr>
      <td>${v.idx}</td>
      <td style="font-family:var(--font)">[${v.perm.join(', ')}]</td>
      <td>${typeTag}${singTag}</td>
      <td>${v.nullity}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

// ─── Build Graph ───
async function buildGraph() {
  const data = await api('build_graph', { session_id: state.sessionId });
  state.graph = data;

  // Show SISO section with qK symbols
  const sel = document.getElementById('siso-select');
  sel.innerHTML = '';
  state.qK_syms = [...state.model.q_sym, ...state.model.K_sym];
  state.qK_syms.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s;
    sel.appendChild(opt);
  });
  document.getElementById('siso-section').style.display = '';

  plotRegimeGraph(data);
  switchTab('tab-graph');
}

function plotRegimeGraph(data) {
  const { nodes, edges } = data;
  const n = nodes.length;

  // Simple force-directed layout using angles
  const positions = {};
  nodes.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / n;
    const r = 2 + Math.sqrt(n) * 0.5;
    positions[node.id] = { x: r * Math.cos(angle), y: r * Math.sin(angle) };
  });

  // Spring layout iterations
  for (let iter = 0; iter < 200; iter++) {
    const forces = {};
    nodes.forEach(nd => { forces[nd.id] = { x: 0, y: 0 }; });

    // Repulsion
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const a = nodes[i].id, b = nodes[j].id;
        let dx = positions[a].x - positions[b].x;
        let dy = positions[a].y - positions[b].y;
        let dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
        let f = 3.0 / (dist * dist);
        forces[a].x += f * dx / dist;
        forces[a].y += f * dy / dist;
        forces[b].x -= f * dx / dist;
        forces[b].y -= f * dy / dist;
      }
    }

    // Attraction along edges
    edges.forEach(e => {
      let dx = positions[e.target].x - positions[e.source].x;
      let dy = positions[e.target].y - positions[e.source].y;
      let dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
      let f = dist * 0.1;
      forces[e.source].x += f * dx / dist;
      forces[e.source].y += f * dy / dist;
      forces[e.target].x -= f * dx / dist;
      forces[e.target].y -= f * dy / dist;
    });

    // Apply
    const cooling = 0.95 - 0.5 * (iter / 200);
    nodes.forEach(nd => {
      positions[nd.id].x += forces[nd.id].x * cooling;
      positions[nd.id].y += forces[nd.id].y * cooling;
    });
  }

  // Node size scaling: use volume-based sizes from backend
  const rawSizes = nodes.map(node => Number(node.size)).filter(size => Number.isFinite(size));
  const minRawSize = rawSizes.length ? Math.min(...rawSizes) : 44;
  const maxRawSize = rawSizes.length ? Math.max(...rawSizes) : 44;
  const rawSpan = Math.max(maxRawSize - minRawSize, 1e-9);
  const nodeSizes = nodes.map(node => {
    const raw = Number(node.size);
    if (!Number.isFinite(raw)) return 20;
    const normalized = rawSpan < 1e-6 ? 0.5 : (raw - minRawSize) / rawSpan;
    const logScaled = rawSpan > 100
      ? Math.log10(1 + normalized * 9)
      : normalized;
    const scaled = 12 + 28 * logScaled;
    return Math.max(8, Math.min(40, scaled));
  });

  // Edge traces
  const edgeX = [], edgeY = [];
  edges.forEach(e => {
    edgeX.push(positions[e.source].x, positions[e.target].x, null);
    edgeY.push(positions[e.source].y, positions[e.target].y, null);
  });

  const colors = nodes.map(nd => {
    if (nd.singular) return '#ff6b6b';
    if (!nd.asymptotic) return '#ffd43b';
    return '#51cf66';
  });

  const traces = [
    {
      x: edgeX, y: edgeY, mode: 'lines',
      line: { color: '#444', width: 1 },
      hoverinfo: 'none', type: 'scatter',
    },
    {
      x: nodes.map(nd => positions[nd.id].x),
      y: nodes.map(nd => positions[nd.id].y),
      mode: 'markers+text',
      marker: { size: nodeSizes, color: colors, line: { width: 1, color: '#666' } },
      text: nodes.map(nd => `${nd.id}`),
      textposition: 'middle center',
      textfont: { size: 10, color: '#fff' },
      hovertext: nodes.map(nd => {
        const volumeStr = nd.volume !== null && nd.volume !== undefined
          ? `volume=${Number(nd.volume).toExponential(3)}`
          : 'volume=n/a';
        return `#${nd.id} [${nd.perm.join(',')}]<br>${nd.asymptotic ? 'Asymptotic' : 'Non-Asymp'} ${nd.singular ? 'Singular' : 'Invertible'}<br>nullity=${nd.nullity}<br>${volumeStr}`;
      }),
      hoverinfo: 'text', type: 'scatter',
    }
  ];

  const layout = {
    showlegend: false,
    xaxis: { visible: false }, yaxis: { visible: false, scaleanchor: 'x' },
    paper_bgcolor: '#0f1117', plot_bgcolor: '#0f1117',
    margin: { t: 30, b: 20, l: 20, r: 20 },
    title: { text: `Regime Graph (${nodes.length} vertices, ${edges.length} edges)`, font: { color: '#8b90a5', size: 14 } },
  };

  Plotly.newPlot('plot-graph', traces, layout, { responsive: true });
}

// ─── SISO Paths ───
async function computeSISO() {
  const changeQK = document.getElementById('siso-select').value;
  const data = await api('siso_paths', { session_id: state.sessionId, change_qK: changeQK });
  state.siso[changeQK] = data;

  renderSISOInfo(data, changeQK);
  plotSISOGraph(data);
  switchTab('tab-siso');
}

function renderSISOInfo(data, changeQK) {
  const info = document.getElementById('siso-info');
  let html = `<strong>SISO for ${data.change_qK}</strong> — ${data.n_paths} paths, `;
  html += `${data.sources.length} sources, ${data.sinks.length} sinks`;
  html += `<div class="path-list">`;
  data.paths.forEach(p => {
    const permStr = p.perms.map(pr => `[${pr.join(',')}]`).join(' → ');
    html += `<div class="path-item" data-idx="${p.idx}" data-qk="${changeQK}" onclick="selectSISOPath(this)">#${p.idx}: ${permStr}</div>`;
  });
  html += '</div>';
  info.innerHTML = html;
}

async function selectSISOPath(el) {
  document.querySelectorAll('.path-item').forEach(p => p.classList.remove('selected'));
  el.classList.add('selected');
  const pathIdx = parseInt(el.dataset.idx);
  const changeQK = el.dataset.qk;

  try {
    const data = await api('siso_trajectory', {
      session_id: state.sessionId,
      change_qK: changeQK,
      path_idx: pathIdx,
    });
    plotTrajectory(data);
    switchTab('tab-trajectory');
  } catch (e) {
    console.error('Trajectory failed:', e);
  }
}

function plotSISOGraph(data) {
  // Build a directed graph from paths
  const nodeSet = new Set();
  const edgeSet = new Set();
  data.paths.forEach(p => {
    for (let i = 0; i < p.vertex_indices.length; i++) {
      nodeSet.add(p.vertex_indices[i]);
      if (i < p.vertex_indices.length - 1) {
        edgeSet.add(`${p.vertex_indices[i]}-${p.vertex_indices[i + 1]}`);
      }
    }
  });

  const nodeArr = [...nodeSet];
  const n = nodeArr.length;
  const positions = {};
  nodeArr.forEach((id, i) => {
    positions[id] = { x: (i % 6) * 2, y: -Math.floor(i / 6) * 2 };
  });

  // Simple layout
  for (let iter = 0; iter < 100; iter++) {
    const forces = {};
    nodeArr.forEach(id => { forces[id] = { x: 0, y: 0 }; });
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const a = nodeArr[i], b = nodeArr[j];
        let dx = positions[a].x - positions[b].x;
        let dy = positions[a].y - positions[b].y;
        let dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
        let f = 2.0 / (dist * dist);
        forces[a].x += f * dx / dist; forces[a].y += f * dy / dist;
        forces[b].x -= f * dx / dist; forces[b].y -= f * dy / dist;
      }
    }
    edgeSet.forEach(e => {
      const [s, t] = e.split('-').map(Number);
      let dx = positions[t].x - positions[s].x;
      let dy = positions[t].y - positions[s].y;
      let dist = Math.sqrt(dx * dx + dy * dy) + 0.01;
      let f = dist * 0.15;
      forces[s].x += f * dx / dist; forces[s].y += f * dy / dist;
      forces[t].x -= f * dx / dist; forces[t].y -= f * dy / dist;
    });
    const cool = 0.8 - 0.4 * (iter / 100);
    nodeArr.forEach(id => {
      positions[id].x += forces[id].x * cool;
      positions[id].y += forces[id].y * cool;
    });
  }

  const edgeX = [], edgeY = [];
  edgeSet.forEach(e => {
    const [s, t] = e.split('-').map(Number);
    edgeX.push(positions[s].x, positions[t].x, null);
    edgeY.push(positions[s].y, positions[t].y, null);
  });

  const isSource = new Set(data.sources);
  const isSink = new Set(data.sinks);
  const colors = nodeArr.map(id => {
    if (isSource.has(id)) return '#6c8cff';
    if (isSink.has(id)) return '#ff6b6b';
    return '#51cf66';
  });

  const traces = [
    { x: edgeX, y: edgeY, mode: 'lines', line: { color: '#555', width: 1.5 }, hoverinfo: 'none', type: 'scatter' },
    {
      x: nodeArr.map(id => positions[id].x),
      y: nodeArr.map(id => positions[id].y),
      mode: 'markers+text', type: 'scatter',
      marker: { size: 18, color: colors, line: { width: 1, color: '#666' } },
      text: nodeArr.map(id => `${id}`),
      textposition: 'middle center',
      textfont: { size: 9, color: '#fff' },
      hovertext: nodeArr.map(id => {
        let label = `#${id}`;
        if (isSource.has(id)) label += ' (Source)';
        if (isSink.has(id)) label += ' (Sink)';
        return label;
      }),
      hoverinfo: 'text',
    }
  ];

  const layout = {
    showlegend: false,
    xaxis: { visible: false }, yaxis: { visible: false, scaleanchor: 'x' },
    paper_bgcolor: '#0f1117', plot_bgcolor: '#0f1117',
    margin: { t: 30, b: 20, l: 20, r: 20 },
    title: { text: `SISO Graph — ${data.change_qK}`, font: { color: '#8b90a5', size: 14 } },
  };

  Plotly.newPlot('plot-siso', traces, layout, { responsive: true });
}

// ─── ROP Point Cloud ───
async function computeROPCloud() {
  const nSamples = parseInt(document.getElementById('cloud-samples').value);
  const span = parseInt(document.getElementById('cloud-span').value);

  const data = await api('rop_cloud', {
    session_id: state.sessionId,
    n_samples: nSamples,
    span: span,
  });

  plotROPCloud(data);
  switchTab('tab-cloud');
}

function plotROPCloud(data) {
  const { reaction_orders, fret_values, q_sym, d } = data;
  const n = reaction_orders.length;

  const darkLayout = {
    paper_bgcolor: '#0f1117', plot_bgcolor: '#1a1d27',
    font: { color: '#8b90a5' },
    margin: { t: 40, b: 50, l: 50, r: 30 },
  };

  if (d === 2) {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const traces = [{
      x, y, mode: 'markers', type: 'scatter',
      marker: {
        size: 3, color: fret_values.map(v => Math.log10(v + 1e-30)),
        colorscale: 'Viridis', showscale: true,
        colorbar: { title: 'log10(FRET)', titlefont: { color: '#8b90a5' } },
      },
      hoverinfo: 'x+y',
    }];
    const layout = {
      ...darkLayout,
      title: { text: 'ROP Point Cloud (2D)', font: { color: '#8b90a5', size: 14 } },
      xaxis: { title: `∂log output / ∂log ${q_sym[0]}`, gridcolor: '#2e3348', zerolinecolor: '#444' },
      yaxis: { title: `∂log output / ∂log ${q_sym[1]}`, gridcolor: '#2e3348', zerolinecolor: '#444' },
    };
    Plotly.newPlot('plot-cloud', traces, layout, { responsive: true });
  } else if (d === 3) {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const z = reaction_orders.map(r => r[2]);
    const traces = [{
      x, y, z, mode: 'markers', type: 'scatter3d',
      marker: {
        size: 2, color: fret_values.map(v => Math.log10(v + 1e-30)),
        colorscale: 'Viridis', showscale: true,
        colorbar: { title: 'log10(FRET)' },
      },
    }];
    const layout = {
      ...darkLayout,
      title: { text: 'ROP Point Cloud (3D)', font: { color: '#8b90a5', size: 14 } },
      scene: {
        xaxis: { title: `∂log/∂log ${q_sym[0]}`, gridcolor: '#2e3348' },
        yaxis: { title: `∂log/∂log ${q_sym[1]}`, gridcolor: '#2e3348' },
        zaxis: { title: `∂log/∂log ${q_sym[2]}`, gridcolor: '#2e3348' },
        bgcolor: '#1a1d27',
      },
    };
    Plotly.newPlot('plot-cloud', traces, layout, { responsive: true });
  } else {
    // For d>3, show first two dimensions
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const traces = [{
      x, y, mode: 'markers', type: 'scatter',
      marker: { size: 3, color: '#6c8cff', opacity: 0.3 },
    }];
    const layout = {
      ...darkLayout,
      title: { text: `ROP Cloud (first 2 of ${d} dims)`, font: { color: '#8b90a5', size: 14 } },
      xaxis: { title: `∂log/∂log ${q_sym[0]}`, gridcolor: '#2e3348' },
      yaxis: { title: `∂log/∂log ${q_sym[1]}`, gridcolor: '#2e3348' },
    };
    Plotly.newPlot('plot-cloud', traces, layout, { responsive: true });
  }
}

// ─── FRET Heatmap ───
async function computeHeatmap() {
  const nGrid = parseInt(document.getElementById('heatmap-grid').value);
  const data = await api('fret_heatmap', {
    session_id: state.sessionId,
    n_grid: nGrid,
  });
  plotHeatmap(data);
  switchTab('tab-heatmap');
}

function plotHeatmap(data) {
  const { logq1, logq2, fret, regime, bounds, q_sym } = data;

  // Log-transform FRET
  const logFret = fret.map(row => row.map(v => Math.log10(v + 1e-30)));

  const traces = [
    {
      z: logFret, x: logq1, y: logq2,
      type: 'heatmap', colorscale: 'Viridis',
      colorbar: { title: 'log10(FRET)', titlefont: { color: '#8b90a5' } },
    },
    {
      z: bounds, x: logq1, y: logq2,
      type: 'contour',
      contours: { start: 0.5, end: 0.5, size: 1, coloring: 'none' },
      line: { color: '#fff', width: 2 },
      showscale: false,
    },
  ];

  const layout = {
    paper_bgcolor: '#0f1117', plot_bgcolor: '#1a1d27',
    font: { color: '#8b90a5' },
    margin: { t: 40, b: 50, l: 60, r: 30 },
    title: { text: 'FRET Heatmap + Regime Boundaries', font: { color: '#8b90a5', size: 14 } },
    xaxis: { title: `log10(${q_sym[0]})`, gridcolor: '#2e3348' },
    yaxis: { title: `log10(${q_sym[1]})`, gridcolor: '#2e3348' },
  };

  Plotly.newPlot('plot-heatmap', traces, layout, { responsive: true });
}

// ─── SISO Trajectory Plot ───
function plotTrajectory(data) {
  const { change_values, logx, regimes, x_sym, change_sym } = data;
  const nSpecies = x_sym.length;
  const nPoints = change_values.length;

  // Assign colors to regimes
  const uniqueRegimes = [...new Set(regimes)];
  const palette = [
    '#6c8cff', '#51cf66', '#ff6b6b', '#ffd43b', '#4ecdc4',
    '#e599f7', '#ff922b', '#74c0fc', '#f06595', '#a9e34b',
  ];
  const regimeColor = {};
  uniqueRegimes.forEach((r, i) => { regimeColor[r] = palette[i % palette.length]; });

  const traces = [];
  for (let s = 0; s < nSpecies; s++) {
    // Split into segments by regime for coloring
    let i = 0;
    while (i < nPoints) {
      const regime = regimes[i];
      const segX = [], segY = [];
      while (i < nPoints && regimes[i] === regime) {
        segX.push(change_values[i]);
        segY.push(logx[i][s]);
        i++;
      }
      traces.push({
        x: segX, y: segY, mode: 'lines', type: 'scatter',
        line: { color: regimeColor[regime], width: 2 },
        name: `${x_sym[s]} (rgm ${regime})`,
        legendgroup: x_sym[s],
        showlegend: segX[0] === change_values[0],
        hoverinfo: 'x+y+name',
      });
    }
  }

  const layout = {
    paper_bgcolor: '#0f1117', plot_bgcolor: '#1a1d27',
    font: { color: '#8b90a5' },
    margin: { t: 40, b: 50, l: 60, r: 30 },
    title: { text: `SISO Trajectory — changing ${change_sym}`, font: { color: '#8b90a5', size: 14 } },
    xaxis: { title: `log ${change_sym}`, gridcolor: '#2e3348', zerolinecolor: '#444' },
    yaxis: { title: 'log(x)', gridcolor: '#2e3348', zerolinecolor: '#444' },
    legend: { bgcolor: 'rgba(0,0,0,0)', font: { color: '#8b90a5' } },
  };

  Plotly.newPlot('plot-trajectory', traces, layout, { responsive: true });
}

// ─── Initialization ───
document.addEventListener('DOMContentLoaded', () => {
  // Default reactions
  addReactionRow('E + S <-> C_ES', 1e-3);
  addReactionRow('E + P <-> C_EP', 1e-3);

  // Button handlers
  document.getElementById('btn-add-reaction').onclick = () => addReactionRow();
  document.getElementById('btn-build').onclick = buildModel;
  document.getElementById('btn-enumerate').onclick = findVertices;
  document.getElementById('btn-graph').onclick = buildGraph;
  document.getElementById('btn-siso').onclick = computeSISO;
  document.getElementById('btn-cloud').onclick = computeROPCloud;
  document.getElementById('btn-heatmap').onclick = computeHeatmap;

  // Tab switching
  document.querySelectorAll('.tab').forEach(tab => {
    tab.onclick = () => switchTab(tab.dataset.tab);
  });
});
