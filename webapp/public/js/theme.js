// Biocircuits Explorer — Theme Management & Plot Theming

import {
  themeState, THEME_MODE_STORAGE_KEY, LEGACY_THEME_MODE_STORAGE_KEY, LIGHT_THEME_STYLESHEET_ID,
  colorSchemeMediaQuery, SISO_FAMILY_COLORS, nodeRegistry,
} from './state.js';

// Lazy imports to avoid circular dependency issues at evaluation time.
// These modules import from theme.js, and theme.js imports from them.
// All accesses happen inside functions (runtime), so this is safe.
let _plotRegimeGraph, _plotTrajectory, _plotSISOBehaviorOverlay, _plotQKPolyhedron, _plotParameterScan1D,
    _plotROPCloud, _plotHeatmap, _plotParameterScan2D, _plotROPPolyhedron;
const lightThemeStylesheetState = {
  link: null,
  ready: false,
  promise: null,
};

async function ensurePlotImports() {
  if (_plotRegimeGraph) return;
  const [regimeGraph, plotting, siso, scan, ropCloud] = await Promise.all([
    import('./regime-graph.js'),
    import('./plotting.js'),
    import('./siso.js'),
    import('./scan.js'),
    import('./rop-cloud.js'),
  ]);
  _plotRegimeGraph = regimeGraph.plotRegimeGraph;
  _plotTrajectory = plotting.plotTrajectory;
  _plotHeatmap = plotting.plotHeatmap;
  _plotSISOBehaviorOverlay = siso.plotSISOBehaviorOverlay;
  _plotQKPolyhedron = siso.plotQKPolyhedron;
  _plotParameterScan1D = scan.plotParameterScan1D;
  _plotParameterScan2D = scan.plotParameterScan2D;
  _plotROPCloud = ropCloud.plotROPCloud;
  _plotROPPolyhedron = scan.plotROPPolyhedron;
}

// ===== Theme Mode =====
export function normalizeThemeMode(mode) {
  return ['auto', 'light', 'dark'].includes(mode) ? mode : 'auto';
}

export function resolveEffectiveTheme(mode = themeState.mode) {
  if (mode === 'light' || mode === 'dark') return mode;
  return colorSchemeMediaQuery?.matches ? 'light' : 'dark';
}

export function getLightThemeStylesheetURL() {
  return 'style-node-light.css';
}

export function ensureLightThemeStylesheet(enabled) {
  if (!enabled) {
    return Promise.resolve();
  }

  if (lightThemeStylesheetState.ready) {
    return Promise.resolve();
  }

  if (lightThemeStylesheetState.promise) {
    return lightThemeStylesheetState.promise;
  }

  const existing = document.getElementById(LIGHT_THEME_STYLESHEET_ID);
  const link = existing || document.createElement('link');
  lightThemeStylesheetState.link = link;

  lightThemeStylesheetState.promise = new Promise((resolve) => {
    const finish = () => {
      lightThemeStylesheetState.ready = true;
      lightThemeStylesheetState.promise = null;
      resolve();
    };

    if (link.sheet || link.dataset.loaded === 'true') {
      finish();
      return;
    }

    link.addEventListener('load', () => {
      link.dataset.loaded = 'true';
      finish();
    }, { once: true });
    link.addEventListener('error', finish, { once: true });

    if (!existing) {
      link.id = LIGHT_THEME_STYLESHEET_ID;
      link.rel = 'stylesheet';
      link.href = getLightThemeStylesheetURL();
      document.head.appendChild(link);
    }
  });

  return lightThemeStylesheetState.promise;
}

export function syncThemeMenuUI() {
  const themeModeBtn = document.getElementById('theme-mode-btn');
  const themeModeMenu = document.getElementById('theme-mode-menu');
  const label = themeState.mode === 'auto'
    ? `Appearance: System (${themeState.effective === 'light' ? 'Light' : 'Dark'})`
    : `Appearance: ${themeState.mode[0].toUpperCase()}${themeState.mode.slice(1)}`;
  if (themeModeBtn) {
    themeModeBtn.title = label;
    themeModeBtn.setAttribute('aria-label', label);
  }
  themeModeMenu?.querySelectorAll('.menu-item').forEach(item => {
    item.classList.toggle('is-selected', item.dataset.themeMode === themeState.mode);
  });
}

export async function applyThemeMode(mode, options = {}) {
  const persist = options.persist !== false;
  const refreshPlots = options.refreshPlots !== false;
  const normalized = normalizeThemeMode(mode);
  const effectiveOverride = ['light', 'dark'].includes(options.effectiveThemeOverride)
    ? options.effectiveThemeOverride
    : null;
  const effective = effectiveOverride || resolveEffectiveTheme(normalized);

  themeState.mode = normalized;
  themeState.effective = effective;

  document.documentElement.dataset.themeMode = normalized;
  document.documentElement.dataset.effectiveTheme = effective;
  document.documentElement.style.colorScheme = effective;

  await ensureLightThemeStylesheet(true);
  window.dispatchEvent(new CustomEvent('biocircuits-explorer:theme-changed', {
    detail: { mode: normalized, effective },
  }));
  window.dispatchEvent(new CustomEvent('rop:theme-changed', {
    detail: { mode: normalized, effective },
  }));
  syncThemeMenuUI();

  if (persist) {
    try {
      window.localStorage.setItem(THEME_MODE_STORAGE_KEY, normalized);
    } catch (_) {}
  }

  if (refreshPlots) {
    window.requestAnimationFrame(() => refreshThemeAwarePlots());
  }
}

export function storedThemeMode() {
  try {
    const stored = window.localStorage.getItem(THEME_MODE_STORAGE_KEY);
    if (stored != null) {
      return normalizeThemeMode(stored);
    }

    const legacyStored = window.localStorage.getItem(LEGACY_THEME_MODE_STORAGE_KEY);
    const normalizedLegacy = normalizeThemeMode(legacyStored);
    if (legacyStored != null) {
      window.localStorage.setItem(THEME_MODE_STORAGE_KEY, normalizedLegacy);
    }
    return normalizedLegacy;
  } catch (_) {
    return 'auto';
  }
}

// ===== Plot Theming =====
export function hexToRgba(hex, alpha) {
  const clean = (hex || '#888888').replace('#', '');
  const value = clean.length === 3
    ? clean.split('').map(ch => ch + ch).join('')
    : clean;
  const intVal = parseInt(value, 16);
  const r = (intVal >> 16) & 255;
  const g = (intVal >> 8) & 255;
  const b = intVal & 255;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

export function prefersLightTheme() {
  return themeState.effective === 'light';
}

export function getPlotTheme() {
  if (prefersLightTheme()) {
    return {
      paperBg: '#f4f8fc',
      plotBg: '#ffffff',
      sceneBg: '#f4f8fc',
      fontColor: '#5f7184',
      titleColor: '#223242',
      gridColor: '#d8e2ec',
      zeroLineColor: '#c9d5e1',
      legendBg: 'rgba(255,255,255,0.72)',
      legendBorderColor: '#d7e1eb',
      annotationBg: 'rgba(248,250,253,0.92)',
      annotationBorderColor: '#cad7e4',
      edgeLineColor: '#8b9caf',
      edgeLine3DColor: '#90a2b5',
      edgeArrowColor: '#72859a',
      edgeConeColor: '#7d90a4',
      edgeLabelColor: '#55687c',
      edgeHoverMarkerColor: 'rgba(124, 142, 164, 0.12)',
      subtleTextColor: '#738498',
      contourLineColor: '#1f2b38',
      nodeOutlineColor: '#1d2733',
      nodeTextColor: '#ffffff',
    };
  }

  return {
    paperBg: '#111',
    plotBg: '#1a1a1a',
    sceneBg: '#111',
    fontColor: '#888',
    titleColor: '#888',
    gridColor: '#333',
    zeroLineColor: '#444',
    legendBg: 'rgba(0,0,0,0)',
    legendBorderColor: 'rgba(0,0,0,0)',
    annotationBg: 'rgba(0,0,0,0.38)',
    annotationBorderColor: '#5e6a78',
    edgeLineColor: '#55606f',
    edgeLine3DColor: '#617082',
    edgeArrowColor: '#7b8794',
    edgeConeColor: '#8fa0b4',
    edgeLabelColor: '#c9d3dc',
    edgeHoverMarkerColor: 'rgba(201, 211, 220, 0.02)',
    subtleTextColor: '#7c8a97',
    contourLineColor: '#fff',
    nodeOutlineColor: '#1d2733',
    nodeTextColor: '#ffffff',
  };
}

export function themeAxisTitle(title, color) {
  if (title == null) return title;
  if (typeof title === 'string') {
    return { text: title, font: { color } };
  }
  return {
    ...title,
    font: {
      ...(title.font || {}),
      color,
    },
  };
}

export function applyPlotAxisTheme(axis, theme) {
  if (!axis) return axis;
  const themed = {
    ...axis,
    tickfont: {
      ...(axis.tickfont || {}),
      color: theme.fontColor,
    },
    title: themeAxisTitle(axis.title, theme.fontColor),
  };

  if (axis.showgrid !== false || axis.gridcolor !== undefined) {
    themed.gridcolor = theme.gridColor;
  }
  if (axis.zeroline !== false || axis.zerolinecolor !== undefined) {
    themed.zerolinecolor = theme.zeroLineColor;
  }
  if (axis.showline || axis.linecolor !== undefined) {
    themed.linecolor = theme.zeroLineColor;
  }

  return themed;
}

export function applyPlotSceneAxisTheme(axis, theme) {
  if (!axis) return axis;
  const themed = {
    ...axis,
    color: theme.fontColor,
    title: themeAxisTitle(axis.title, theme.fontColor),
  };

  if (axis.showgrid !== false || axis.gridcolor !== undefined) {
    themed.gridcolor = theme.gridColor;
  }
  if (axis.zeroline !== false || axis.zerolinecolor !== undefined) {
    themed.zerolinecolor = theme.zeroLineColor;
  }
  if (axis.showbackground !== false || axis.backgroundcolor !== undefined) {
    themed.backgroundcolor = theme.sceneBg;
  }

  return themed;
}

export function applyPlotLayoutTheme(layout) {
  const theme = getPlotTheme();
  const themed = {
    ...layout,
    paper_bgcolor: theme.paperBg,
    plot_bgcolor: theme.plotBg,
    font: {
      ...(layout.font || {}),
      color: theme.fontColor,
    },
  };

  if (layout.title) {
    themed.title = {
      ...layout.title,
      font: {
        ...(layout.title.font || {}),
        color: theme.titleColor,
      },
    };
  }

  if (layout.legend) {
    themed.legend = {
      ...layout.legend,
      bgcolor: theme.legendBg,
      bordercolor: theme.legendBorderColor,
      font: {
        ...(layout.legend.font || {}),
        color: theme.fontColor,
      },
    };
  }

  if (layout.xaxis) themed.xaxis = applyPlotAxisTheme(layout.xaxis, theme);
  if (layout.yaxis) themed.yaxis = applyPlotAxisTheme(layout.yaxis, theme);

  if (layout.scene) {
    themed.scene = {
      ...layout.scene,
      bgcolor: theme.sceneBg,
      xaxis: applyPlotSceneAxisTheme(layout.scene.xaxis, theme),
      yaxis: applyPlotSceneAxisTheme(layout.scene.yaxis, theme),
      zaxis: applyPlotSceneAxisTheme(layout.scene.zaxis, theme),
    };
  }

  return themed;
}

export function themedColorbar(title) {
  const theme = getPlotTheme();
  return {
    title,
    titlefont: { color: theme.fontColor, size: 9 },
    tickfont: { color: theme.fontColor, size: 8 },
  };
}

export function getFamilyColor(index, offset = 0) {
  const safeIndex = Math.max(1, Number(index) || 1);
  return SISO_FAMILY_COLORS[(safeIndex - 1 + offset) % SISO_FAMILY_COLORS.length];
}

// ===== Theme-Aware Plot Refresh =====
export async function refreshThemeAwarePlots() {
  await ensurePlotImports();

  Object.entries(nodeRegistry).forEach(([nodeId, info]) => {
    const nodeData = info?.data || {};

    try {
      switch (info.type) {
        case 'regime-graph':
          if (nodeData.graphData && document.getElementById(`${nodeId}-plot`)) {
            _plotRegimeGraph(nodeData.graphData, `${nodeId}-plot`, { viewMode: nodeData.config?.viewMode || '3d' });
          }
          break;

        case 'siso-result':
          if (nodeData.sisoPlotMode === 'overlay' && nodeData.behaviorData && document.getElementById(`${nodeId}-traj-plot`)) {
            _plotSISOBehaviorOverlay(nodeId);
          } else if (nodeData.trajectoryData && document.getElementById(`${nodeId}-traj-plot`)) {
            _plotTrajectory(nodeData.trajectoryData, `${nodeId}-traj-plot`);
          }
          break;

        case 'qk-poly-result': {
          const poly = nodeData.polyhedronPayload?.polyhedra?.[0];
          const qkSymbols = nodeData.polyhedronPayload?.qk_symbols || [];
          if (poly && document.getElementById(`${nodeId}-plot`)) {
            _plotQKPolyhedron(poly, qkSymbols, `${nodeId}-plot`);
          }
          break;
        }

        case 'scan-1d-result':
        case 'parameter-scan-1d':
          if (nodeData.scan1DResult && document.getElementById(`${nodeId}-plot`)) {
            _plotParameterScan1D(nodeData.scan1DResult, `${nodeId}-plot`);
          }
          break;

        case 'rop-cloud':
        case 'rop-cloud-result':
          if (nodeData.ropCloudData && document.getElementById(`${nodeId}-plot`)) {
            _plotROPCloud(nodeData.ropCloudData, `${nodeId}-plot`, { ranges: nodeData.ropCloudRanges });
          }
          break;

        case 'fret-result':
        case 'fret-heatmap':
          if (nodeData.fretHeatmapData && document.getElementById(`${nodeId}-plot`)) {
            _plotHeatmap(nodeData.fretHeatmapData, `${nodeId}-plot`);
          }
          break;

        case 'scan-2d-result':
        case 'parameter-scan-2d':
          if (nodeData.scan2DResult && document.getElementById(`${nodeId}-plot`)) {
            _plotParameterScan2D(nodeData.scan2DResult, `${nodeId}-plot`);
          }
          break;

        case 'rop-poly-result':
        case 'rop-polyhedron':
          if (nodeData.ropPlotData && document.getElementById(`${nodeId}-plot`)) {
            _plotROPPolyhedron(nodeData.ropPlotData, `${nodeId}-plot`, { fitInnerPoints: nodeData.fitInnerPoints });
          }
          break;

        default:
          break;
      }
    } catch (error) {
      console.warn('Failed to refresh themed plot', info.type, error);
    }
  });
}

export async function installThemeChangeObserver() {
  await ensureLightThemeStylesheet(true);
  await applyThemeMode(storedThemeMode(), { persist: false, refreshPlots: false });
  if (!colorSchemeMediaQuery) return;
  const rerender = () => {
    if (themeState.mode !== 'auto') return;
    void applyThemeMode('auto', { persist: false, refreshPlots: true });
  };

  if (typeof colorSchemeMediaQuery.addEventListener === 'function') {
    colorSchemeMediaQuery.addEventListener('change', rerender);
  } else if (typeof colorSchemeMediaQuery.addListener === 'function') {
    colorSchemeMediaQuery.addListener(rerender);
  }
}
