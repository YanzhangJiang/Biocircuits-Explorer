function escapeHtml(text) {
  return String(text ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function shortNid(nid) {
  const s = String(nid);
  return s.length > 28 ? `${s.slice(0, 26)}...` : s;
}

function fmt(value, digits = 2) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits).replace(/\.?0+$/, '') : 'n/a';
}

export function designCandidateKey(nid, inp, out) {
  return `${String(nid || '')}::${String(inp || '')}::${String(out || '')}`;
}

function selectedCandidateKey(selection) {
  if (!selection) return null;
  if (typeof selection === 'object') {
    if (selection.selectedCandidateKey) return String(selection.selectedCandidateKey);
    if (selection.selectedNid && selection.selectedInput && selection.selectedOutput) {
      return designCandidateKey(selection.selectedNid, selection.selectedInput, selection.selectedOutput);
    }
    return null;
  }
  return String(selection);
}

function selectedNid(selection) {
  if (!selection) return null;
  if (typeof selection === 'object') return selection.selectedNid || null;
  return String(selection);
}

export const DESIGNABILITY_SPEC_VERSION = 'bne-designability/v1.0.0';

function defaultCandidateBudget(kind) {
  return {
    mode: 'near_minimal',
    max_extra_reactions: 1,
    max_extra_species: 1,
    max_extra_mu: 1,
    max_recommended: 24,
    max_verified_recommendations: 24,
    max_screened: 24,
    max_near_misses: 12,
    max_exact_placements: kind === 'exact' ? 3 : 0,
  };
}

function parseLegacyTarget(kind, rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) throw new Error('Enter a target behavior');
  let target;
  if (kind === 'label') {
    target = raw;
    if (!target) throw new Error('Pick a behavior label');
  } else if (kind === 'sign') {
    target = raw.replace(/[^+\-]/g, '');
    if (!target) throw new Error('A qualitative target looks like + - + (use + and -)');
  } else if (kind === 'exact') {
    if (/(^|,)\s*(,|$)/.test(raw)) throw new Error('A precise target looks like 1, 0, -1');
    const tokens = raw.split(/[,\s]+/);
    target = tokens.map(Number);
    if (!target.length || target.some(v => !Number.isFinite(v))) {
      throw new Error('A precise target looks like 1, 0, -1');
    }
  } else {
    throw new Error('Unknown target kind');
  }
  return target;
}

export function buildDesignabilitySpecFromLegacyTarget(kind, rawValue, options = {}) {
  const target = parseLegacyTarget(kind, rawValue);
  return {
    schema_version: DESIGNABILITY_SPEC_VERSION,
    source: {
      kind: options.sourceKind || 'legacy_shorthand',
      ...(options.nodeId ? { node_id: options.nodeId } : {}),
    },
    target: {
      legacy_target: {
        target_kind: kind,
        target,
      },
    },
    constraints: options.constraints || {},
    candidate_budget: {
      ...defaultCandidateBudget(kind),
      ...(options.candidateBudget || {}),
    },
    ranking_policy: {
      verified_only: true,
      ...(options.rankingPolicy || {}),
    },
    audit_policy: {
      unsupported: 'block_if_hard',
      path_format: 'json_pointer',
      include_supported: true,
      ...(options.auditPolicy || {}),
    },
  };
}

export function buildDesignScreenRequest(kind, rawValue) {
  return {
    designability_spec: buildDesignabilitySpecFromLegacyTarget(kind, rawValue),
  };
}

export function buildDesignScreenRequestFromSpec(spec) {
  if (!spec || typeof spec !== 'object') throw new Error('DesignabilitySpec is missing');
  if (spec.schema_version !== DESIGNABILITY_SPEC_VERSION) {
    throw new Error(`DesignabilitySpec must use ${DESIGNABILITY_SPEC_VERSION}`);
  }
  return {
    designability_spec: spec,
  };
}

function renderMetricChips(metrics = {}) {
  const chips = [];
  const hasWitnessedTransitionSpacing = Number.isFinite(Number(metrics.transition_spacing)) &&
    Number.isFinite(Number(metrics.witness_min_spacing_decades)) &&
    Number(metrics.transition_spacing) === Number(metrics.witness_min_spacing_decades) &&
    typeof metrics.witness_spacing_basis === 'string' &&
    metrics.witness_spacing_basis.trim().length > 0;
  if (['theta_union_cell', 'theta_union_path'].includes(metrics.chebyshev_radius_source)) {
    chips.push(['rho', metrics.chebyshev_radius, 2]);
  }
  if (metrics.tunable_volume_source === 'chebyshev_ball_lower_bound') {
    chips.push(['vol', metrics.tunable_volume, 3]);
  }
  if (Number.isFinite(Number(metrics.sampled_dynamic_range_fold_change)) &&
      metrics.sampled_dynamic_range_source === 'sampled_forward_dose_response') {
    chips.push(['dyn', metrics.sampled_dynamic_range_fold_change, 2]);
  }
  if (Number.isFinite(Number(metrics.transition_spacing)) &&
      (['feasible_region', 'exact_window', 'exact_solver'].includes(metrics.transition_spacing_source) ||
        hasWitnessedTransitionSpacing)) {
    chips.push(['space', metrics.transition_spacing, 2]);
  }
  return chips.map(([label, value, digits]) =>
    `<span class="summary-chip design-metric-chip">${escapeHtml(label)} ${escapeHtml(fmt(value, digits))}</span>`,
  ).join('');
}

function renderCandidateRow(card, { selectedKey = null, minimalOnly = false, verified = false } = {}) {
  const nid = String(card.nid || '');
  const inp = String(card.inp || '');
  const out = String(card.out || '');
  const isSel = selectedKey && designCandidateKey(nid, inp, out) === selectedKey;
  const complexity = card.complexity || {};
  const metrics = card.metrics || {};
  const grade = card.certificate_grade || (minimalOnly ? 'minimal-structural-certificate' : 'screened');
  const score = !verified && Number.isFinite(Number(card.tunability_score))
    ? `<span class="summary-chip">score ${escapeHtml(fmt(card.tunability_score, 2))}</span>`
    : '';
  const statusTag = !verified || card.evidence_grade === 'proxy_only' || card.screen_status === 'screened_proxy'
    ? '<span class="tag tag-atlas-muted">exploratory</span>'
    : card.pass === false
      ? '<span class="tag tag-atlas-failed">screened out</span>'
      : '<span class="tag tag-atlas-ok">verified</span>';
  const metricLine = minimalOnly ? '' :
    `<div class="design-net-metrics">${score}${renderMetricChips(metrics)}</div>`;
  const failures = Array.isArray(card.active_failures) && card.active_failures.length
    ? `<div class="design-net-failures">${card.active_failures.map(escapeHtml).join(' · ')}</div>`
    : '';
  const canBuildTune = !minimalOnly &&
    verified &&
    card.pass !== false;
  return `<div class="path-item design-net-row${isSel ? ' selected' : ''}" ` +
    `data-nid="${escapeHtml(nid)}" data-inp="${escapeHtml(inp)}" data-out="${escapeHtml(out)}" ` +
    `role="button" tabindex="0" title="Click to emit this network on the Reactions port">` +
    `<div class="design-net-id">${escapeHtml(shortNid(nid))}` +
    `<div class="design-net-io">[${escapeHtml(inp)} -> ${escapeHtml(out)}]` +
    ` · (d,r,mu)=(${escapeHtml(complexity.d ?? card.d ?? '?')}, ${escapeHtml(complexity.r ?? card.r ?? '?')}, ${escapeHtml(complexity.mu ?? card.mu ?? '?')})</div>` +
    `<div class="design-net-grade">${escapeHtml(grade)}</div>` +
    metricLine + failures + `</div>` +
    `<div class="design-net-actions">` +
    `<span class="tag tag-atlas-ok design-emit-badge" style="${isSel ? '' : 'display:none;'}">emitting</span>` +
    `${minimalOnly ? '<span class="tag tag-atlas-muted">minimal</span>' : statusTag}` +
    `${canBuildTune ? '<button class="btn btn-small design-build-btn">Build &amp; tune -></button>' : ''}` +
    `</div></div>`;
}

function renderMinimalCertificates(certificates = [], selectedKey = null) {
  if (!certificates.length) return '';
  const sections = certificates.map(cell => {
    const networks = cell.networks || [];
    const rows = networks.map(nw => renderCandidateRow({
      ...nw,
      d: cell.d,
      r: cell.r,
      mu: cell.mu,
      certificate_grade: nw.certificate_grade || 'minimal-structural-certificate',
    }, { selectedKey, minimalOnly: true })).join('');
    return `<section class="siso-section design-cert-section">` +
      `<div class="siso-section-head">` +
      `<div class="siso-section-title">minimal (d, r, mu) = (${escapeHtml(cell.d)}, ${escapeHtml(cell.r)}, ${escapeHtml(cell.mu)})</div>` +
      `<div class="text-dim">${networks.length} network${networks.length === 1 ? '' : 's'}</div></div>` +
      rows + `</section>`;
  }).join('');
  return `<section class="siso-section design-screen-group">` +
    `<div class="siso-section-head"><div class="siso-section-title">Minimal certificates</div>` +
    `<div class="text-dim">structural proof objects</div></div>` +
    `<div class="text-dim">These remain useful certificates of qualitative realizability. They are not automatically the easiest networks to tune.</div>` +
    sections + `</section>`;
}

function renderScreenedCandidates(cards = [], selectedKey = null) {
  if (!cards.length) return '';
  const rows = cards.map(card => renderCandidateRow(card, { selectedKey })).join('');
  return `<section class="siso-section design-screen-group">` +
    `<div class="siso-section-head"><div class="siso-section-title">Exploratory screened candidates</div>` +
    `<div class="text-dim">kept for Agent review</div></div>` +
    `<div class="text-dim">These candidates are not verified recommendations for the active spec.</div>` +
    rows + `</section>`;
}

function renderConstraintAudit(audit = []) {
  if (!Array.isArray(audit) || !audit.length) return '';
  const rows = audit.map((item) => {
    const support = String(item.support_level || 'unknown');
    const tagClass = support === 'unsupported'
      ? 'tag-atlas-failed'
      : support === 'sampled_forward'
        ? 'tag-atlas-muted'
        : 'tag-atlas-ok';
    const hard = item.hard === false ? 'soft' : 'hard';
    const reason = item.reason ? `<div class="text-dim">${escapeHtml(item.reason)}</div>` : '';
    return `<div class="path-item design-audit-row">` +
      `<div><strong>${escapeHtml(item.path || '(unknown path)')}</strong>` +
      `<div class="design-net-io">${escapeHtml(item.kind || 'constraint')} · ${escapeHtml(hard)}</div>` +
      reason + `</div>` +
      `<span class="tag ${tagClass}">${escapeHtml(support)}</span>` +
      `</div>`;
  }).join('');
  return `<section class="siso-section design-screen-group">` +
    `<div class="siso-section-head"><div class="siso-section-title">Constraint audit</div>` +
    `<div class="text-dim">backend support status for the active spec</div></div>` +
    rows + `</section>`;
}

function hasHardUnsupportedAudit(audit = []) {
  return Array.isArray(audit) &&
    audit.some(item => item && item.support_level === 'unsupported' && item.hard !== false);
}

function isVerifiedRecommendationCard(card) {
  const evidenceGrades = new Set(['enforced_exact', 'enforced_sampled', 'enforced_exact+sampled_forward']);
  const screenStatuses = new Set(['verified_exact', 'verified_sampled']);
  return Boolean(card) &&
    card.pass === true &&
    evidenceGrades.has(card.evidence_grade) &&
    screenStatuses.has(card.screen_status);
}

export function renderDesignScreenResults(nodeId, data, selection = null) {
  const activeNid = selectedNid(selection);
  const activeKey = selectedCandidateKey(selection);
  const emitChip = activeNid
    ? `emitting <strong>${escapeHtml(shortNid(activeNid))}</strong>`
    : '<span class="text-dim">no network selected</span>';
  const summaryPrefix = data?.designable
    ? `<div class="siso-summary-line">` +
      `<span class="summary-chip"><span class="tag tag-atlas-ok">designable</span></span>` +
      `<span class="summary-chip"><strong>${escapeHtml(data.n_matches || 0)}</strong> realizing slices</span>` +
      `<span class="summary-chip"><strong>${escapeHtml(data.screened_count || 0)}</strong> screened</span>` +
      `<span class="summary-chip" id="${escapeHtml(nodeId)}-emit">${emitChip}</span></div>` +
      `<div class="text-dim">Design Target consumes a DesignabilitySpec, separates verified evidence from exploratory candidates, then emits the selected reaction network downstream. Model Builder and Placer stay as separate nodes.</div>`
    : `<div class="siso-summary-line"><span class="summary-chip"><span class="tag tag-atlas-failed">not designable</span></span></div>` +
      `<div class="text-dim">Within the grammar (d<=4, r<=5, mu<=5). Under dominance-closure this is a parameter-independent impossibility, not merely unobserved.</div>`;
  const audit = data?.constraint_audit || [];
  if (!data?.designable) {
    return summaryPrefix +
      renderConstraintAudit(audit) +
      `<section class="siso-section design-screen-group">` +
      `<div class="siso-section-head"><div class="siso-section-title">Verified recommendations</div>` +
      `<div class="text-dim">exact or sampled evidence only</div></div>` +
      '<div class="text-dim">No verified recommendation is available for the active spec.</div>' +
      `</section>`;
  }
  const recommendedRaw = data.verified_recommendations || [];
  const hardBlocked = hasHardUnsupportedAudit(audit);
  const recommended = hardBlocked ? [] : recommendedRaw.filter(isVerifiedRecommendationCard);
  const rejectedRecommended = hardBlocked ? recommendedRaw : recommendedRaw.filter(card => !isVerifiedRecommendationCard(card));
  const screenedCandidates = [
    ...rejectedRecommended,
    ...(data.screened_candidates || data.near_misses || []),
  ];
  const recommendedRows = recommended.map(card => renderCandidateRow(card, { selectedKey: activeKey, verified: true })).join('');
  return summaryPrefix +
    renderConstraintAudit(audit) +
    `<section class="siso-section design-screen-group">` +
    `<div class="siso-section-head"><div class="siso-section-title">Verified recommendations</div>` +
    `<div class="text-dim">exact or sampled evidence only</div></div>` +
    (recommendedRows || '<div class="text-dim">No verified recommendation is available for the active spec.</div>') +
    `</section>` +
    renderScreenedCandidates(screenedCandidates, activeKey) +
    renderMinimalCertificates(data.minimal_certificates || [], activeKey);
}
