// Biocircuits Explorer — Design Agent view
// ---------------------------------------------------------------------------
// The conversational inverse-design surface. The left pane (chat) is the *front
// end*: describe the behavior you want and the agent compiles it to a behavior
// spec, retrieves verified candidates from the atlas, and returns reaction
// networks with their evidence. It POSTs each turn to the design-chat backend
// (webapp/scripts/chat_api.py — see chatApiUrl()); a backend-status pill at the
// top of the chat reflects /health. The right pane shows the top candidate: a
// per-family visualisation (truth table / surface bars / context strip /
// illustrative dose curve synthesised from the candidate's real metrics) + its
// reaction rules. Nothing here is canned — the first real reply comes from the
// backend; without one the agent shows an actionable offline state.
//
// As with editor-ui.js, all DOM is created programmatically — index-node.html
// only carries the header view-switch markup and the stylesheet link.

import { getLLMConfig } from './llm-settings.js';   // UI key panel -> per-request LLM config

const SVG_NS = 'http://www.w3.org/2000/svg';
const CHATW_KEY = 'bcx-agent-chatw';
const VIEW_KEY = 'bcx-node-view';
const CHAT_API_KEY = 'bcx-chat-api';
const DEFAULT_CHATW = 440;
const DEFAULT_CHAT_API = 'http://127.0.0.1:8765/design-chat';
const DESIGNABILITY_SPEC_VERSION = 'bne-designability/v1.0.0';

// Backend chat endpoint (webapp/scripts/chat_api.py), resolved lazily each call so
// the native macOS shell can pin the real port after the page has loaded:
//   window.__BCX_CHAT_API__ (set by setDesignChatEndpoint) > localStorage > default.
function chatApiUrl() {
  if (typeof window !== 'undefined' && window.__BCX_CHAT_API__) return window.__BCX_CHAT_API__;
  try { return localStorage.getItem(CHAT_API_KEY) || DEFAULT_CHAT_API; }
  catch (_) { return DEFAULT_CHAT_API; }
}
function healthUrl() {
  try { return new URL('/health', chatApiUrl()).toString(); }
  catch (_) { return DEFAULT_CHAT_API.replace('/design-chat', '/health'); }
}

// Let the native shell (or the dev console) point the agent at a specific backend.
export function setDesignChatEndpoint(url) {
  if (!url) return;
  window.__BCX_CHAT_API__ = String(url);
  try { localStorage.setItem(CHAT_API_KEY, String(url)); } catch (_) { /* ignore */ }
  refreshBackendStatus();   // re-probe so the status pill reflects the new target
}
if (typeof window !== 'undefined') window.setDesignChatEndpoint = setDesignChatEndpoint;

let agentBuilt = false;
let threadEl = null;
let resultsRulesEl = null;   // the right-pane rules list, updated to the top candidate
let resultsChartEl = null;   // the right-pane chart-wrap, updated to a per-candidate viz
let resultsChartTitleEl = null;
let statusDotEl = null;      // backend-health pill (dot + text) at the top of the chat
let statusTextEl = null;
let chatState = {};          // conversation spec state, echoed to the backend each turn
let activeCandidate = null;  // the candidate currently shown on the right (export target)
let exportBtnEl = null;      // "Export to Workspace" button (enabled once a candidate is active)
let exportSpecBtnEl = null;  // exports the compiled DesignabilitySpec, when the backend returns one
let convoLog = [];           // re-renderable turn log {role:'user',text} | {role:'agent',res} —
                             // persisted WITH the workspace document so each project carries its
                             // own Design-Agent conversation (one project = one workspace + one chat).

function setActiveCandidate(card) {
  activeCandidate = card || null;
  if (exportBtnEl) exportBtnEl.disabled = !isExportableAgentCard(card);
  if (exportSpecBtnEl) exportSpecBtnEl.disabled = !extractAgentDesignabilitySpec(card);
}

function finiteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function nonnegativeFiniteNumber(value) {
  const number = finiteNumber(value);
  return number != null && number >= 0 ? number : null;
}

function hasOwn(obj, key) {
  return obj && Object.prototype.hasOwnProperty.call(obj, key);
}

function plainObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value);
}

function ownKeysAllowed(obj, allowed) {
  if (!plainObject(obj)) return false;
  const allowedSet = new Set(allowed);
  return Object.keys(obj).every(key => allowedSet.has(key));
}

function finiteBounds(value) {
  return Array.isArray(value) &&
    value.length === 2 &&
    value.every(item => finiteNumber(item) != null) &&
    value[0] <= value[1];
}

function positiveInteger(value) {
  return typeof value === 'number' && Number.isInteger(value) && value > 0 ? value : null;
}

function nonnegativeInteger(value) {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null;
}

function samplePointsInteger(value) {
  return typeof value === 'number' &&
    Number.isInteger(value) &&
    value >= 11 &&
    value <= 1001
    ? value
    : null;
}

const SOURCE_KINDS = new Set(['manual_config', 'agent_design', 'legacy_shorthand', 'imported_json', 'test_fixture', 'hand_authored']);
const RANKING_PREFERENCES = new Set(['evidence_grade', 'certificate_grade', 'chebyshev_radius', 'tunable_volume', 'dynamic_range', 'transition_spacing', 'sampled_robustness', 'condition_number', 'complexity']);

function wrappedOptionalString(obj, key) {
  return !hasOwn(obj, key) || typeof obj[key] === 'string';
}

function wrappedOptionalBoolean(obj, key) {
  return !hasOwn(obj, key) || typeof obj[key] === 'boolean';
}

function wrappedOptionalNonnegativeNumber(obj, key) {
  return !hasOwn(obj, key) || nonnegativeFiniteNumber(obj[key]) != null;
}

function wrappedOptionalFraction(obj, key) {
  const value = nonnegativeFiniteNumber(obj[key]);
  return !hasOwn(obj, key) || (value != null && value <= 1);
}

function wrappedOptionalNonnegativeInt(obj, key) {
  return !hasOwn(obj, key) || nonnegativeInteger(obj[key]) != null;
}

function wrappedOptionalPositiveInt(obj, key) {
  return !hasOwn(obj, key) || positiveInteger(obj[key]) != null;
}

function lowerAgentBehaviorProgram(program) {
  if (!Array.isArray(program) || program.length === 0) return null;
  const out = [];
  for (const step of program) {
    if (!ownKeysAllowed(step, ['kind', 'value', 'operator', 'hard'])) return null;
    if (step.kind !== 'reaction_order') return null;
    const value = finiteNumber(step.value);
    if (value == null) return null;
    const operator = hasOwn(step, 'operator') ? step.operator : '=';
    if (operator !== '=' && operator !== '==') return null;
    if (!wrappedOptionalBoolean(step, 'hard')) return null;
    out.push({ kind: 'reaction_order', operator, value });
  }
  return out;
}

function wrappedBehaviorStepValid(step) {
  if (!ownKeysAllowed(step, ['kind', 'value', 'operator', 'hard'])) return false;
  if (step.kind !== 'reaction_order') return false;
  if (finiteNumber(step.value) == null) return false;
  if (hasOwn(step, 'operator') && !['=', '=='].includes(step.operator)) return false;
  return wrappedOptionalBoolean(step, 'hard');
}

function wrappedBehaviorProgramLength(behavior) {
  const program = behavior?.program;
  if (!Array.isArray(program) || program.length === 0) return null;
  return program.every(wrappedBehaviorStepValid) ? program.length : null;
}

function wrappedInputWindowValid(inputWindow) {
  if (!ownKeysAllowed(inputWindow, ['input_log10', 'hard', 'min_spacing_decades', 'operating_points_log10'])) return false;
  if (hasOwn(inputWindow, 'input_log10') && !finiteBounds(inputWindow.input_log10)) return false;
  if (!wrappedOptionalBoolean(inputWindow, 'hard')) return false;
  if (!wrappedOptionalNonnegativeNumber(inputWindow, 'min_spacing_decades')) return false;
  if (!hasOwn(inputWindow, 'input_log10') && hasOwn(inputWindow, 'min_spacing_decades')) return false;
  if (hasOwn(inputWindow, 'operating_points_log10') &&
      (!Array.isArray(inputWindow.operating_points_log10) ||
       inputWindow.operating_points_log10.some(value => finiteNumber(value) == null))) {
    return false;
  }
  if (!hasOwn(inputWindow, 'input_log10') && hasOwn(inputWindow, 'operating_points_log10')) return false;
  return true;
}

function wrappedOperatingPointsValid(inputWindow, programLength, constraints) {
  if (!hasOwn(inputWindow, 'operating_points_log10')) return true;
  const points = inputWindow.operating_points_log10;
  const bounds = inputWindow.input_log10;
  if (!Array.isArray(points) || programLength == null || points.length !== programLength) return false;
  if (!finiteBounds(bounds)) return false;
  const [lo, hi] = bounds;
  for (const point of points) {
    if (finiteNumber(point) == null || point < lo || point > hi) return false;
  }
  const windowSpacing = hasOwn(inputWindow, 'min_spacing_decades')
    ? nonnegativeFiniteNumber(inputWindow.min_spacing_decades)
    : 0;
  if (windowSpacing == null) return false;
  const transitions = plainObject(constraints?.transitions) ? constraints.transitions : {};
  const transitionSpacing = hasOwn(transitions, 'min_spacing_decades')
    ? nonnegativeFiniteNumber(transitions.min_spacing_decades)
    : 0;
  if (transitionSpacing == null) return false;
  const spacing = Math.max(windowSpacing, transitionSpacing);
  for (let index = 0; index < points.length - 1; index += 1) {
    if (points[index + 1] - points[index] + 1e-9 < spacing) return false;
  }
  return true;
}

function wrappedSourceValid(source) {
  if (!ownKeysAllowed(source, ['kind', 'node_id', 'agent_message_id', 'provenance'])) return false;
  if (!SOURCE_KINDS.has(source.kind)) return false;
  if (!wrappedOptionalString(source, 'node_id')) return false;
  if (!wrappedOptionalString(source, 'agent_message_id')) return false;
  return !hasOwn(source, 'provenance') || plainObject(source.provenance);
}

function wrappedLegacyTargetValid(legacy) {
  if (!ownKeysAllowed(legacy, ['target_kind', 'target'])) return false;
  if (!['sign', 'exact', 'label'].includes(legacy.target_kind)) return false;
  if (!hasOwn(legacy, 'target')) return false;
  if (legacy.target_kind !== 'exact') return true;
  return Array.isArray(legacy.target) &&
    legacy.target.length > 0 &&
    legacy.target.every(value => finiteNumber(value) != null);
}

function wrappedTemporalDynamicsValid(temporal) {
  if (!ownKeysAllowed(temporal, ['stimulus', 'trace', 'peak_width_seconds', 'hard'])) return false;
  if (hasOwn(temporal, 'stimulus') && !plainObject(temporal.stimulus)) return false;
  if (hasOwn(temporal, 'trace') && !Array.isArray(temporal.trace)) return false;
  if (hasOwn(temporal, 'peak_width_seconds')) {
    const peak = temporal.peak_width_seconds;
    if (!ownKeysAllowed(peak, ['min', 'max'])) return false;
    if (!hasOwn(peak, 'min') && !hasOwn(peak, 'max')) return false;
    if (!wrappedOptionalNonnegativeNumber(peak, 'min')) return false;
    if (!wrappedOptionalNonnegativeNumber(peak, 'max')) return false;
    const minPeak = hasOwn(peak, 'min') ? finiteNumber(peak.min) : null;
    const maxPeak = hasOwn(peak, 'max') ? finiteNumber(peak.max) : null;
    if (minPeak != null && maxPeak != null && minPeak > maxPeak) return false;
  }
  return wrappedOptionalBoolean(temporal, 'hard');
}

function wrappedParameterBoundsValid(bounds) {
  if (!ownKeysAllowed(bounds, ['basis', 'kd_log10', 'total_log10', 'by_class'])) return false;
  if (hasOwn(bounds, 'basis') && bounds.basis !== 'log10_qK') return false;
  if (!hasOwn(bounds, 'kd_log10') && !hasOwn(bounds, 'total_log10') && !hasOwn(bounds, 'by_class')) return false;
  if (hasOwn(bounds, 'kd_log10') && !finiteBounds(bounds.kd_log10)) return false;
  if (hasOwn(bounds, 'total_log10') && !finiteBounds(bounds.total_log10)) return false;
  if (hasOwn(bounds, 'by_class')) {
    const byClass = bounds.by_class;
    if (!ownKeysAllowed(byClass, ['kd', 'total'])) return false;
    if (!hasOwn(byClass, 'kd') && !hasOwn(byClass, 'total')) return false;
    if (hasOwn(byClass, 'kd') && !finiteBounds(byClass.kd)) return false;
    if (hasOwn(byClass, 'total') && !finiteBounds(byClass.total)) return false;
    if (hasOwn(bounds, 'kd_log10') && hasOwn(byClass, 'kd')) return false;
    if (hasOwn(bounds, 'total_log10') && hasOwn(byClass, 'total')) return false;
  }
  return true;
}

function wrappedNetworkValid(network) {
  if (!ownKeysAllowed(network, ['max_species', 'max_reactions', 'max_mu', 'allow_near_minimal'])) return false;
  if (!wrappedOptionalPositiveInt(network, 'max_species')) return false;
  if (!wrappedOptionalNonnegativeInt(network, 'max_reactions')) return false;
  if (!wrappedOptionalPositiveInt(network, 'max_mu')) return false;
  return wrappedOptionalBoolean(network, 'allow_near_minimal');
}

function wrappedRobustnessValid(robustness) {
  if (!ownKeysAllowed(robustness, ['min_chebyshev_radius', 'min_tunable_volume_lower_bound', 'min_tunable_volume', 'condition_number_max', 'min_sampled_pass_fraction', 'hard'])) return false;
  if (hasOwn(robustness, 'min_tunable_volume_lower_bound') && hasOwn(robustness, 'min_tunable_volume')) return false;
  for (const key of ['min_chebyshev_radius', 'min_tunable_volume_lower_bound', 'min_tunable_volume', 'condition_number_max']) {
    if (!wrappedOptionalNonnegativeNumber(robustness, key)) return false;
  }
  if (!wrappedOptionalFraction(robustness, 'min_sampled_pass_fraction')) return false;
  return wrappedOptionalBoolean(robustness, 'hard');
}

function wrappedCandidateBudgetValid(candidateBudget) {
  if (!ownKeysAllowed(candidateBudget, ['mode', 'max_extra_species', 'max_extra_reactions', 'max_extra_mu', 'max_screened', 'max_verified_recommendations', 'max_recommended', 'max_near_misses', 'max_exact_placements'])) return false;
  if (hasOwn(candidateBudget, 'mode') && !['near_minimal', 'all_matches'].includes(candidateBudget.mode)) return false;
  return ['max_extra_species', 'max_extra_reactions', 'max_extra_mu', 'max_screened', 'max_verified_recommendations', 'max_recommended', 'max_near_misses', 'max_exact_placements']
    .every(key => wrappedOptionalNonnegativeInt(candidateBudget, key));
}

function wrappedRankingPolicyValid(rankingPolicy) {
  if (!ownKeysAllowed(rankingPolicy, ['verified_only', 'prefer'])) return false;
  if (hasOwn(rankingPolicy, 'verified_only') && rankingPolicy.verified_only !== true) return false;
  return !hasOwn(rankingPolicy, 'prefer') ||
    (Array.isArray(rankingPolicy.prefer) &&
     rankingPolicy.prefer.every(value => typeof value === 'string' && RANKING_PREFERENCES.has(value)));
}

function wrappedRankingPreferencePrerequisitesValid(raw) {
  const prefer = raw?.ranking_policy?.prefer;
  if (!Array.isArray(prefer)) return true;
  const constraints = plainObject(raw.constraints) ? raw.constraints : {};
  const transitions = plainObject(constraints.transitions) ? constraints.transitions : {};
  for (const preference of prefer) {
    if (preference === 'dynamic_range' && !plainObject(constraints.dynamic_range)) return false;
    if (preference === 'transition_spacing' && !hasOwn(transitions, 'min_spacing_decades')) return false;
    if (preference === 'condition_number' || preference === 'sampled_robustness') return false;
  }
  return true;
}

function wrappedPositiveBudget(candidateBudget, key) {
  return plainObject(candidateBudget) &&
    hasOwn(candidateBudget, key) &&
    Number.isInteger(candidateBudget[key]) &&
    candidateBudget[key] > 0;
}

function wrappedParameterBoundsPrerequisitesValid(raw) {
  const constraints = plainObject(raw.constraints) ? raw.constraints : {};
  if (plainObject(constraints.parameter_bounds)) return true;

  const candidateBudget = plainObject(raw.candidate_budget) ? raw.candidate_budget : {};
  if (wrappedPositiveBudget(candidateBudget, 'max_exact_placements')) return false;
  if (wrappedPositiveBudget(candidateBudget, 'max_verified_recommendations')) return false;

  const rankingPolicy = plainObject(raw.ranking_policy) ? raw.ranking_policy : {};
  if (rankingPolicy.verified_only === true) return false;
  const prefer = rankingPolicy.prefer;
  if (Array.isArray(prefer) &&
      prefer.some(value => ['chebyshev_radius', 'tunable_volume', 'dynamic_range', 'transition_spacing'].includes(value))) {
    return false;
  }

  const target = plainObject(raw.target) ? raw.target : {};
  if (hasOwn(target, 'output_feature') || hasOwn(target, 'shape')) return false;
  if (hasOwn(constraints, 'dynamic_range') || hasOwn(constraints, 'transitions')) return false;
  const robustness = plainObject(constraints.robustness) ? constraints.robustness : {};
  return !['min_chebyshev_radius', 'min_tunable_volume_lower_bound', 'min_tunable_volume']
    .some(key => hasOwn(robustness, key));
}

function wrappedUnsupportedHardRobustnessValid(raw) {
  const constraints = plainObject(raw.constraints) ? raw.constraints : {};
  const robustness = plainObject(constraints.robustness) ? constraints.robustness : {};
  const hard = robustness.hard !== false;
  if (!hard) return true;
  return !['condition_number_max', 'min_sampled_pass_fraction']
    .some(key => hasOwn(robustness, key));
}

function wrappedUnsupportedHardTargetClausesValid(raw) {
  const target = plainObject(raw.target) ? raw.target : {};
  for (const key of ['input_window', 'temporal_dynamics']) {
    if (!hasOwn(target, key)) continue;
    const clause = target[key];
    if (!plainObject(clause) || clause.hard !== false) return false;
  }
  return true;
}

function wrappedAuditPolicyValid(auditPolicy) {
  if (!ownKeysAllowed(auditPolicy, ['unsupported', 'path_format', 'include_supported'])) return false;
  if (hasOwn(auditPolicy, 'unsupported') && auditPolicy.unsupported !== 'block_if_hard') return false;
  if (hasOwn(auditPolicy, 'path_format') && auditPolicy.path_format !== 'json_pointer') return false;
  return wrappedOptionalBoolean(auditPolicy, 'include_supported');
}

function readAgentBehaviorSymbol(behavior, key, card, fallbackKeys) {
  if (Object.prototype.hasOwnProperty.call(behavior, key)) {
    const raw = behavior[key];
    return typeof raw === 'string' && raw.trim() ? raw.trim() : null;
  }
  for (const fallbackKey of fallbackKeys) {
    const raw = card?.[fallbackKey];
    if (typeof raw === 'string' && raw.trim()) return raw.trim();
  }
  return null;
}

function lowerAgentBehaviorTarget(raw, card = null) {
  const behavior = raw?.behavior_spec;
  const program = behavior ? lowerAgentBehaviorProgram(behavior.program) : null;
  if (behavior && program) {
    if (!ownKeysAllowed(behavior, ['feature_space', 'input', 'output', 'program', 'input_window'])) return null;
    const featureSpace = behavior.feature_space ?? 'reaction_order';
    if (featureSpace !== 'reaction_order') return null;
    const input = readAgentBehaviorSymbol(behavior, 'input', card, ['input_symbol', 'input']);
    const output = readAgentBehaviorSymbol(behavior, 'output', card, ['observe_species', 'output_symbol', 'output']);
    if (!input || !output) return null;
    if (hasOwn(behavior, 'input_window') && !wrappedInputWindowValid(behavior.input_window)) return null;
    return {
      behavior_spec: {
        feature_space: 'reaction_order',
        input,
        output,
        program,
        ...(hasOwn(behavior, 'input_window') ? { input_window: behavior.input_window } : {}),
      },
    };
  }
  return null;
}

function lowerAgentBehaviorConstraints(raw) {
  const constraints = {};
  const network = {};
  const ncRaw = raw?.network_constraints;
  if (hasOwn(raw, 'network_constraints') && (ncRaw == null || typeof ncRaw !== 'object' || Array.isArray(ncRaw))) return null;
  const nc = ncRaw || {};
  if (hasOwn(nc, 'max_reactions')) {
    const maxReactions = positiveInteger(nc.max_reactions);
    if (maxReactions == null) return null;
    network.max_reactions = maxReactions;
  }
  if (hasOwn(nc, 'max_base_species')) {
    const maxSpecies = positiveInteger(nc.max_base_species);
    if (maxSpecies == null) return null;
    network.max_species = maxSpecies;
  }
  if (Object.keys(network).length) constraints.network = network;

  const kdProfileRaw = nc.kd_profile;
  if (hasOwn(nc, 'kd_profile')) {
    if (kdProfileRaw == null || typeof kdProfileRaw !== 'object' || Array.isArray(kdProfileRaw)) return null;
    const hasMin = hasOwn(kdProfileRaw, 'log10_kd_min');
    const hasMax = hasOwn(kdProfileRaw, 'log10_kd_max');
    if (hasMin !== hasMax) return null;
    const kdMin = finiteNumber(kdProfileRaw.log10_kd_min);
    const kdMax = finiteNumber(kdProfileRaw.log10_kd_max);
    if (kdMin == null || kdMax == null || kdMin > kdMax) return null;
    constraints.parameter_bounds = { kd_log10: [kdMin, kdMax] };
  }

  const shapePreferencesRaw = raw?.shape_preferences;
  if (hasOwn(raw, 'shape_preferences')) {
    if (shapePreferencesRaw == null || typeof shapePreferencesRaw !== 'object' || Array.isArray(shapePreferencesRaw)) return null;
    if (!ownKeysAllowed(shapePreferencesRaw, ['dynamic_range_log10'])) return null;
    const dynamicRangeRaw = shapePreferencesRaw.dynamic_range_log10;
    if (hasOwn(shapePreferencesRaw, 'dynamic_range_log10')) {
      if (dynamicRangeRaw == null || typeof dynamicRangeRaw !== 'object' || Array.isArray(dynamicRangeRaw)) return null;
      if (!ownKeysAllowed(dynamicRangeRaw, ['min', 'sample_points'])) return null;
      if (!hasOwn(dynamicRangeRaw, 'min')) return null;
      const dynamicMin = finiteNumber(dynamicRangeRaw.min);
      if (dynamicMin == null) return null;
      const samplePoints = samplePointsInteger(dynamicRangeRaw.sample_points);
      if (samplePoints == null) return null;
      constraints.dynamic_range = {
        min_fold_change: Math.pow(10, dynamicMin),
        sample_points: samplePoints,
        hard: true,
      };
    }
  }

  return constraints;
}

function wrappedDesignabilitySpecValid(raw) {
  if (!ownKeysAllowed(raw, ['schema_version', 'source', 'target', 'constraints', 'candidate_budget', 'ranking_policy', 'audit_policy'])) return false;
  if (!wrappedSourceValid(raw.source)) return false;
  const target = raw.target;
  if (!ownKeysAllowed(target, ['legacy_target', 'behavior_spec', 'input_window', 'output_feature', 'temporal_dynamics', 'shape'])) return false;
  if (hasOwn(target, 'legacy_target') && hasOwn(target, 'behavior_spec')) return false;
  if (hasOwn(target, 'behavior_spec') && hasOwn(target, 'input_window')) return false;
  if (!['legacy_target', 'behavior_spec', 'output_feature', 'temporal_dynamics', 'shape'].some(key => hasOwn(target, key))) return false;
  if (hasOwn(target, 'legacy_target') && !wrappedLegacyTargetValid(target.legacy_target)) return false;
  if (hasOwn(target, 'input_window') && !wrappedInputWindowValid(target.input_window)) return false;
  if (hasOwn(target, 'temporal_dynamics') && !wrappedTemporalDynamicsValid(target.temporal_dynamics)) return false;
  const hasBehaviorSpec = hasOwn(target, 'behavior_spec');
  const behavior = target.behavior_spec;
  let programLength = null;
  if (hasBehaviorSpec) {
    if (!ownKeysAllowed(behavior, ['input', 'output', 'program', 'feature_space', 'input_window'])) return false;
    const featureSpace = behavior.feature_space ?? 'reaction_order';
    if (featureSpace !== 'reaction_order') return false;
    if (typeof behavior.input !== 'string' || !behavior.input.trim()) return false;
    if (typeof behavior.output !== 'string' || !behavior.output.trim()) return false;
    programLength = wrappedBehaviorProgramLength(behavior);
    if (programLength == null) return false;
  }
  const inputBounds = behavior?.input_window?.input_log10;
  const hasBehaviorInputWindow = finiteBounds(inputBounds);
  const operatingPoints = behavior?.input_window?.operating_points_log10;
  const constraintsForWindow = plainObject(raw.constraints) ? raw.constraints : {};
  if (hasOwn(behavior || {}, 'input_window')) {
    const inputWindow = behavior.input_window;
    if (!wrappedInputWindowValid(inputWindow)) return false;
  }
  if (Array.isArray(operatingPoints) &&
      (programLength == null || operatingPoints.length !== programLength)) {
    return false;
  }
  if (operatingPoints !== undefined) {
    if (!hasBehaviorInputWindow) return false;
    if (!Array.isArray(operatingPoints) ||
        operatingPoints.some(value => finiteNumber(value) == null)) {
      return false;
    }
    if (!wrappedOperatingPointsValid(behavior.input_window, programLength, constraintsForWindow)) return false;
  }

  if (hasOwn(target, 'output_feature')) {
    const outputFeature = target.output_feature;
    if (!ownKeysAllowed(outputFeature, ['feature', 'operator', 'value', 'sample_points', 'tolerance_log10', 'hard'])) return false;
    if (!['fold_change', 'level', 'threshold'].includes(outputFeature.feature)) return false;
    const outputOperator = hasOwn(outputFeature, 'operator') ? outputFeature.operator : '=';
    if (!['>=', '<=', '='].includes(outputOperator)) return false;
    const value = finiteNumber(outputFeature.value);
    if (value == null) return false;
    if (outputFeature.feature === 'fold_change' && value <= 0) return false;
    if (samplePointsInteger(outputFeature.sample_points) == null) return false;
    if (nonnegativeFiniteNumber(outputFeature.tolerance_log10) == null) return false;
    if (!wrappedOptionalBoolean(outputFeature, 'hard')) return false;
    if (!hasBehaviorInputWindow) return false;
  }

  if (hasOwn(target, 'shape')) {
    const shape = target.shape;
    if (!ownKeysAllowed(shape, ['class', 'monotonicity', 'sample_points', 'tolerance_log10', 'min_prominence_log10', 'min_prominence_decades', 'hard'])) return false;
    if (!['monotonic', 'bell_shaped'].includes(shape.class)) return false;
    if (samplePointsInteger(shape.sample_points) == null) return false;
    if (nonnegativeFiniteNumber(shape.tolerance_log10) == null) return false;
    if (shape.class === 'monotonic' && !['increasing', 'decreasing', 'any'].includes(shape.monotonicity)) return false;
    if (shape.class === 'monotonic' && (hasOwn(shape, 'min_prominence_log10') || hasOwn(shape, 'min_prominence_decades'))) return false;
    if (shape.class === 'bell_shaped') {
      if (hasOwn(shape, 'min_prominence_log10') && hasOwn(shape, 'min_prominence_decades')) return false;
      const prominence = hasOwn(shape, 'min_prominence_log10')
        ? nonnegativeFiniteNumber(shape.min_prominence_log10)
        : nonnegativeFiniteNumber(shape.min_prominence_decades);
      if (prominence == null) return false;
    }
    if (!wrappedOptionalBoolean(shape, 'hard')) return false;
    if (!hasBehaviorInputWindow) return false;
  }

  if (hasOwn(raw, 'constraints')) {
    const constraints = raw.constraints;
    if (!ownKeysAllowed(constraints, ['network', 'parameter_bounds', 'robustness', 'dynamic_range', 'transitions'])) return false;
    if (hasOwn(constraints, 'network') && !wrappedNetworkValid(constraints.network)) return false;
    if (hasOwn(constraints, 'parameter_bounds') && !wrappedParameterBoundsValid(constraints.parameter_bounds)) return false;
    if (hasOwn(constraints, 'robustness') && !wrappedRobustnessValid(constraints.robustness)) return false;
    if (hasOwn(constraints, 'dynamic_range')) {
      const dynamicRange = constraints.dynamic_range;
      if (!ownKeysAllowed(dynamicRange, ['min_fold_change', 'sample_points', 'hard'])) return false;
      if (nonnegativeFiniteNumber(dynamicRange.min_fold_change) == null) return false;
      if (samplePointsInteger(dynamicRange.sample_points) == null) return false;
      if (!wrappedOptionalBoolean(dynamicRange, 'hard')) return false;
      if (!hasBehaviorInputWindow) return false;
    }
    const transitions = constraints.transitions;
    if (hasOwn(constraints, 'transitions')) {
      if (!ownKeysAllowed(transitions, ['min_spacing_decades', 'order', 'hard'])) return false;
      if (!hasOwn(transitions, 'min_spacing_decades') && !hasOwn(transitions, 'order')) return false;
      if (hasOwn(transitions, 'order')) {
        const order = transitions.order;
        if (!hasBehaviorInputWindow) return false;
        if (!Array.isArray(order) || programLength == null || order.length !== programLength) return false;
        if (order.some(value => !Number.isInteger(value) || value < 0 || value >= programLength)) return false;
        if (new Set(order).size !== order.length) return false;
      }
      if (hasOwn(transitions, 'min_spacing_decades')) {
        const spacing = nonnegativeFiniteNumber(transitions.min_spacing_decades);
        if (spacing == null) return false;
        if (!hasBehaviorInputWindow || programLength == null || programLength < 2) return false;
      }
      if (!wrappedOptionalBoolean(transitions, 'hard')) return false;
    }
  }
  if (hasOwn(raw, 'candidate_budget') && !wrappedCandidateBudgetValid(raw.candidate_budget)) return false;
  if (hasOwn(raw, 'ranking_policy') && !wrappedRankingPolicyValid(raw.ranking_policy)) return false;
  if (!wrappedRankingPreferencePrerequisitesValid(raw)) return false;
  if (!wrappedParameterBoundsPrerequisitesValid(raw)) return false;
  if (!wrappedUnsupportedHardTargetClausesValid(raw)) return false;
  if (!wrappedUnsupportedHardRobustnessValid(raw)) return false;
  if (hasOwn(raw, 'audit_policy') && !wrappedAuditPolicyValid(raw.audit_policy)) return false;

  return true;
}

export function normalizeAgentDesignabilitySpec(raw, card = null) {
  if (!raw || typeof raw !== 'object') return null;
  if (raw.schema_version === DESIGNABILITY_SPEC_VERSION) {
    if (!wrappedDesignabilitySpecValid(raw)) return null;
    const source = raw.source && typeof raw.source === 'object' && !Array.isArray(raw.source) ? raw.source : {};
    return {
      ...raw,
      source: { ...source, kind: 'agent_design' },
    };
  }
  const target = lowerAgentBehaviorTarget(raw, card);
  if (!target) return null;
  const constraints = lowerAgentBehaviorConstraints(raw);
  if (!constraints) return null;
  const spec = {
    schema_version: DESIGNABILITY_SPEC_VERSION,
    source: {
      kind: 'agent_design',
      provenance: { agent_behavior_spec: raw },
    },
    target,
    constraints,
    candidate_budget: {
      mode: 'near_minimal',
      max_extra_species: 1,
      max_extra_reactions: 1,
      max_extra_mu: 1,
      max_recommended: 24,
      max_verified_recommendations: 24,
      max_screened: 24,
      max_near_misses: 12,
      max_exact_placements: 3,
    },
    ranking_policy: { verified_only: true },
    audit_policy: {
      unsupported: 'block_if_hard',
      path_format: 'json_pointer',
      include_supported: true,
    },
  };
  return wrappedDesignabilitySpecValid(spec) ? spec : null;
}

function extractAgentDesignabilitySpec(card) {
  if (!card) return null;
  const payload = cardSpecPayload(card);
  return payload.present ? normalizeAgentDesignabilitySpec(payload.value, card) : null;
}

function cardSpecPayload(card) {
  if (!card || typeof card !== 'object') return { present: false, value: null };
  if (hasOwn(card, 'designability_spec')) return { present: true, value: card.designability_spec };
  if (hasOwn(card, 'designabilitySpec')) return { present: true, value: card.designabilitySpec };
  if (hasOwn(card, 'compiled_spec')) return { present: true, value: card.compiled_spec };
  if (hasOwn(card, 'behavior_spec')) return { present: true, value: card.behavior_spec };
  if (hasOwn(card, 'agent_compiled_spec')) return { present: true, value: card.agent_compiled_spec };
  return { present: false, value: null };
}

function isExportableAgentCard(card) {
  if (!(card && (card.rules || []).length)) return false;
  const payload = cardSpecPayload(card);
  return !payload.present || !!normalizeAgentDesignabilitySpec(payload.value, card);
}

export function cardsWithAgentSpec(res) {
  return (res?.cards || []).filter((card) => {
    const payload = cardSpecPayload(card);
    return !payload.present || !!normalizeAgentDesignabilitySpec(payload.value, card);
  });
}

// (A,B) corner order (00,01,10,11), output high=1 — mirrors evaluators.jl / cards.py.
const LOGIC_TABLES = {
  AND: [0,0,0,1], OR: [0,1,1,1], NAND: [1,1,1,0], NOR: [1,0,0,0], XOR: [0,1,1,0], XNOR: [1,0,0,1],
  NIMPLY: [0,0,1,0], IMPLY: [1,1,0,1], NOT_A: [1,1,0,0], NOT_B: [1,0,1,0], A: [0,0,1,1], B: [0,1,0,1],
  CIMPLY: [1,0,1,1], BNIMPLY: [0,1,0,0], TRUE: [1,1,1,1], FALSE: [0,0,0,0],
};
function gateTable(gate) {
  if (LOGIC_TABLES[gate]) return LOGIC_TABLES[gate];
  const m = String(gate || '').match(/\(([\d,\s]+)\)/);   // legacy "none(1, 0, 1, 1)"
  return m ? m[1].split(',').map((x) => Number(x.trim())) : null;
}

/* ─── tiny DOM helpers ─── */
function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === 'class') node.className = v;
    else if (k === 'html') node.innerHTML = v;
    else if (k === 'text') node.textContent = v;
    else if (k === 'dataset') Object.assign(node.dataset, v);
    else if (k.startsWith('on') && typeof v === 'function') node.addEventListener(k.slice(2).toLowerCase(), v);
    else node.setAttribute(k, v);
  }
  for (const c of [].concat(children)) {
    if (c == null) continue;
    node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  }
  return node;
}
function svgEl(tag, attrs = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v != null) node.setAttribute(k, String(v));
  }
  return node;
}

function placeholder(text) {
  return el('div', { class: 'agent-placeholder', text });
}

function buildResultsPanel() {
  // The right pane starts empty; showCandidateViz / showCandidateRules fill it in
  // from the top candidate of a real backend reply (no fabricated seed data).
  resultsChartTitleEl = el('span', { class: 'card-title', text: 'Response curve' });
  resultsChartEl = el('div', { class: 'chart-wrap' },
    placeholder('Describe a behavior on the left — the top candidate’s response curve appears here.'));
  const chartCard = el('div', { class: 'card chart-card' }, [
    el('div', { class: 'card-head' }, resultsChartTitleEl),
    resultsChartEl,
  ]);

  resultsRulesEl = el('div', { class: 'rules-list' },
    placeholder('Reaction rules appear here once a candidate is found.'));
  // Export the active (engine-verified) candidate into the node Workspace to analyse it there.
  exportBtnEl = el('button', { class: 'export-ws-btn', type: 'button', text: 'Export to Workspace ↗',
    title: 'Open this design as a Reaction-Network node in the Workspace' });
  exportBtnEl.disabled = true;
  exportBtnEl.addEventListener('click', () => {
    if (activeCandidate && typeof window.exportNetworkToWorkspace === 'function') {
      window.exportNetworkToWorkspace(activeCandidate.rules, activeCandidate.kd);
    }
  });
  exportSpecBtnEl = el('button', { class: 'export-ws-btn', type: 'button', text: 'Export Spec',
    title: 'Open this request as a Design Spec Config connected to Design Target' });
  exportSpecBtnEl.disabled = true;
  exportSpecBtnEl.addEventListener('click', () => {
    const spec = extractAgentDesignabilitySpec(activeCandidate);
    if (spec && typeof window.exportDesignSpecToWorkspace === 'function') {
      window.exportDesignSpecToWorkspace(spec);
    }
  });
  const rulesCard = el('div', { class: 'card rules-card' }, [
    el('div', { class: 'card-head' }, [el('span', { class: 'card-title', text: 'Reaction rules' }), exportSpecBtnEl, exportBtnEl]),
    resultsRulesEl,
  ]);

  return el('div', { class: 'agent-results' }, el('div', { class: 'results-inner' }, [chartCard, rulesCard]));
}

/* ─── conversation ─── */
// An honest opening: what the agent does and how to phrase a request. No
// fabricated conversation or results — the first real reply comes from the backend.
function welcomeMessage() {
  return {
    role: 'agent',
    text: 'I’m the Biocircuits design agent. Describe the <b>behavior</b> you want from a binding network and I’ll compile it to a behavior spec, search the verified atlas, and return candidate reaction networks with their evidence.',
    closing: 'Try: <i>“a bandpass response with a gentle rise, sharp fall and a wide plateau, at most 4 reactions”</i> · <i>“an AND gate on inputs A and B”</i> · <i>“a ratio sensor for A versus B”</i>. Add an LLM key in the ⚙ panel for free-form phrasing — optional, keyword parsing works without it.',
  };
}

/* ─── backend status pill ─── */
function buildStatusBar() {
  statusDotEl = el('span', { class: 'agent-status-dot' });
  statusTextEl = el('span', { class: 'agent-status-text', text: 'Checking backend…' });
  return el('div', { class: 'agent-status', title: 'Design backend (webapp/scripts/chat_api.py)' }, [statusDotEl, statusTextEl]);
}

async function refreshBackendStatus() {
  if (!statusDotEl) return;   // pill not built yet (e.g. endpoint set before first paint)
  statusDotEl.className = 'agent-status-dot checking';
  if (statusTextEl) statusTextEl.textContent = 'Checking backend…';
  try {
    const res = await fetch(healthUrl(), { method: 'GET' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const h = await res.json();
    // The agent can only return VERIFIED designs when the live compute engine is up; surface
    // that distinctly from the chat backend (the engine does the real ODE/equilibrium solves).
    const engineReady = !(h && h.engine) || h.engine.ready;
    if (engineReady) {
      statusDotEl.className = 'agent-status-dot online';
      if (statusTextEl) statusTextEl.textContent = 'Agent ready';
    } else {
      statusDotEl.className = 'agent-status-dot checking';
      if (statusTextEl) statusTextEl.textContent = 'Chat up, but COMPUTE ENGINE OFFLINE — start the node Workspace server to get verified designs';
    }
  } catch (_) {
    statusDotEl.className = 'agent-status-dot offline';
    if (statusTextEl) statusTextEl.textContent = 'Backend offline — start chat_api.py';
  }
}

function buildMessage(m) {
  if (m.role === 'user') {
    // User text is rendered as textContent (never innerHTML) — no injection.
    return el('div', { class: 'msg user' }, el('div', { class: 'bubble', text: m.text }));
  }

  const parts = [el('div', { class: 'who' }, [el('span', { class: 'dot' }), 'Design Agent'])];
  // m.text / m.closing here are trusted CONSTANT copy (welcome / pending) with inline
  // <b>/<i> emphasis. Dynamic backend text is rendered via textContent elsewhere
  // (buildReplyMessage, candCard, and the submit() error path) — never innerHTML.
  if (m.text) parts.push(el('div', { class: 'agent-text', html: m.text }));
  if (m.closing) parts.push(el('div', { class: 'agent-text', html: m.closing }));

  return el('div', { class: 'msg agent' }, parts);
}

function scrollThreadToBottom() {
  if (threadEl) threadEl.scrollTop = threadEl.scrollHeight;
}

function appendMessage(m) {
  if (!threadEl) return;
  threadEl.appendChild(buildMessage(m));
  scrollThreadToBottom();
}

/* ─── live backend reply rendering ─── */
function rxnChips(rules, networkId, kd) {
  const list = (rules && rules.length) ? rules : (networkId ? [networkId] : []);
  if (!list.length) return null;
  const kds = Array.isArray(kd) ? kd : [];
  return el('div', { class: 'rxn-inline' }, list.map((r, i) =>
    el('span', { class: 'rxn-chip' }, [
      String(r).replace(/<->|<=>/g, '⇌'),
      (kds[i] != null ? el('span', { class: 'chip-kd', text: ' · Kd ' + kds[i] }) : null),
    ])));
}

function candCard(card, family) {
  let head, meta = '';
  if (family === 'logic') {
    head = `${card.realized_gate} gate · ${(card.inputs || []).join(',')}→${card.output}`;
    meta = `support ${card.gate_support} · margin ${card.margin_decades} dec`;
  } else if (family === 'analog_surface') {
    head = `${card.kind} · ${(card.inputs || []).join(',')}→${card.output}`;
    meta = `coact ${card.coactivation_corr} · ratio ${card.ratio_corr} · bump ${card.bump_fraction} · ${card.dynamic_range_decades} dec`;
  } else if (family === 'contextual_versatility') {
    head = `reprogrammable: ${(card.distinct_gates || []).join(' / ')} · ${(card.inputs || []).join(',')}→${card.output}`;
    meta = (card.per_context || []).map((p) => `${card.context_sym}=${p.context}:${p.gate}(${p.support})`).join('  ');
  } else {
    head = `${card.dominant_shape || card.verdict || 'computed'} · r=${card.n_reactions} → ${card.output_symbol}`;
    const bits = [];
    if (card.shape_support != null) bits.push(`shape_support ${card.shape_support}`);
    if (card.evidence_tier) bits.push(card.evidence_tier);
    meta = bits.join(' · ');
  }
  const parts = [el('div', { class: 'cand-head', text: head })];
  if (meta) parts.push(el('div', { class: 'cand-meta', text: meta }));
  const chips = rxnChips(card.rules, card.network_id, card.kd);
  if (chips) parts.push(chips);
  return el('div', { class: 'cand-card fam-' + family }, parts);
}

/* ─── per-candidate visualisation for the results pane ─── */
// "nice" round tick values spanning [min,max] (~target of them) + a compact label formatter.
function niceTicks(min, max, target) {
  const span = max - min;
  if (!(span > 0) || !isFinite(span)) return [min];
  const raw = span / Math.max(1, target);
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const n = raw / mag;
  const step = (n < 1.5 ? 1 : n < 3 ? 2 : n < 7 ? 5 : 10) * mag;
  const out = [];
  for (let v = Math.ceil(min / step) * step; v <= max + 1e-9; v += step) out.push(Math.abs(v) < 1e-9 ? 0 : v);
  return out.length ? out : [min, max];
}
function fmtTick(v) {
  if (v !== 0 && (Math.abs(v) >= 1000 || Math.abs(v) < 0.01)) return v.toExponential(0);
  return String(Math.round(v * 100) / 100);
}
// dark-blue → teal → yellow colour ramp for the 2-input surface heatmap
function _heatColor(t) {
  t = Math.max(0, Math.min(1, isFinite(t) ? t : 0));
  const stops = [[0, [21, 35, 58]], [0.5, [23, 136, 156]], [1, [232, 210, 74]]];
  let a = stops[0], b = stops[stops.length - 1];
  for (let i = 0; i < stops.length - 1; i++) { if (t >= stops[i][0] && t <= stops[i + 1][0]) { a = stops[i]; b = stops[i + 1]; break; } }
  const f = (t - a[0]) / ((b[0] - a[0]) || 1);
  const c = a[1].map((v, k) => Math.round(v + (b[1][k] - v) * f));
  return `rgb(${c[0]},${c[1]},${c[2]})`;
}
// Real 2-input response surface (engine-computed) as a heatmap with axes, units + a colourbar.
function buildHeatmap(card) {
  const s = card.surface;
  const z = s && Array.isArray(s.z) ? s.z : [];
  const flat = z.flat().filter((v) => isFinite(v));
  if (!flat.length) return placeholder('No engine-computed surface for this candidate.');
  const xs = s.x, ys = s.y, nx = xs.length, ny = ys.length;
  const zmin = Math.min(...flat), zmax = Math.max(...flat), zr = (zmax - zmin) || 1;
  const W = 640, H = 360, padL = 58, padR = 92, padT = 16, padB = 48;
  const plotW = W - padL - padR, plotH = H - padT - padB, cw = plotW / nx, ch = plotH / ny;
  const svg = svgEl('svg', { viewBox: `0 0 ${W} ${H}` });
  for (let i = 0; i < nx; i++) for (let j = 0; j < ny; j++) {
    const v = z[i][j]; if (!isFinite(v)) continue;
    svg.appendChild(svgEl('rect', { x: (padL + i * cw).toFixed(1), y: (padT + (ny - 1 - j) * ch).toFixed(1),
      width: (cw + 0.6).toFixed(1), height: (ch + 0.6).toFixed(1), fill: _heatColor((v - zmin) / zr) }));
  }
  const xMin = xs[0], xMax = xs[nx - 1], yMin = ys[0], yMax = ys[ny - 1];
  const sx = (x) => padL + (x - xMin) / ((xMax - xMin) || 1) * plotW;
  const sy = (y) => padT + (yMax - y) / ((yMax - yMin) || 1) * plotH;
  niceTicks(xMin, xMax, 6).forEach((t) => { const X = sx(t); const l = svgEl('text', { class: 'tick-text', x: X, y: padT + plotH + 15, 'text-anchor': 'middle' }); l.textContent = fmtTick(t); svg.appendChild(l); });
  niceTicks(yMin, yMax, 5).forEach((t) => { const Y = sy(t); const l = svgEl('text', { class: 'tick-text', x: padL - 8, y: Y + 3, 'text-anchor': 'end' }); l.textContent = fmtTick(t); svg.appendChild(l); });
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: padL, y1: padT + plotH, x2: padL + plotW, y2: padT + plotH }));
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: padL, y1: padT, x2: padL, y2: padT + plotH }));
  const xt = svgEl('text', { class: 'axis-title', x: padL + plotW / 2, y: H - 8, 'text-anchor': 'middle' }); xt.textContent = 'log₁₀ ' + (s.input1 || 'input 1'); svg.appendChild(xt);
  const ymid = padT + plotH / 2; const yt = svgEl('text', { class: 'axis-title', x: 14, y: ymid, 'text-anchor': 'middle', transform: `rotate(-90 14 ${ymid})` }); yt.textContent = 'log₁₀ ' + (s.input2 || 'input 2'); svg.appendChild(yt);
  // colourbar
  const cbx = padL + plotW + 24, cbw = 12, cbh = plotH;
  const defs = svgEl('defs'), lg = svgEl('linearGradient', { id: 'hmgrad', x1: '0', y1: '1', x2: '0', y2: '0' });
  for (let k = 0; k <= 8; k++) lg.appendChild(svgEl('stop', { offset: (k / 8 * 100) + '%', 'stop-color': _heatColor(k / 8) }));
  defs.appendChild(lg); svg.appendChild(defs);
  svg.appendChild(svgEl('rect', { x: cbx, y: padT, width: cbw, height: cbh, fill: 'url(#hmgrad)' }));
  const ct = svgEl('text', { class: 'tick-text', x: cbx + cbw + 4, y: padT + 9, 'text-anchor': 'start' }); ct.textContent = fmtTick(zmax); svg.appendChild(ct);
  const cb = svgEl('text', { class: 'tick-text', x: cbx + cbw + 4, y: padT + cbh, 'text-anchor': 'start' }); cb.textContent = fmtTick(zmin); svg.appendChild(cb);
  const cl = svgEl('text', { class: 'axis-title', x: cbx + cbw / 2, y: padT - 5, 'text-anchor': 'middle' }); cl.textContent = 'log₁₀[' + (s.observe || 'out') + ']'; svg.appendChild(cl);
  return el('div', { class: 'cand-viz' }, [el('div', { class: 'chart' }, svg)]);
}
function buildCandidateViz(card, family) {
  if (card && card.surface) return buildHeatmap(card);   // engine-computed 2-input surface
  if (family === 'logic') {
    const t = gateTable(card.realized_gate) || [0, 0, 0, 0];
    const cell = (v) => el('div', { class: 'tt-cell ' + (v ? 'hi' : 'lo'), text: String(v) });
    const lbl = (s) => el('div', { class: 'tt-cell lbl', text: s });
    const grid = el('div', { class: 'tt-grid' }, [
      lbl(''), lbl('B=lo'), lbl('B=hi'),
      lbl('A=lo'), cell(t[0]), cell(t[1]),
      lbl('A=hi'), cell(t[2]), cell(t[3]),
    ]);
    return el('div', { class: 'cand-viz' }, [grid]);
  }
  if (family === 'analog_surface') {
    const bar = (name, v) => {
      const fill = el('div', { class: 'bar-fill' }); fill.style.width = Math.round(Math.min(1, Math.abs(v || 0)) * 100) + '%';
      return el('div', { class: 'bar-row' }, [el('span', { text: name }), el('div', { class: 'bar-track' }, fill), el('span', { text: String(v) })]);
    };
    return el('div', { class: 'cand-viz' }, [
      bar('coactivation', card.coactivation_corr), bar('ratio (A/B)', card.ratio_corr), bar('interior bump', card.bump_fraction),
    ]);
  }
  if (family === 'contextual_versatility') {
    const pills = (card.per_context || []).map((p) =>
      el('span', { class: 'ctx-pill' }, [`${card.context_sym}=${p.context} → `, el('b', { text: p.gate }), ` (${p.support})`]));
    return el('div', { class: 'cand-viz' }, [el('div', { class: 'ctx-strip' }, pills)]);
  }
  // dose_shape: plot the REAL engine-computed curve. There is NO synthetic fallback — if no
  // computed curve is present we say so rather than drawing a fabricated one.
  const cs = Array.isArray(card.computed_series)
    ? card.computed_series.map((p) => [Number(p.x), Number(p.y)]).filter((p) => isFinite(p[0]) && isFinite(p[1]))
    : [];
  if (cs.length < 2) {
    return el('div', { class: 'cand-viz' }, placeholder('No engine-computed curve for this candidate.'));
  }
  const W = 640, H = 320, padL = 60, padR = 18, padT = 14, padB = 50;
  const xs = cs.map((p) => p[0]), ys = cs.map((p) => p[1]);
  let xMin = Math.min(...xs), xMax = Math.max(...xs), yMin = Math.min(...ys), yMax = Math.max(...ys);
  if (xMax - xMin < 1e-9) { xMin -= 1; xMax += 1; }
  const yp = (yMax - yMin < 1e-9) ? 1 : (yMax - yMin) * 0.08; yMin -= yp; yMax += yp;
  const x0 = padL, y0 = H - padB, x1 = W - padR, yTop = padT;
  const sx = (x) => x0 + (x - xMin) / (xMax - xMin) * (x1 - x0);
  const sy = (y) => yTop + (yMax - y) / (yMax - yMin) * (y0 - yTop);
  const svg = svgEl('svg', { viewBox: `0 0 ${W} ${H}` });
  // gridlines + numeric ticks — X (log10 input)
  niceTicks(xMin, xMax, 6).forEach((t) => {
    const X = sx(t);
    svg.appendChild(svgEl('line', { class: 'grid-line', x1: X, y1: yTop, x2: X, y2: y0 }));
    const lab = svgEl('text', { class: 'tick-text', x: X, y: y0 + 15, 'text-anchor': 'middle' });
    lab.textContent = fmtTick(t); svg.appendChild(lab);
  });
  // gridlines + numeric ticks — Y (log10 output concentration)
  niceTicks(yMin, yMax, 5).forEach((t) => {
    const Y = sy(t);
    svg.appendChild(svgEl('line', { class: 'grid-line', x1: x0, y1: Y, x2: x1, y2: Y }));
    const lab = svgEl('text', { class: 'tick-text', x: x0 - 8, y: Y + 3, 'text-anchor': 'end' });
    lab.textContent = fmtTick(t); svg.appendChild(lab);
  });
  // axes
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: x0, y1: y0, x2: x1, y2: y0 }));
  svg.appendChild(svgEl('line', { class: 'axis-line', x1: x0, y1: yTop, x2: x0, y2: y0 }));
  // the real engine-computed curve
  let d = ''; cs.forEach((p, i) => { d += (i ? 'L' : 'M') + sx(p[0]).toFixed(1) + ' ' + sy(p[1]).toFixed(1) + ' '; });
  svg.appendChild(svgEl('path', { class: 'agent-series target', pathLength: '1', d: d.trim(), stroke: '#17c4d6', fill: 'none' }));
  // axis titles (units)
  const xt = svgEl('text', { class: 'axis-title', x: (x0 + x1) / 2, y: H - 8, 'text-anchor': 'middle' });
  xt.textContent = ('log₁₀ input total ' + (card.input_symbol || '')).trim(); svg.appendChild(xt);
  const ymid = (y0 + yTop) / 2;
  const yt = svgEl('text', { class: 'axis-title', x: 14, y: ymid, 'text-anchor': 'middle', transform: `rotate(-90 14 ${ymid})` });
  yt.textContent = 'log₁₀ [' + (card.output_symbol || 'output') + ']'; svg.appendChild(yt);
  return el('div', { class: 'cand-viz' }, [el('div', { class: 'chart' }, svg)]);
}
function showCandidateViz(card, family) {
  if (!resultsChartEl || !card) return;
  resultsChartEl.replaceChildren(buildCandidateViz(card, family));
  if (resultsChartTitleEl) resultsChartTitleEl.textContent = 'Top candidate';
}

function showCandidateRules(card) {
  if (!resultsRulesEl || !card) return;
  const rules = (card.rules && card.rules.length) ? card.rules : (card.network_id ? [card.network_id] : []);
  const kd = Array.isArray(card.kd) ? card.kd : [];
  resultsRulesEl.replaceChildren(...rules.map((r, i) =>
    el('div', { class: 'rule' }, [
      el('span', { class: 'eq', text: String(r).replace(/<->|<=>/g, '⇌') }),
      (kd[i] != null ? el('span', { class: 'kd' }, ['Kd ', el('b', { text: String(kd[i]) })]) : null),
    ])));
}

// Make a candidate card the active one: highlight it among its siblings and drive the right pane.
function selectCandidate(cardEl, card, family) {
  const scope = cardEl.closest('.msg.agent') || cardEl.parentNode;
  if (scope) scope.querySelectorAll('.cand-card.active').forEach((e) => e.classList.remove('active'));
  cardEl.classList.add('active');
  setActiveCandidate(card);
  showCandidateRules(card);
  showCandidateViz(card, family);
}

function buildReplyMessage(res) {
  const parts = [el('div', { class: 'who' }, [el('span', { class: 'dot' }), 'Design Agent'])];
  // The agent's natural-language reply IS the content (kind: agent / chat / need_key / error).
  // No "Compiled → …" prefix and no fabricated notes — the LLM wrote this, grounded in tool results.
  if (res.reply) parts.push(el('div', { class: 'agent-text', text: res.reply }));
  const info = res.info || {};
  if (info.engine_offline) {
    parts.push(el('div', { class: 'agent-abstain', text: '⚠ Compute engine offline — no verified design produced. Start the node Workspace server, then retry.' }));
  }
  // Cards shown are ENGINE-VERIFIED candidates; CLICK one to drive the right-pane viz + rules.
  const fam = res.family || 'dose_shape';
  const cards = cardsWithAgentSpec(res);
  const explored = (res.info && res.info.explored) || 0;
  if (cards.length > 1) {
    parts.push(el('div', { class: 'cand-hint', text: explored > cards.length
      ? `explored ${explored} designs · showing ${cards.length} best — click a card to view it`
      : `${cards.length} verified designs — click a card to view it` }));
  }
  cards.forEach((c, idx) => {
    const cc = candCard(c, fam);
    cc.addEventListener('click', () => selectCandidate(cc, c, c.family || fam));
    if (idx === 0) cc.classList.add('active');
    parts.push(cc);
  });
  return el('div', { class: 'msg agent' }, parts);
}

async function sendToBackend(text) {
  const res = await fetch(chatApiUrl(), {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: text, state: chatState, llm: getLLMConfig(), top: 3 }),
  });
  if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + (await res.text()).slice(0, 140));
  return res.json();
}

/* ─── composer ─── */
function buildComposer() {
  const ta = el('textarea', {
    rows: '1',
    'aria-label': 'Message the design agent',
    placeholder: 'Describe the behavior you want, or your available parts…',
  });
  const sendBtn = el('button', { class: 'send-btn', disabled: '', text: 'Send' });

  const grow = () => {
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
    sendBtn.disabled = !ta.value.trim();
  };
  const submit = async () => {
    const text = ta.value.trim();
    if (!text) return;
    appendMessage({ role: 'user', text });
    convoLog.push({ role: 'user', text });
    ta.value = ''; ta.style.height = 'auto'; sendBtn.disabled = true;
    const pending = buildMessage({ role: 'agent', text: 'Searching the atlas…' });
    threadEl.appendChild(pending); scrollThreadToBottom();
    try {
      const res = await sendToBackend(text);
      chatState = res.state || chatState;
      threadEl.replaceChild(buildReplyMessage(res), pending);
      convoLog.push({ role: 'agent', res });
      if (convoLog.length > 60) convoLog = convoLog.slice(-60);
      const first = cardsWithAgentSpec(res)[0];
      if (first) { setActiveCandidate(first); showCandidateRules(first); showCandidateViz(first, first.family || res.family); }
      else { setActiveCandidate(null); }
      refreshBackendStatus();
    } catch (e) {
      // Guidance is trusted constant copy (html); the error detail can echo a
      // backend/proxy response body, so render it as textContent — never innerHTML.
      const errMsg = el('div', { class: 'msg agent' }, [
        el('div', { class: 'who' }, [el('span', { class: 'dot' }), 'Design Agent']),
        el('div', { class: 'agent-text', html: 'Backend unreachable — start it with <b>python3 webapp/scripts/chat_api.py</b> (default 127.0.0.1:8765). An LLM key (⚙ panel) is optional; keyword parsing works without one.' }),
        el('div', { class: 'agent-text', text: String(e.message || e) }),
      ]);
      threadEl.replaceChild(errMsg, pending);
      refreshBackendStatus();
    }
    scrollThreadToBottom();
  };

  ta.addEventListener('input', grow);
  ta.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(); }
  });
  sendBtn.addEventListener('click', submit);

  return el('div', { class: 'agent-composer' }, el('div', { class: 'composer-box' }, [ta, sendBtn]));
}

/* ─── splitter ─── */
function clampChatWidth(w, host) {
  // Keep the chat between 320px and (viewport − 360px) so the results pane
  // never collapses — used for both the initial paint and live dragging.
  return Math.max(320, Math.min(w, host.clientWidth - 360));
}

function readChatWidth() {
  let saved = 0;
  try { saved = Number(localStorage.getItem(CHATW_KEY)); } catch (_) { /* ignore */ }
  return saved && saved > 300 ? saved : DEFAULT_CHATW;
}

function installSplitter(host, chat, split) {
  let dragging = false;
  const onMove = (e) => {
    if (!dragging) return;
    const left = host.getBoundingClientRect().left;
    chat.style.width = clampChatWidth(e.clientX - left, host) + 'px';
  };
  const onUp = () => {
    if (!dragging) return;
    dragging = false;
    split.classList.remove('dragging');
    const w = parseInt(chat.style.width, 10);
    if (w) { try { localStorage.setItem(CHATW_KEY, String(w)); } catch (_) { /* ignore */ } }
  };
  split.addEventListener('mousedown', (e) => {
    e.preventDefault();
    dragging = true;
    split.classList.add('dragging');
  });
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}

/* ─── view assembly + switching ─── */
function buildAgentView() {
  const chat = el('div', { class: 'agent-chat' });
  chat.appendChild(buildStatusBar());
  threadEl = el('div', { class: 'agent-thread' });
  threadEl.appendChild(buildMessage(welcomeMessage()));
  chat.appendChild(threadEl);
  chat.appendChild(buildComposer());
  refreshBackendStatus();   // probe /health so the pill shows connected/offline up front

  const split = el('div', { class: 'agent-split' });
  const host = el('div', { id: 'agent-view' }, [chat, split, buildResultsPanel()]);

  // Place the agent view as a sibling of #editor so the shared header sits above it.
  const editor = document.getElementById('editor');
  if (editor && editor.parentNode) editor.parentNode.insertBefore(host, editor.nextSibling);
  else document.body.appendChild(host);

  // Width is set after insertion (setNodeView flips data-node-view first, so the
  // host is laid out) and clamped to the live viewport — a value persisted on a
  // wide display can't squeeze the results pane to zero on a narrow one.
  chat.style.width = clampChatWidth(readChatWidth(), host) + 'px';

  installSplitter(host, chat, split);
  scrollThreadToBottom();
}

function ensureAgentBuilt() {
  if (agentBuilt) return;
  buildAgentView();
  agentBuilt = true;
}

// ─── Per-project conversation persistence ─────────────────────────────────────
// The workspace document carries the Design-Agent conversation, so each project =
// one workspace + one conversation. workspace.js serializeState()/applyState() call these.
function getDesignAgentConversation() {
  return { convo: convoLog, chatState };
}
function setDesignAgentConversation(rec) {
  ensureAgentBuilt();
  if (!threadEl) return;
  convoLog = (rec && Array.isArray(rec.convo)) ? rec.convo : [];
  chatState = (rec && rec.chatState) ? rec.chatState : {};
  threadEl.replaceChildren();
  if (!convoLog.length) {
    threadEl.appendChild(buildMessage(welcomeMessage()));
  } else {
    let lastCard = null, lastFam = null;
    for (const e of convoLog) {
      if (e.role === 'user') threadEl.appendChild(buildMessage({ role: 'user', text: e.text }));
      else if (e.role === 'agent' && e.res) {
        threadEl.appendChild(buildReplyMessage(e.res));
        const f = cardsWithAgentSpec(e.res)[0];
        if (f) { lastCard = f; lastFam = f.family || e.res.family; }
      }
    }
    if (lastCard) { setActiveCandidate(lastCard); showCandidateRules(lastCard); showCandidateViz(lastCard, lastFam); }
  }
  if (!convoLog.length || !activeCandidate) {
    setActiveCandidate(null);
    if (resultsRulesEl) resultsRulesEl.replaceChildren(placeholder('Reaction rules appear here once a candidate is found.'));
    if (resultsChartEl) resultsChartEl.replaceChildren(placeholder('Describe a behavior on the left — the top candidate’s response curve appears here.'));
  }
  scrollThreadToBottom();
}
if (typeof window !== 'undefined') {
  window.getDesignAgentConversation = getDesignAgentConversation;
  window.setDesignAgentConversation = setDesignAgentConversation;
}

export function setNodeView(view) {
  const v = view === 'agent' ? 'agent' : 'workspace';

  // Flip the surface attribute first so #agent-view is laid out (display:flex)
  // before we build into it — keeps width clamping / measurements correct.
  document.documentElement.dataset.nodeView = v;
  if (v === 'agent') ensureAgentBuilt();

  const agentBtn = document.getElementById('view-switch-agent');
  const wsBtn = document.getElementById('view-switch-workspace');
  if (agentBtn) { agentBtn.classList.toggle('active', v === 'agent'); agentBtn.setAttribute('aria-pressed', String(v === 'agent')); }
  if (wsBtn) { wsBtn.classList.toggle('active', v === 'workspace'); wsBtn.setAttribute('aria-pressed', String(v === 'workspace')); }

  try { localStorage.setItem(VIEW_KEY, v); } catch (_) { /* ignore */ }

  if (v === 'agent') {
    scrollThreadToBottom();
  } else {
    // The canvas was display:none while hidden; nudge size-sensitive bits
    // (grid canvas, Plotly viewers) to recompute now that it is visible again.
    window.dispatchEvent(new Event('resize'));
  }
}

export function initAgentView() {
  const agentBtn = document.getElementById('view-switch-agent');
  const wsBtn = document.getElementById('view-switch-workspace');
  if (agentBtn) agentBtn.addEventListener('click', () => setNodeView('agent'));
  if (wsBtn) wsBtn.addEventListener('click', () => setNodeView('workspace'));

  // The inline bootstrap in index-node.html already set documentElement's
  // data-node-view (from hash / localStorage). Honour it: build + sync now.
  const initial = document.documentElement.dataset.nodeView === 'agent' ? 'agent' : 'workspace';
  setNodeView(initial);
}
