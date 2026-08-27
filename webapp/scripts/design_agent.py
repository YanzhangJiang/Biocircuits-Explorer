#!/usr/bin/env python3
# Tool-calling DESIGN AGENT — engine-in-the-loop (the architecture the roadmap.tex always
# specified: "conversational agent ... calling atlas, surrogate, scan, and ROP tools", with
# "the exact verifier in the loop" and "Model as Compass, Verifier as Truth").
# ---------------------------------------------------------------------------
# The LLM is the planner/reasoner. For a design request it: (optionally) pulls a SEED from the
# atlas (a PRIOR — "what's known to exist", never the final answer), proposes a network, and
# CALLS THE LIVE JULIA ENGINE to actually solve the equilibrium dose-response and phenotype it
# (build_model → parameter_scan_1d → behavior_families). It reads the COMPUTED shape, checks it
# against the request, and refines (mutate reactions/kd, re-sweep) across turns. Every candidate
# it presents carries a fresh in-session engine result.
#
# It NEVER fabricates: if the engine is offline, the simulate tool returns {engine_offline} and
# the system prompt forbids inventing a curve/label/metric — the agent says compute is offline.
# (This is the fix for the old lookup agent that "背答案" from precomputed .jsonl and would have
# returned identical canned cards with the engine down.)
#
# Requires an LLM key (provider-agnostic: OpenAI-compatible incl. a local proxy, or Anthropic).
import os, sys, json, collections, hashlib, time, uuid, copy, math, tempfile, threading
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_search as ds
import cards as C
import llm_compile as L
import engine_client as E

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
DOSE_DS = os.path.join(ROOT, "datasets", "latent-atlas-v0")
# Richer d≤4 full-parity dose corpus (2467 networks); unioned into the dose prior when present.
DOSE_DS_D4 = os.path.join(ROOT, "datasets", "latent-atlas-d4-v0")
# Curated multimodal (RO sign-oscillation) seed pack extracted from the d4 atlas — networks the
# phenotyper found robustly multi-sign-change. Read by retrieve_multimodal_seed.
MULTIMODAL_SEEDS = os.path.join(ROOT, "datasets", "multimodal_seeds_d4.json")
LOGIC_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-logic-v0", "logic_labels.jsonl")
ANALOG_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-analog-v0", "analog_labels.jsonl")
CONTEXTUAL_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-contextual-v0", "contextual_labels.jsonl")

# ── DesignAgentTrace: one replayable record per turn (large arrays → artifacts/<hash>.json). ──
TRACE_SCHEMA_VERSION = "design-agent-trace/v0.1.0"
COMPILER_PROMPT_VERSION = "design-agent-prompt/v0.6.0"   # v0.6.0: canonical fixed-network ROP shape optimization
TRACE_DIR = os.environ.get("BNE_TRACE_DIR", os.path.join(ROOT, "traces"))

def _trace_integer_config(name, default, minimum, maximum):
    raw = os.environ.get(name)
    if raw is None:
        return default
    if (not raw.isascii() or not raw.isdecimal() or
            (len(raw) > 1 and raw.startswith("0"))):
        raise ValueError(f"{name} must be a canonical decimal integer")
    value = int(raw)
    if value < minimum or value > maximum:
        raise ValueError(f"{name} must be within [{minimum}, {maximum}]")
    return value


# Trace storage is diagnostic and best-effort, but it must not grow forever.
# A single record may exceed the segment target; in that case it occupies one
# segment by itself. Full-card artifacts are independently bounded and oldest
# artifacts may be retired while their compact trace summaries remain.
TRACE_MAX_BYTES = _trace_integer_config(
    "BNE_TRACE_MAX_BYTES", 16 * 1024 * 1024, 64 * 1024, 1024 * 1024 * 1024,
)
TRACE_ARCHIVE_COUNT = _trace_integer_config("BNE_TRACE_ARCHIVE_COUNT", 3, 0, 32)
TRACE_ARTIFACT_MAX_BYTES = _trace_integer_config(
    "BNE_TRACE_ARTIFACT_MAX_BYTES", 256 * 1024 * 1024, 1024 * 1024, 64 * 1024 * 1024 * 1024,
)
TRACE_ARTIFACT_MAX_FILES = _trace_integer_config(
    "BNE_TRACE_ARTIFACT_MAX_FILES", 4096, 1, 65536,
)
_TRACE_STORE_LOCK = threading.RLock()

DESIGNABILITY_SPEC_VERSION = "bne-designability/v1.0.0"
DESIGN_SCREEN_SCHEMA_VERSION = "bne-design-screen/v0.3.0"
ROP_SHAPE_OPTIMIZE_REQUEST_VERSION = "bne-rop-shape-optimize-request/v1.0.0"
ROP_SHAPE_OPTIMIZATION_VERSION = "bne-rop-shape-optimization/v1.0.0"

def _hash(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]

def _sha256_json(obj):
    """Full content fingerprint for reusable scientific-result identity.

    Trace summaries intentionally use the short `_hash` above.  A scan request,
    however, is part of a candidate card's scientific identity and must not use
    that display-sized prefix.
    """
    payload = json.dumps(
        obj, sort_keys=True, default=str, ensure_ascii=False, separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _evidence(card):
    """Honest evidence tier for an engine result (no laundering, per the harness evidence policy).
    A dose shape with shape_support over the Kd prior Π (phenotype_classify, K=8) = Tier 3b
    (sampled verification under a prior); a single-kd 2-input corner scan = Tier 1 (fresh scan).
    We do NOT emit Tier 3a (geometric/volume certificate) — the live agent doesn't compute one."""
    if card.get("family") == "logic" or card.get("surface"):
        return {"tier": "1", "label": "Tier 1 · fresh 2-input scan (single kd)",
                "basis": f"realized gate from a fresh equilibrium corner scan at the given kd; on/off margin {card.get('margin_decades')} dec — single kd, not Π-sampled"}
    if card.get("shape_support") is not None:
        return {"tier": "3b", "label": f"Tier 3b · support {card.get('shape_support')} over Π (K=8)",
                "basis": f"shape_support over the Kd prior Π=LogUniform(-3,3), totals pinned, K=8; phenotyper {card.get('phenotyper_version', 'v0.4.0')}"}
    return {"tier": "1", "label": "Tier 1 · fresh dose scan", "basis": "single-kd dose-response scan"}

_HEAVY_KEYS = {"_card", "surface", "candidates", "seeds", "curve_sample_log10", "phenotype_families",
               "gate_corners_00_01_10_11", "shape_fractions", "dose_classes", "logic_gate_counts"}
def _tool_summary(res):
    """Compact, array-free summary of a tool result for the trace (scalars only)."""
    if not isinstance(res, dict):
        return {}
    return {k: v for k, v in res.items() if k not in _HEAVY_KEYS and not isinstance(v, (list, dict))}

def _spill_card(card):
    """Write a candidate card's FULL artifact (incl. computed_series / surface) keyed by content hash,
    so the trace can stay compact yet the heavy result is replayable. Returns the hash."""
    rh = _hash(card)
    tmp_path = None
    try:
        payload = json.dumps(card, default=str).encode("utf-8")
        canonical_payload = json.dumps(
            card, sort_keys=True, default=str, separators=(",", ":"),
        )
        with _TRACE_STORE_LOCK:
            adir = os.path.join(TRACE_DIR, "artifacts")
            os.makedirs(adir, exist_ok=True)
            path = os.path.join(adir, rh + ".json")
            reusable = False
            if os.path.exists(path):
                try:
                    with open(path, encoding="utf-8") as fh:
                        stored = json.load(fh)
                    reusable = json.dumps(
                        stored, sort_keys=True, default=str, separators=(",", ":"),
                    ) == canonical_payload
                except (OSError, ValueError, TypeError):
                    reusable = False
            if reusable:
                # Content-addressed artifacts can be reused by later turns.
                # Refresh their recency so retention behaves as LRU rather
                # than deleting a card that the just-written trace needs.
                os.utime(path, None)
            else:
                # Repair a truncated/invalid predecessor with the same key by
                # publishing the complete payload through the same atomic path.
                fd, tmp_path = tempfile.mkstemp(prefix=f".{rh}.", suffix=".tmp", dir=adir)
                with os.fdopen(fd, "wb") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, path)
                tmp_path = None
    except Exception:
        pass
    finally:
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    return rh

def _card_summary(card, result_hash):
    keep = ("family", "realized_gate", "dominant_shape", "verdict", "shape_support", "margin_decades",
            "n_reactions", "output", "output_symbol", "inputs", "input_symbol", "kd", "rules",
            "network_ir_hash", "request_fingerprint", "verification_status", "evidence_grade",
            "evidence_tier", "partial")
    s = {k: card[k] for k in keep if k in card}
    s["result_hash"] = result_hash
    return s

def _trace_log_path(archive_index=0):
    base = os.path.join(TRACE_DIR, "traces.jsonl")
    return base if archive_index == 0 else f"{base}.{archive_index}"


def _prune_trace_archives():
    prefix = "traces.jsonl."
    try:
        names = os.listdir(TRACE_DIR)
    except FileNotFoundError:
        return
    for name in names:
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):]
        if not suffix.isascii() or not suffix.isdecimal():
            continue
        index = int(suffix)
        if index >= 1 and index <= TRACE_ARCHIVE_COUNT and str(index) == suffix:
            continue
        try:
            os.unlink(os.path.join(TRACE_DIR, name))
        except FileNotFoundError:
            pass


def _rotate_trace_log():
    current = _trace_log_path()
    if TRACE_ARCHIVE_COUNT == 0:
        try:
            os.unlink(current)
        except FileNotFoundError:
            pass
        return
    oldest = _trace_log_path(TRACE_ARCHIVE_COUNT)
    try:
        os.unlink(oldest)
    except FileNotFoundError:
        pass
    for index in range(TRACE_ARCHIVE_COUNT - 1, 0, -1):
        source = _trace_log_path(index)
        if os.path.exists(source):
            os.replace(source, _trace_log_path(index + 1))
    if os.path.exists(current):
        os.replace(current, _trace_log_path(1))


def _enforce_trace_artifact_retention():
    artifact_dir = os.path.join(TRACE_DIR, "artifacts")
    try:
        names = os.listdir(artifact_dir)
    except FileNotFoundError:
        return

    now = time.time()
    artifacts = []
    for name in names:
        path = os.path.join(artifact_dir, name)
        try:
            stat = os.stat(path)
        except OSError:
            continue
        if not os.path.isfile(path):
            continue
        if name.endswith(".tmp"):
            if now - stat.st_mtime >= 3600:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            continue
        if name.endswith(".json"):
            artifacts.append((stat.st_mtime_ns, name, stat.st_size, path))

    artifacts.sort()
    total_bytes = sum(item[2] for item in artifacts)
    while artifacts and (len(artifacts) > TRACE_ARTIFACT_MAX_FILES or
                         total_bytes > TRACE_ARTIFACT_MAX_BYTES):
        _, _, size, path = artifacts.pop(0)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError:
            continue
        total_bytes -= size


def _write_trace(trace):
    """Append one compact trace to a bounded rotating store.

    Persistence remains best-effort and never breaks an Agent turn. The LLM
    api_key is never part of the trace (only provider/model/effort).
    """
    final_response = trace.get("final_response") if isinstance(trace, dict) else None
    cards = final_response.get("cards") if isinstance(final_response, dict) else None
    enforce_artifact_retention = bool(cards)
    try:
        line = json.dumps(trace, default=str) + "\n"
        line_bytes = len(line.encode("utf-8"))
        with _TRACE_STORE_LOCK:
            os.makedirs(TRACE_DIR, exist_ok=True)
            # Configuration can be reduced between helper launches. Prune
            # stale higher-numbered archives even when this append does not
            # itself cross the rotation threshold.
            _prune_trace_archives()
            path = _trace_log_path()
            current_bytes = os.path.getsize(path) if os.path.exists(path) else 0
            if current_bytes > 0 and current_bytes + line_bytes > TRACE_MAX_BYTES:
                _rotate_trace_log()
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception as e:
        sys.stderr.write(f"[trace] write failed (non-fatal): {e}\n")
    finally:
        # Artifact growth is independent of JSONL availability. Even if log
        # rotation or append fails, a turn that spilled cards still runs the
        # bounded-store cleanup in its own best-effort path.
        if enforce_artifact_retention:
            try:
                with _TRACE_STORE_LOCK:
                    _enforce_trace_artifact_retention()
            except Exception as e:
                sys.stderr.write(
                    f"[trace] artifact retention failed (non-fatal): {e}\n"
                )

def _agent_finite_number(raw):
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    return value if math.isfinite(value) else None

def _agent_nonnegative_finite_number(raw):
    value = _agent_finite_number(raw)
    return value if value is not None and value >= 0 else None

def _agent_integral_number(raw):
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    if not math.isfinite(value) or not value.is_integer():
        return None
    return int(value)

def _agent_positive_int(raw):
    value = _agent_integral_number(raw)
    return value if value is not None and value > 0 else None

def _agent_sample_points(raw):
    value = _agent_integral_number(raw)
    return value if value is not None and 11 <= value <= 1001 else None

def _agent_nonnegative_int(raw):
    value = _agent_integral_number(raw)
    return value if value is not None and value >= 0 else None

def _agent_plain_dict(raw):
    return isinstance(raw, dict)

def _agent_keys_allowed(raw, allowed):
    return _agent_plain_dict(raw) and set(raw.keys()).issubset(set(allowed))

def _agent_finite_bounds(raw):
    return (
        isinstance(raw, list) and
        len(raw) == 2 and
        _agent_finite_number(raw[0]) is not None and
        _agent_finite_number(raw[1]) is not None and
        raw[0] <= raw[1]
    )

def _agent_optional_string(obj, key):
    return key not in obj or isinstance(obj.get(key), str)

def _agent_optional_bool(obj, key):
    return key not in obj or isinstance(obj.get(key), bool)

def _agent_optional_nonnegative_number(obj, key):
    return key not in obj or _agent_nonnegative_finite_number(obj.get(key)) is not None

def _agent_optional_fraction(obj, key):
    if key not in obj:
        return True
    value = _agent_nonnegative_finite_number(obj.get(key))
    return value is not None and value <= 1

def _agent_optional_nonnegative_int(obj, key):
    return key not in obj or _agent_nonnegative_int(obj.get(key)) is not None

def _agent_optional_finite(obj, key):
    if key not in obj:
        return True, None
    value = _agent_finite_number(obj.get(key))
    return value is not None, value

def _agent_optional_positive_int(obj, key):
    if key not in obj:
        return True, None
    value = _agent_positive_int(obj.get(key))
    return value is not None, value

def _agent_behavior_symbol(behavior, key, card, fallback_keys):
    if key in behavior:
        raw = behavior.get(key)
        return raw.strip() if isinstance(raw, str) and raw.strip() else None
    for fallback_key in fallback_keys:
        raw = (card or {}).get(fallback_key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None

def _agent_behavior_program(behavior):
    program = behavior.get("program")
    if not isinstance(program, list) or not program:
        return None
    out = []
    for step in program:
        if not _agent_keys_allowed(step, ("kind", "value", "operator", "hard")):
            return None
        if step.get("kind") != "reaction_order":
            return None
        value = _agent_finite_number(step.get("value"))
        if value is None:
            return None
        op = step.get("operator") if "operator" in step else "="
        if op not in ("=", "=="):
            return None
        if not _agent_optional_bool(step, "hard"):
            return None
        out.append({"kind": "reaction_order", "operator": op, "value": value})
    return out

_AGENT_SOURCE_KINDS = {
    "manual_config", "agent_design", "legacy_shorthand", "imported_json", "test_fixture", "hand_authored",
}
_AGENT_RANKING_PREFERENCES = {
    "evidence_grade", "certificate_grade", "chebyshev_radius", "tunable_volume",
    "dynamic_range", "transition_spacing", "sampled_robustness", "condition_number", "complexity",
}

def _agent_wrapped_behavior_step_valid(step):
    if not _agent_keys_allowed(step, ("kind", "value", "operator", "hard")):
        return False
    if step.get("kind") != "reaction_order":
        return False
    if _agent_finite_number(step.get("value")) is None:
        return False
    if step.get("operator", "=") not in ("=", "=="):
        return False
    return _agent_optional_bool(step, "hard")

def _agent_wrapped_behavior_program_length(behavior):
    program = behavior.get("program")
    if not isinstance(program, list) or not program:
        return None
    return len(program) if all(_agent_wrapped_behavior_step_valid(step) for step in program) else None

def _agent_wrapped_input_window_valid(input_window):
    if not _agent_keys_allowed(input_window, ("input_log10", "hard", "min_spacing_decades", "operating_points_log10")):
        return False
    if "input_log10" in input_window and not _agent_finite_bounds(input_window.get("input_log10")):
        return False
    if not _agent_optional_bool(input_window, "hard"):
        return False
    if not _agent_optional_nonnegative_number(input_window, "min_spacing_decades"):
        return False
    if "input_log10" not in input_window and "min_spacing_decades" in input_window:
        return False
    if "operating_points_log10" in input_window:
        points = input_window.get("operating_points_log10")
        if not isinstance(points, list):
            return False
        if any(_agent_finite_number(value) is None for value in points):
            return False
        if "input_log10" not in input_window:
            return False
    return True

def _agent_wrapped_operating_points_valid(input_window, program_length, constraints):
    if "operating_points_log10" not in input_window:
        return True
    points = input_window.get("operating_points_log10")
    bounds = input_window.get("input_log10")
    if not isinstance(points, list) or program_length is None or len(points) != program_length:
        return False
    if not _agent_finite_bounds(bounds):
        return False
    lo = _agent_finite_number(bounds[0])
    hi = _agent_finite_number(bounds[1])
    if lo is None or hi is None:
        return False
    numeric_points = []
    for raw_point in points:
        point = _agent_finite_number(raw_point)
        if point is None or point < lo or point > hi:
            return False
        numeric_points.append(point)
    spacing = 0.0
    if "min_spacing_decades" in input_window:
        spacing = _agent_nonnegative_finite_number(input_window.get("min_spacing_decades"))
        if spacing is None:
            return False
    transitions = constraints.get("transitions") if isinstance(constraints, dict) else {}
    if isinstance(transitions, dict) and "min_spacing_decades" in transitions:
        transition_spacing = _agent_nonnegative_finite_number(transitions.get("min_spacing_decades"))
        if transition_spacing is None:
            return False
        spacing = max(spacing, transition_spacing)
    for idx in range(len(numeric_points) - 1):
        if numeric_points[idx + 1] - numeric_points[idx] + 1e-9 < spacing:
            return False
    return True

def _agent_wrapped_source_valid(source):
    if not _agent_keys_allowed(source, ("kind", "node_id", "agent_message_id", "provenance")):
        return False
    if source.get("kind") not in _AGENT_SOURCE_KINDS:
        return False
    if not _agent_optional_string(source, "node_id"):
        return False
    if not _agent_optional_string(source, "agent_message_id"):
        return False
    return "provenance" not in source or _agent_plain_dict(source.get("provenance"))

def _agent_wrapped_legacy_target_valid(legacy):
    if not _agent_plain_dict(legacy):
        return False
    if not _agent_keys_allowed(legacy, ("target_kind", "target")):
        return False
    if legacy.get("target_kind") not in ("sign", "exact", "label"):
        return False
    if "target" not in legacy:
        return False
    if legacy.get("target_kind") != "exact":
        return True
    target = legacy.get("target")
    if isinstance(target, str) or not isinstance(target, (list, tuple)) or len(target) == 0:
        return False
    return all(
        not isinstance(value, bool) and
        isinstance(value, (int, float)) and
        math.isfinite(float(value))
        for value in target
    )

def _agent_wrapped_temporal_dynamics_valid(temporal):
    if not _agent_keys_allowed(temporal, ("stimulus", "trace", "peak_width_seconds", "hard")):
        return False
    if "stimulus" in temporal and not _agent_plain_dict(temporal.get("stimulus")):
        return False
    if "trace" in temporal and not isinstance(temporal.get("trace"), list):
        return False
    if "peak_width_seconds" in temporal:
        peak = temporal.get("peak_width_seconds")
        if not _agent_keys_allowed(peak, ("min", "max")):
            return False
        if "min" not in peak and "max" not in peak:
            return False
        if not _agent_optional_nonnegative_number(peak, "min"):
            return False
        if not _agent_optional_nonnegative_number(peak, "max"):
            return False
        min_peak = _agent_finite_number(peak.get("min")) if "min" in peak else None
        max_peak = _agent_finite_number(peak.get("max")) if "max" in peak else None
        if min_peak is not None and max_peak is not None and min_peak > max_peak:
            return False
    return _agent_optional_bool(temporal, "hard")

def _agent_wrapped_parameter_bounds_valid(bounds):
    if not _agent_keys_allowed(bounds, ("basis", "kd_log10", "total_log10", "by_class")):
        return False
    if "basis" in bounds and bounds.get("basis") != "log10_qK":
        return False
    if "kd_log10" not in bounds and "total_log10" not in bounds and "by_class" not in bounds:
        return False
    if "kd_log10" in bounds and not _agent_finite_bounds(bounds.get("kd_log10")):
        return False
    if "total_log10" in bounds and not _agent_finite_bounds(bounds.get("total_log10")):
        return False
    if "by_class" in bounds:
        by_class = bounds.get("by_class")
        if not _agent_keys_allowed(by_class, ("kd", "total")):
            return False
        if "kd" not in by_class and "total" not in by_class:
            return False
        if "kd" in by_class and not _agent_finite_bounds(by_class.get("kd")):
            return False
        if "total" in by_class and not _agent_finite_bounds(by_class.get("total")):
            return False
        if "kd_log10" in bounds and "kd" in by_class:
            return False
        if "total_log10" in bounds and "total" in by_class:
            return False
    return True

def _agent_wrapped_network_valid(network):
    if not _agent_keys_allowed(network, ("max_species", "max_reactions", "max_mu", "allow_near_minimal")):
        return False
    if not _agent_optional_positive_int(network, "max_species")[0]:
        return False
    if not _agent_optional_nonnegative_int(network, "max_reactions"):
        return False
    if not _agent_optional_positive_int(network, "max_mu")[0]:
        return False
    return _agent_optional_bool(network, "allow_near_minimal")

def _agent_wrapped_robustness_valid(robustness):
    if not _agent_keys_allowed(robustness, (
        "min_chebyshev_radius", "min_tunable_volume_lower_bound", "min_tunable_volume",
        "condition_number_max", "min_sampled_pass_fraction", "hard",
    )):
        return False
    if "min_tunable_volume_lower_bound" in robustness and "min_tunable_volume" in robustness:
        return False
    for key in ("min_chebyshev_radius", "min_tunable_volume_lower_bound", "min_tunable_volume", "condition_number_max"):
        if not _agent_optional_nonnegative_number(robustness, key):
            return False
    if not _agent_optional_fraction(robustness, "min_sampled_pass_fraction"):
        return False
    return _agent_optional_bool(robustness, "hard")

def _agent_wrapped_candidate_budget_valid(candidate_budget):
    if not _agent_keys_allowed(candidate_budget, (
        "mode", "max_extra_species", "max_extra_reactions", "max_extra_mu",
        "max_screened", "max_verified_recommendations", "max_recommended",
        "max_near_misses", "max_exact_placements",
    )):
        return False
    if "mode" in candidate_budget and candidate_budget.get("mode") not in ("near_minimal", "all_matches"):
        return False
    return all(_agent_optional_nonnegative_int(candidate_budget, key) for key in (
        "max_extra_species", "max_extra_reactions", "max_extra_mu", "max_screened",
        "max_verified_recommendations", "max_recommended", "max_near_misses", "max_exact_placements",
    ))

def _agent_wrapped_ranking_policy_valid(ranking_policy, constraints=None):
    if not _agent_keys_allowed(ranking_policy, ("verified_only", "prefer")):
        return False
    if "verified_only" in ranking_policy and ranking_policy.get("verified_only") is not True:
        return False
    if "prefer" in ranking_policy:
        prefer = ranking_policy.get("prefer")
        if not isinstance(prefer, list):
            return False
        if any(not isinstance(value, str) or value not in _AGENT_RANKING_PREFERENCES for value in prefer):
            return False
        constraints = constraints if isinstance(constraints, dict) else {}
        transitions = constraints.get("transitions") if isinstance(constraints.get("transitions"), dict) else {}
        for value in prefer:
            if value == "dynamic_range" and not isinstance(constraints.get("dynamic_range"), dict):
                return False
            if value == "transition_spacing" and "min_spacing_decades" not in transitions:
                return False
            if value in ("condition_number", "sampled_robustness"):
                return False
    return True

def _agent_wrapped_positive_budget(candidate_budget, key):
    if not isinstance(candidate_budget, dict) or key not in candidate_budget:
        return False
    value = _agent_integral_number(candidate_budget.get(key))
    return value is not None and value > 0

def _agent_wrapped_parameter_bounds_prerequisites_valid(spec):
    constraints = spec.get("constraints") if isinstance(spec.get("constraints"), dict) else {}
    if isinstance(constraints.get("parameter_bounds"), dict):
        return True

    candidate_budget = spec.get("candidate_budget") if isinstance(spec.get("candidate_budget"), dict) else {}
    if _agent_wrapped_positive_budget(candidate_budget, "max_exact_placements"):
        return False
    if _agent_wrapped_positive_budget(candidate_budget, "max_verified_recommendations"):
        return False

    ranking_policy = spec.get("ranking_policy") if isinstance(spec.get("ranking_policy"), dict) else {}
    if ranking_policy.get("verified_only") is True:
        return False
    prefer = ranking_policy.get("prefer")
    if isinstance(prefer, list) and any(
        value in ("chebyshev_radius", "tunable_volume", "dynamic_range", "transition_spacing")
        for value in prefer
    ):
        return False

    target = spec.get("target") if isinstance(spec.get("target"), dict) else {}
    if "output_feature" in target or "shape" in target:
        return False
    if "dynamic_range" in constraints or "transitions" in constraints:
        return False
    robustness = constraints.get("robustness") if isinstance(constraints.get("robustness"), dict) else {}
    return not any(
        key in robustness
        for key in ("min_chebyshev_radius", "min_tunable_volume_lower_bound", "min_tunable_volume")
    )

def _agent_wrapped_unsupported_hard_robustness_valid(spec):
    constraints = spec.get("constraints") if isinstance(spec.get("constraints"), dict) else {}
    robustness = constraints.get("robustness") if isinstance(constraints.get("robustness"), dict) else {}
    if robustness.get("hard") is False:
        return True
    return not any(
        key in robustness
        for key in ("condition_number_max", "min_sampled_pass_fraction")
    )

def _agent_wrapped_unsupported_hard_target_clauses_valid(spec):
    target = spec.get("target") if isinstance(spec.get("target"), dict) else {}
    for key in ("input_window", "temporal_dynamics"):
        if key not in target:
            continue
        clause = target.get(key)
        if not isinstance(clause, dict) or clause.get("hard") is not False:
            return False
    return True

def _agent_wrapped_audit_policy_valid(audit_policy):
    if not _agent_keys_allowed(audit_policy, ("unsupported", "path_format", "include_supported")):
        return False
    if "unsupported" in audit_policy and audit_policy.get("unsupported") != "block_if_hard":
        return False
    if "path_format" in audit_policy and audit_policy.get("path_format") != "json_pointer":
        return False
    return _agent_optional_bool(audit_policy, "include_supported")

def _agent_designability_constraints_from_behavior_payload(raw):
    constraints = {}
    network = {}
    if "network_constraints" in raw and not isinstance(raw.get("network_constraints"), dict):
        return None
    nc = raw.get("network_constraints") if isinstance(raw.get("network_constraints"), dict) else {}
    ok, max_reactions = _agent_optional_positive_int(nc, "max_reactions")
    if not ok:
        return None
    if max_reactions is not None:
        network["max_reactions"] = max_reactions
    ok, max_species = _agent_optional_positive_int(nc, "max_base_species")
    if not ok:
        return None
    if max_species is not None:
        network["max_species"] = max_species
    if network:
        constraints["network"] = network

    if "kd_profile" in nc and not isinstance(nc.get("kd_profile"), dict):
        return None
    kd_profile = nc.get("kd_profile") if isinstance(nc.get("kd_profile"), dict) else {}
    has_kd_min = "log10_kd_min" in kd_profile
    has_kd_max = "log10_kd_max" in kd_profile
    if "kd_profile" in nc and not has_kd_min and not has_kd_max:
        return None
    if has_kd_min or has_kd_max:
        if has_kd_min != has_kd_max:
            return None
        ok_min, kd_min = _agent_optional_finite(kd_profile, "log10_kd_min")
        ok_max, kd_max = _agent_optional_finite(kd_profile, "log10_kd_max")
        if not ok_min or not ok_max or kd_min > kd_max:
            return None
        constraints["parameter_bounds"] = {"kd_log10": [kd_min, kd_max]}

    if "shape_preferences" in raw and not isinstance(raw.get("shape_preferences"), dict):
        return None
    shape_preferences = raw.get("shape_preferences") if isinstance(raw.get("shape_preferences"), dict) else {}
    if "shape_preferences" in raw and not _agent_keys_allowed(shape_preferences, ("dynamic_range_log10",)):
        return None
    if "dynamic_range_log10" in shape_preferences and not isinstance(shape_preferences.get("dynamic_range_log10"), dict):
        return None
    dynamic_range = shape_preferences.get("dynamic_range_log10") if isinstance(shape_preferences.get("dynamic_range_log10"), dict) else {}
    if "min" in dynamic_range:
        if not _agent_keys_allowed(dynamic_range, ("min", "sample_points")):
            return None
        ok, dynamic_min = _agent_optional_finite(dynamic_range, "min")
        if not ok:
            return None
        sample_points = _agent_sample_points(dynamic_range.get("sample_points"))
        if sample_points is None:
            return None
        try:
            min_fold_change = 10 ** dynamic_min
        except OverflowError:
            return None
        if not math.isfinite(min_fold_change):
            return None
        constraints["dynamic_range"] = {
            "min_fold_change": min_fold_change,
            "sample_points": sample_points,
            "hard": True,
        }
    elif "dynamic_range_log10" in shape_preferences:
        return None
    return constraints

def _agent_wrapped_designability_spec_valid(spec):
    if not _agent_keys_allowed(spec, ("schema_version", "source", "target", "constraints", "candidate_budget", "ranking_policy", "audit_policy")):
        return False
    target = spec.get("target")
    if not _agent_wrapped_source_valid(spec.get("source")):
        return False
    if not _agent_keys_allowed(target, ("legacy_target", "behavior_spec", "input_window", "output_feature", "temporal_dynamics", "shape")):
        return False
    if "legacy_target" in target and "behavior_spec" in target:
        return False
    if "behavior_spec" in target and "input_window" in target:
        return False
    if not any(key in target for key in ("legacy_target", "behavior_spec", "output_feature", "temporal_dynamics", "shape")):
        return False
    if "legacy_target" in target and not _agent_wrapped_legacy_target_valid(target.get("legacy_target")):
        return False
    if "input_window" in target and not _agent_wrapped_input_window_valid(target.get("input_window")):
        return False
    if "temporal_dynamics" in target and not _agent_wrapped_temporal_dynamics_valid(target.get("temporal_dynamics")):
        return False
    has_behavior_spec = "behavior_spec" in target
    behavior = target.get("behavior_spec")
    program_length = None
    if has_behavior_spec:
        if not _agent_keys_allowed(behavior, ("input", "output", "program", "feature_space", "input_window")):
            return False
        if behavior.get("feature_space", "reaction_order") != "reaction_order":
            return False
        if not isinstance(behavior.get("input"), str) or not behavior.get("input").strip():
            return False
        if not isinstance(behavior.get("output"), str) or not behavior.get("output").strip():
            return False
        program_length = _agent_wrapped_behavior_program_length(behavior)
        if program_length is None:
            return False
    else:
        behavior = {}
    input_window = behavior.get("input_window") if "input_window" in behavior else {}
    if "input_window" in behavior:
        if not _agent_wrapped_input_window_valid(input_window):
            return False
    input_bounds = input_window.get("input_log10") if isinstance(input_window, dict) else None
    has_behavior_input_window = _agent_finite_bounds(input_bounds)
    operating_points = input_window.get("operating_points_log10") if isinstance(input_window, dict) else None
    constraints_for_window = spec.get("constraints") if isinstance(spec.get("constraints"), dict) else {}
    if isinstance(operating_points, list) and (
        program_length is None or len(operating_points) != program_length
    ):
        return False
    if operating_points is not None:
        if not has_behavior_input_window:
            return False
        if not isinstance(operating_points, list):
            return False
        if any(_agent_finite_number(value) is None for value in operating_points):
            return False
        if not _agent_wrapped_operating_points_valid(input_window, program_length, constraints_for_window):
            return False

    if "output_feature" in target:
        output_feature = target.get("output_feature")
        if not _agent_keys_allowed(output_feature, ("feature", "operator", "value", "sample_points", "tolerance_log10", "hard")):
            return False
        feature = output_feature.get("feature")
        if feature not in ("fold_change", "level", "threshold"):
            return False
        if output_feature.get("operator", "=") not in (">=", "<=", "="):
            return False
        value = _agent_finite_number(output_feature.get("value"))
        if value is None:
            return False
        if feature == "fold_change" and value <= 0:
            return False
        if _agent_sample_points(output_feature.get("sample_points")) is None:
            return False
        if _agent_nonnegative_finite_number(output_feature.get("tolerance_log10")) is None:
            return False
        if not _agent_optional_bool(output_feature, "hard"):
            return False
        if not has_behavior_input_window:
            return False

    if "shape" in target:
        shape = target.get("shape")
        if not _agent_keys_allowed(shape, ("class", "monotonicity", "sample_points", "tolerance_log10", "min_prominence_log10", "min_prominence_decades", "hard")):
            return False
        cls = shape.get("class")
        if cls not in ("monotonic", "bell_shaped"):
            return False
        if _agent_sample_points(shape.get("sample_points")) is None:
            return False
        if _agent_nonnegative_finite_number(shape.get("tolerance_log10")) is None:
            return False
        if cls == "monotonic" and shape.get("monotonicity") not in ("increasing", "decreasing", "any"):
            return False
        if cls == "monotonic" and ("min_prominence_log10" in shape or "min_prominence_decades" in shape):
            return False
        if cls == "bell_shaped":
            if "min_prominence_log10" in shape and "min_prominence_decades" in shape:
                return False
            prominence = (
                _agent_nonnegative_finite_number(shape.get("min_prominence_log10"))
                if "min_prominence_log10" in shape else
                _agent_nonnegative_finite_number(shape.get("min_prominence_decades"))
            )
            if prominence is None:
                return False
        if not _agent_optional_bool(shape, "hard"):
            return False
        if not has_behavior_input_window:
            return False

    if "constraints" in spec:
        constraints = spec.get("constraints")
        if not _agent_keys_allowed(constraints, ("network", "parameter_bounds", "robustness", "dynamic_range", "transitions")):
            return False
        if "network" in constraints and not _agent_wrapped_network_valid(constraints.get("network")):
            return False
        if "parameter_bounds" in constraints and not _agent_wrapped_parameter_bounds_valid(constraints.get("parameter_bounds")):
            return False
        if "robustness" in constraints and not _agent_wrapped_robustness_valid(constraints.get("robustness")):
            return False
        if "dynamic_range" in constraints:
            dynamic_range = constraints.get("dynamic_range")
            if not _agent_keys_allowed(dynamic_range, ("min_fold_change", "sample_points", "hard")):
                return False
            if _agent_nonnegative_finite_number(dynamic_range.get("min_fold_change")) is None:
                return False
            if _agent_sample_points(dynamic_range.get("sample_points")) is None:
                return False
            if not _agent_optional_bool(dynamic_range, "hard"):
                return False
            if not has_behavior_input_window:
                return False
        if "transitions" in constraints:
            transitions = constraints.get("transitions")
            if not _agent_keys_allowed(transitions, ("min_spacing_decades", "order", "hard")):
                return False
            if "min_spacing_decades" not in transitions and "order" not in transitions:
                return False
            if "order" in transitions:
                order = transitions.get("order")
                if not has_behavior_input_window:
                    return False
                if not isinstance(order, list) or program_length is None or len(order) != program_length:
                    return False
                seen = set()
                for value in order:
                    idx = _agent_integral_number(value)
                    if idx is None:
                        return False
                    if idx < 0 or idx >= program_length or idx in seen:
                        return False
                    seen.add(idx)
            if "min_spacing_decades" in transitions:
                if _agent_nonnegative_finite_number(transitions.get("min_spacing_decades")) is None:
                    return False
                if not has_behavior_input_window or program_length is None or program_length < 2:
                    return False
            if not _agent_optional_bool(transitions, "hard"):
                return False

    if "candidate_budget" in spec and not _agent_wrapped_candidate_budget_valid(spec.get("candidate_budget")):
        return False
    if "ranking_policy" in spec and not _agent_wrapped_ranking_policy_valid(spec.get("ranking_policy"), spec.get("constraints")):
        return False
    if not _agent_wrapped_parameter_bounds_prerequisites_valid(spec):
        return False
    if not _agent_wrapped_unsupported_hard_target_clauses_valid(spec):
        return False
    if not _agent_wrapped_unsupported_hard_robustness_valid(spec):
        return False
    if "audit_policy" in spec and not _agent_wrapped_audit_policy_valid(spec.get("audit_policy")):
        return False

    return True

def _agent_designability_spec_from_payload(raw, card=None):
    if not isinstance(raw, dict):
        return None
    if raw.get("schema_version") == DESIGNABILITY_SPEC_VERSION:
        if not _agent_wrapped_designability_spec_valid(raw):
            return None
        spec = copy.deepcopy(raw)
        source = spec.get("source") if isinstance(spec.get("source"), dict) else {}
        source = copy.deepcopy(source)
        source["kind"] = "agent_design"
        spec["source"] = source
        return spec

    behavior = raw.get("behavior_spec")
    if not isinstance(behavior, dict):
        return None
    if not _agent_keys_allowed(behavior, ("feature_space", "input", "output", "program", "input_window")):
        return None
    feature_space = behavior.get("feature_space", "reaction_order")
    if feature_space != "reaction_order":
        return None
    program = _agent_behavior_program(behavior)
    if program is None:
        return None
    input_symbol = _agent_behavior_symbol(behavior, "input", card, ("input_symbol", "input"))
    output_symbol = _agent_behavior_symbol(behavior, "output", card, ("observe_species", "output_symbol", "output"))
    if not input_symbol or not output_symbol:
        return None

    lowered_behavior = {
        "feature_space": "reaction_order",
        "input": input_symbol,
        "output": output_symbol,
        "program": program,
    }
    input_window = behavior.get("input_window")
    if "input_window" in behavior:
        if not _agent_wrapped_input_window_valid(input_window):
            return None
        lowered_behavior["input_window"] = copy.deepcopy(input_window)
    constraints = _agent_designability_constraints_from_behavior_payload(raw)
    if constraints is None:
        return None
    spec = {
        "schema_version": DESIGNABILITY_SPEC_VERSION,
        "source": {
            "kind": "agent_design",
            "provenance": {"agent_behavior_spec": copy.deepcopy(raw)},
        },
        "target": {"behavior_spec": lowered_behavior},
        "constraints": constraints,
        "candidate_budget": {
            "mode": "near_minimal",
            "max_extra_species": 1,
            "max_extra_reactions": 1,
            "max_extra_mu": 1,
            "max_recommended": 24,
            "max_verified_recommendations": 24,
            "max_screened": 24,
            "max_near_misses": 12,
            "max_exact_placements": 3,
        },
        "ranking_policy": {"verified_only": True},
        "audit_policy": {
            "unsupported": "block_if_hard",
            "path_format": "json_pointer",
            "include_supported": True,
        },
    }
    return spec if _agent_wrapped_designability_spec_valid(spec) else None

_AGENT_SPEC_PAYLOAD_KEYS = (
    "designability_spec",
    "designabilitySpec",
    "compiled_spec",
    "behavior_spec",
    "agent_compiled_spec",
)

def _agent_first_present_spec_payload(res):
    if not isinstance(res, dict):
        return False, None
    for key in _AGENT_SPEC_PAYLOAD_KEYS:
        if key in res:
            return True, res.get(key)
    return False, None

# ── atlas corpora (the PRIOR / seed source — cached once per process) ──
_CACHE = {}
def _rows_file(path):
    if path not in _CACHE:
        _CACHE[path] = [json.loads(l) for l in open(path)] if os.path.isfile(path) else []
    return _CACHE[path]
def _json_file(path):                          # whole-file JSON (e.g. the multimodal seed pack array)
    if path not in _CACHE:
        try: _CACHE[path] = json.load(open(path)) if os.path.isfile(path) else []
        except Exception: _CACHE[path] = []
    return _CACHE[path]
def _dose_rows():
    if "dose" not in _CACHE:
        rows = list(ds.iter_rows(DOSE_DS))
        if os.path.isdir(DOSE_DS_D4):          # union the richer d≤4 corpus when present
            rows += list(ds.iter_rows(DOSE_DS_D4))
        _CACHE["dose"] = rows
    return _CACHE["dose"]

# ── tools ────────────────────────────────────────────────────────────────────
def corpus_overview():
    """Prevalence of each behaviour in the atlas PRIOR — so the agent can judge feasibility
    (e.g. valley shapes are rarer than peaks) before proposing. NOT an answer; a map."""
    if "overview" in _CACHE:
        return _CACHE["overview"]
    dose = _dose_rows()
    classes = {}
    for cls in sorted(set(ds.CLASS_MAP.values())):
        vals = [(r.get("shape_fractions") or {}).get(cls) or 0.0 for r in dose]
        classes[cls] = {"n_robust_ge_0.5": sum(1 for v in vals if v >= 0.5), "max_support": round(max(vals), 3) if vals else 0.0}
    logic = _rows_file(LOGIC_LABELS)
    ov = {"note": "atlas PRIOR (precomputed survey); use it to pick seeds & gauge feasibility, NOT as the answer.",
          "dose_corpus": len(dose), "dose_classes": classes,
          "logic_total": len(logic),
          "logic_gate_counts": dict(collections.Counter(r.get("realized_gate") for r in logic).most_common()),
          "analog_total": len(_rows_file(ANALOG_LABELS)),
          "contextual_total": len(_rows_file(CONTEXTUAL_LABELS)),
          "contextual_reprogrammable": sum(1 for r in _rows_file(CONTEXTUAL_LABELS) if r.get("reprogrammable"))}
    _CACHE["overview"] = ov
    return ov

def retrieve_atlas_seed(behavior_class, max_reactions=99, min_robustness=0.4, top=3):
    """SEED networks (reactions+kd topology) the atlas suggests for a 1-input dose shape — a
    PRIOR to feed into `simulate`, NOT a verified answer. Tagged unverified-this-session."""
    cls = ds.CLASS_MAP.get(behavior_class)
    if not cls:
        return {"error": f"unknown behavior_class '{behavior_class}'", "valid_classes": sorted(ds.CLASS_MAP)}
    flat = {"behavior_class": behavior_class, "min_robustness": min_robustness, "max_reactions": max_reactions}
    pool = [r for r in _dose_rows() if (r.get("n_reactions") or 99) <= max_reactions]
    scored = [(ss, len(rs), r, rs) for r in pool for (ss, rs) in [ds.eval_row(r, flat, cls)]]
    scored.sort(key=lambda x: (-x[0], x[1]))
    seeds = []
    for (ss, _n, r, _rs) in scored[:top]:
        # carry the SAME axes the atlas used to assign this label, so `simulate` can reproduce it:
        seeds.append({"reactions": r.get("rules") or [], "kd": r.get("kd"),
                      "input_symbol": r.get("input_symbol"), "observe_species": r.get("output_symbol"),
                      "atlas_label": r.get("dominant_shape"), "atlas_shape_support": round(ss, 3),
                      "n_reactions": r.get("n_reactions"),
                      "note": "atlas prior — NOT re-verified this session; pass reactions+input_symbol+observe_species to simulate"})
    return {"behavior_class": behavior_class, "corpus": len(pool),
            "best_atlas_support": round(scored[0][0], 3) if scored else 0.0, "seeds": seeds}

def retrieve_logic_seed(gate, top=3):
    """SEED 2-input networks the atlas realised as a Boolean gate (PRIOR; live 2-input
    re-verification is not yet wired, so these are clearly unverified-this-session)."""
    rows = _rows_file(LOGIC_LABELS)
    hits = sorted([r for r in rows if r.get("realized_gate") == gate],
                  key=lambda r: (-(r.get("gate_support") or 0), -(r.get("median_margin_decades") or 0)))
    return {"gate": gate, "matches": len(hits), "total": len(rows),
            "best_gate_support": round(hits[0].get("gate_support") or 0, 3) if hits else 0.0,
            "note": "atlas PRIOR for a 2-input gate — pass each seed's reactions to simulate_2d to compute its real surface and verify the realized gate this session.",
            "seeds": [{"reactions": C.logic_card(r)[0].get("rules") or [], "realized_gate": r.get("realized_gate"),
                       "gate_support": r.get("gate_support"), "inputs": r.get("inputs"), "output": r.get("output")}
                      for r in hits[:top]]}

_ANALOG_KEYFN = {
    "two_input_bump": lambda r: r.get("bump_fraction") or 0,
    "interior_bump": lambda r: r.get("bump_fraction") or 0,
    "ratio_sensor": lambda r: abs(r.get("ratio_corr") or 0),
    "synergy": lambda r: r.get("coactivation_corr") or 0,
    "coactivation": lambda r: r.get("coactivation_corr") or 0,
}
def retrieve_analog_seed(target, top=3):
    """SEED 2-input networks for an analog SURFACE target (PRIOR). target='two_input_bump' ranks by
    interior-bump strength, 'ratio_sensor' by |ratio_corr|, 'synergy' by coactivation. Pass each
    seed's reactions+input1+input2+observe_species to simulate_2d to compute & verify the surface."""
    rows = _rows_file(ANALOG_LABELS)
    keyfn = _ANALOG_KEYFN.get(target, lambda r: r.get("dynamic_range_decades") or 0)
    ranked = sorted(rows, key=lambda r: -keyfn(r))
    seeds = [{"reactions": r.get("rules") or [],
              "input1": (r.get("inputs") or [None, None])[0], "input2": (r.get("inputs") or [None, None])[1],
              "observe_species": r.get("output"), "bump_fraction": r.get("bump_fraction"),
              "ratio_corr": r.get("ratio_corr"), "coactivation_corr": r.get("coactivation_corr"),
              "note": "atlas prior — pass reactions+input1+input2+observe_species to simulate_2d to verify"}
             for r in ranked[:top]]
    return {"target": target, "total": len(rows),
            "best_score": round(keyfn(ranked[0]), 3) if ranked else 0.0, "seeds": seeds}

def retrieve_multimodal_seed(top=6):
    """SEED 1-input networks the atlas found to produce a MULTIMODAL dose-response — the response
    order reverses sign ≥2 times (up-down-up / down-up-down). A PRIOR / starting point: pass each
    seed's reactions + input_symbol + observe_species to `simulate` to recompute and VERIFY it this
    session. Ranked by the phenotyper's multimodal fraction over the Kd prior Π (highest-confidence
    multimodal topologies first). Use this when the user asks for an oscillating / 来回穿梭 / 先增后减再增
    response — it is far more likely to yield a real multimodal than a hand-guessed topology."""
    pack = _json_file(MULTIMODAL_SEEDS)
    if not pack:
        return {"target": "multimodal", "total": 0, "seeds": [],
                "note": "no multimodal seed pack on disk — propose a topology and verify with simulate / ro_behavior instead"}
    seeds = [{"reactions": s.get("rules") or [], "input_symbol": s.get("input_symbol"),
              "observe_species": s.get("output_symbol"),
              "atlas_multimodal_fraction": s.get("complex_fraction"),
              "atlas_label": s.get("dominant_shape"), "n_reactions": s.get("n_reactions"),
              "note": "atlas prior (multimodal) — NOT re-verified this session; pass reactions+input_symbol+observe_species to simulate"}
             for s in pack[:top]]
    return {"target": "multimodal", "total": len(pack),
            "best_atlas_fraction": pack[0].get("complex_fraction") if pack else 0.0, "seeds": seeds}

def _interior_peak(xs, ys, grid):
    """Detect an interior peak (a 'bump'): is the global max strictly inside the grid and above the
    best edge value? Returns is_interior_peak + prominence (decades) + where the peak sits."""
    nx = len(grid); ny = len(grid[0]) if nx else 0
    pts = [(grid[i][j], i, j) for i in range(nx) for j in range(ny) if isinstance(grid[i][j], (int, float))]
    if nx < 3 or ny < 3 or not pts:
        return None
    gmax, gi, gj = max(pts, key=lambda t: t[0])
    border = [v for (v, i, j) in pts if i in (0, nx - 1) or j in (0, ny - 1)]
    bmax = max(border) if border else gmax
    is_int = 0 < gi < nx - 1 and 0 < gj < ny - 1
    return {"is_interior_peak": bool(is_int and gmax > bmax),
            "prominence_decades": round(gmax - bmax, 3),
            "peak_at_log10": [round(xs[gi], 2), round(ys[gj], 2)]}

def _active_window(xs, ys, margin=1.0):
    """Pick the informative x-interval from a recon scan: where the curve actually moves (slope
    magnitude above a fraction of the max), padded by `margin` decades. Falls back to the full
    scan when the curve is essentially flat. Used to focus the displayed dose-response window."""
    n = len(xs)
    if n < 3 or (max(ys) - min(ys)) < 0.05:
        return (xs[0], xs[-1]) if xs else (-6.0, 6.0)
    slopes = [abs((ys[i + 1] - ys[i]) / ((xs[i + 1] - xs[i]) or 1e-9)) for i in range(n - 1)]
    smax = max(slopes) or 1e-9
    active = [i for i, s in enumerate(slopes) if s >= 0.08 * smax]
    if not active:
        return xs[0], xs[-1]
    lo, hi = xs[active[0]] - margin, xs[active[-1] + 1] + margin
    if hi - lo < 2.0:                       # keep a readable minimum width
        c = 0.5 * (lo + hi); lo, hi = c - 1.0, c + 1.0
    return max(-12.0, lo), min(12.0, hi)

def simulate(reactions, kd=None, input_symbol=None, observe_species=None,
             param_min=-6.0, param_max=6.0, n_points=121, **_):
    # The sweep is over a standard LOG10 window; clamp to a sane range so a mis-supplied
    # value can never push the solve off the feasible map (the cause of an all-floor curve).
    param_min = max(-12.0, min(11.0, float(param_min)))
    param_max = min(12.0, max(param_min + 1.0, float(param_max)))
    n_points = int(max(21, min(401, n_points)))
    """THE compute tool. Builds the network on the live Julia engine and ACTUALLY SOLVES its
    1-input dose-response + phenotypes it. Returns the COMPUTED curve sample + the engine's
    motif/exact label + robustness volume. Use this to verify any candidate (atlas seed OR
    your own design, incl. networks not in the atlas). Returns {engine_offline:true} if the
    compute engine is down — in that case DO NOT fabricate; tell the user."""
    reactions = list(reactions or [])
    if not reactions:
        return {"error": "simulate needs a non-empty reactions list, e.g. ['A + B <-> AB']"}
    kd = [float(x) for x in kd] if kd else [1.0] * len(reactions)
    m = E.build_model(reactions=reactions, kd=kd)
    if m.get("engine_offline"):
        return {"engine_offline": True, "error": m.get("error")}
    if m.get("error"):
        return {"error": f"build_model rejected the network: {m.get('error')}"}
    sid, q_sym, species, prod = m.get("session_id"), m.get("q_sym") or [], m.get("x_sym") or [], m.get("product_species") or []
    inp = input_symbol or (q_sym[0] if q_sym else None)
    if inp not in q_sym:
        # tolerate a species name for the input axis ("A" -> its total "tA"), a common slip.
        for alt in (("t" + inp) if inp else None, (inp[1:] if inp and inp.startswith("t") else None)):
            if alt in q_sym:
                inp = alt; break
    if inp not in q_sym:
        return {"error": f"input_symbol '{input_symbol}' is not a swept input for this network",
                "valid_inputs": q_sym, "species": species, "hint": "the input axis is a total-concentration symbol like 'tA' (one of valid_inputs)"}
    obs = observe_species or (prod[-1] if prod else (species[-1] if species else None))
    if obs not in species:
        return {"error": f"observe_species '{obs}' is not a species", "valid_species": species}
    # CLOSED LOOP for the display window: a wide reconnaissance solve → find where the curve is
    # actually active (the regime-transition region) → a focused solve over that interval, so the
    # plotted curve shows the informative range with correct bounds, not flat tails. Both real solves.
    def _scan(lo, hi, n):
        return E.dose_response(sid, param_symbol=inp, output_exprs=[obs], param_min=lo, param_max=hi, n_points=n)
    recon = _scan(-8.0, 8.0, 81)
    if recon.get("engine_offline"):
        return {"engine_offline": True, "error": recon.get("error")}
    if recon.get("error"):
        return {"error": f"dose_response failed: {recon.get('error')}"}
    rxs = recon.get("param_values") or []
    rys = [(row[0] if isinstance(row, list) else row) for row in (recon.get("output_traj") or [])]
    lo, hi = _active_window(rxs, rys, margin=1.0)
    dr = _scan(lo, hi, int(n_points))
    if dr.get("engine_offline"):
        return {"engine_offline": True, "error": dr.get("error")}
    if dr.get("error"):
        dr = recon                          # focused scan failed → keep the recon curve
    xs = dr.get("param_values") or []
    ys = [(row[0] if isinstance(row, list) else row) for row in (dr.get("output_traj") or [])]
    series = [{"x": round(float(x), 4), "y": round(float(y), 4)} for x, y in zip(xs, ys)]
    # faithful verification: the SAME SISO phenotyper that labelled the atlas — shape_support over
    # the Kd prior Π (NOT behavior_families' ROP-volume view, which can disagree with the atlas).
    ph = E.phenotype_classify(sid, input_symbol=inp, output_expr=obs, K=8)
    shape = support = None; fracs = {}; sign_seq = None; n_sign_changes = None
    pheno_offline = bool(ph.get("engine_offline"))
    pheno_err = ph.get("error") if isinstance(ph, dict) else None
    if isinstance(ph, dict) and not pheno_offline and not pheno_err:
        shape = ph.get("dominant_shape")
        support = round(ph.get("shape_support") or 0, 3)
        fracs = {k: round(v, 3) for k, v in (ph.get("shape_fractions") or {}).items() if (v or 0) > 0}
        # the actual ±-pattern of the verified shape (e.g. [1,-1,1] = rise-fall-rise),
        # so the agent can cite RO sign oscillation and the UI can show n_sign_changes
        sign_seq = ph.get("sign_seq"); n_sign_changes = ph.get("n_sign_changes")
    # compact view for the LLM (downsampled curve so it can SEE+reason about the shape)
    step = max(1, len(series) // 16)
    sample = [[s["x"], s["y"]] for s in series[::step]]
    ymax_i = max(range(len(series)), key=lambda i: series[i]["y"]) if series else 0
    ymin_i = min(range(len(series)), key=lambda i: series[i]["y"]) if series else 0
    result = {"family": "dose_shape", "reactions": reactions, "kd": kd, "input_symbol": inp,
              "observe_species": obs, "n_points": len(series), "curve_sample_log10": sample,
              "y_min": round(min(ys), 4) if ys else None, "y_max": round(max(ys), 4) if ys else None,
              "x_at_ymax": round(xs[ymax_i], 3) if xs else None, "x_at_ymin": round(xs[ymin_i], 3) if xs else None,
              "phenotype_shape": shape, "shape_support": support, "shape_fractions": fracs,
              "sign_seq": sign_seq, "n_sign_changes": n_sign_changes,
              "phenotype_note": ("phenotyper offline" if pheno_offline else pheno_err),
              "evidence_tier": "engine-verified (this session)"}
    # full-resolution UI card (computed_series drives the real plotted curve in agent-view.js)
    card = {"family": "dose_shape", "verdict": shape or "computed", "shape_support": support,
            "n_reactions": len(reactions), "output_symbol": obs, "rules": reactions, "kd": kd,
            "dominant_shape": shape, "shape_fractions": fracs, "input_symbol": inp,
            "sign_seq": sign_seq, "n_sign_changes": n_sign_changes,
            "phenotyper_version": (ph or {}).get("phenotyper_version"),
            "metrics_median": {}, "computed_series": series}
    ev = _evidence(card); card["evidence"] = ev; card["evidence_tier"] = ev["label"]
    result["evidence_tier"] = ev["label"]; result["_card"] = card
    return result

# (A,B) corner order (00,01,10,11), high=1 — mirrors evaluators.jl / cards.py / agent-view.js.
_GATE_TABLES = {
    "AND": (0, 0, 0, 1), "OR": (0, 1, 1, 1), "NAND": (1, 1, 1, 0), "NOR": (1, 0, 0, 0),
    "XOR": (0, 1, 1, 0), "XNOR": (1, 0, 0, 1), "NIMPLY": (0, 0, 1, 0), "IMPLY": (1, 1, 0, 1),
    "NOT_A": (1, 1, 0, 0), "NOT_B": (1, 0, 1, 0), "A": (0, 0, 1, 1), "B": (0, 1, 0, 1),
    "CIMPLY": (1, 0, 1, 1), "BNIMPLY": (0, 1, 0, 0), "TRUE": (1, 1, 1, 1), "FALSE": (0, 0, 0, 0),
}
def _booleanize_corners(grid):
    """grid[i][j] over input1(rows A) × input2(cols B), log-space. Threshold at the surface midpoint;
    read the 4 corners (00,01,10,11) → realized gate + on/off margin (decades)."""
    flat = [v for row in grid for v in row if isinstance(v, (int, float))]
    if not flat:
        return None, None, None
    lo, hi = min(flat), max(flat); thr = 0.5 * (lo + hi)
    c = [grid[0][0], grid[0][-1], grid[-1][0], grid[-1][-1]]
    if any(not isinstance(v, (int, float)) for v in c):
        return None, None, None
    bits = tuple(1 if v >= thr else 0 for v in c)
    name = next((g for g, t in _GATE_TABLES.items() if t == bits), "custom" + "".join(map(str, bits)))
    return name, list(bits), round(min(abs(v - thr) for v in c), 3)

def _is_finite_scan_number(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(float(value)))

def _validated_2d_surface(xs, ys, output_grid, raw_validity_grid):
    """Return a finite-value-masked surface and strict Boolean validity grid.

    The backend validity marker remains authoritative, but a literal `true`
    cannot make a missing/non-finite output usable.  Malformed or absent
    validity therefore fails closed instead of silently restoring the old
    all-points-valid behavior.
    """
    shape_ok = (
        isinstance(xs, list) and bool(xs)
        and isinstance(ys, list) and bool(ys)
        and all(_is_finite_scan_number(value) for value in xs + ys)
        and isinstance(output_grid, list) and len(output_grid) == len(xs)
        and all(isinstance(row, list) and len(row) == len(ys) for row in output_grid)
        and isinstance(raw_validity_grid, list) and len(raw_validity_grid) == len(xs)
        and all(isinstance(row, list) and len(row) == len(ys)
                for row in raw_validity_grid)
    )
    if not shape_ok:
        return [], [], False

    validity_grid = []
    masked_grid = []
    for output_row, validity_row in zip(output_grid, raw_validity_grid):
        normalized_validity_row = []
        masked_row = []
        for value, valid in zip(output_row, validity_row):
            usable = valid is True and _is_finite_scan_number(value)
            normalized_validity_row.append(usable)
            masked_row.append(value if usable else None)
        validity_grid.append(normalized_validity_row)
        masked_grid.append(masked_row)
    return masked_grid, validity_grid, True

def _required_2d_corners_valid(validity_grid):
    if not validity_grid or not validity_grid[0]:
        return False
    return all((
        validity_grid[0][0], validity_grid[0][-1],
        validity_grid[-1][0], validity_grid[-1][-1],
    ))

def _valid_sha256(value):
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))

def _same_finite_number(left, right):
    if not (_is_finite_scan_number(left) and _is_finite_scan_number(right)):
        return False
    return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)

def _same_finite_identity_multiset(left, right):
    if not (isinstance(left, list) and isinstance(right, list)
            and len(left) == len(right)
            and all(_is_finite_scan_number(value) for value in left + right)):
        return False
    return sorted(float(value) for value in left) == sorted(
        float(value) for value in right
    )

def _same_finite_identity_sequence(left, right):
    return (
        isinstance(left, list)
        and isinstance(right, list)
        and len(left) == len(right)
        and all(float(actual) == float(expected)
                for actual, expected in zip(left, right))
    )

def _canonical_network_ir_identity(network):
    """Ask the Julia canonical owner to parse and hash one network payload."""
    validated = E._post(
        "/api/v1/ir/network/validate",
        {"network": copy.deepcopy(network)},
        30,
    )
    if not (isinstance(validated, dict) and validated.get("valid") is True
            and validated.get("ir_schema_version") == "bne-ir/v1.0.0"):
        return None
    network_ir = validated.get("network")
    network_ir_hash = validated.get("hash")
    if not (isinstance(network_ir, dict)
            and network_ir.get("ir_schema_version") == "bne-ir/v1.0.0"
            and isinstance(network_ir.get("reactions"), list)
            and bool(network_ir["reactions"])
            and _valid_sha256(network_ir_hash)):
        return None
    return {
        "network_ir": copy.deepcopy(network_ir),
        "network_ir_hash": network_ir_hash,
    }

def _canonical_simulate_2d_request_identity(reactions, kd):
    """Normalize and hash the caller's legacy network through the canonical owner."""
    return _canonical_network_ir_identity({
        "reactions": list(reactions),
        "kd": list(kd),
    })

def _same_unique_string_population(actual, expected):
    return (
        isinstance(actual, list)
        and all(isinstance(value, str) and value for value in actual)
        and len(actual) == len(set(actual))
        and len(actual) == len(expected)
        and set(actual) == set(expected)
    )

def _canonical_legacy_model_symbol_populations(network_ir):
    """Derive build-model symbol populations from canonicalized legacy IR."""
    if not isinstance(network_ir, dict):
        return None
    provenance = network_ir.get("provenance")
    species = network_ir.get("species")
    reactions = network_ir.get("reactions")
    if not (isinstance(provenance, dict)
            and provenance.get("source") == "legacy_reactions_kd"
            and isinstance(species, list) and bool(species)
            and isinstance(reactions, list) and bool(reactions)):
        return None
    names = []
    free_names = []
    bound_names = []
    for declaration in species:
        if not isinstance(declaration, dict):
            return None
        name = declaration.get("name")
        role = declaration.get("role")
        if not (isinstance(name, str) and name and role in ("free", "bound")):
            return None
        names.append(name)
        (free_names if role == "free" else bound_names).append(name)
    if len(names) != len(set(names)):
        return None
    return {
        "species": names,
        "free_species": free_names,
        "product_species": bound_names,
        "q_symbols": [f"t{name}" for name in free_names],
        "k_symbols": [f"Kd{index + 1}" for index in range(len(reactions))],
    }

def _canonical_simulate_2d_model_identity(model_description, requested_kd,
                                          canonical_request_identity):
    """Bind build_model output to an independently normalized caller request."""
    if not isinstance(model_description, dict):
        return False
    network_ir = model_description.get("network_ir")
    network_ir_hash = model_description.get("network_ir_hash")
    artifact = model_description.get("artifact")
    artifact_hashes = artifact.get("input_hashes") if isinstance(artifact, dict) else None
    model_kd = model_description.get("kd")
    q_symbols = model_description.get("q_sym")
    k_symbols = model_description.get("K_sym")
    x_symbols = model_description.get("x_sym")
    model_species = model_description.get("species")
    free_species = model_description.get("free_species")
    product_species = model_description.get("product_species")
    expected_network_ir = (canonical_request_identity.get("network_ir")
                           if isinstance(canonical_request_identity, dict) else None)
    expected_network_ir_hash = (canonical_request_identity.get("network_ir_hash")
                                if isinstance(canonical_request_identity, dict) else None)
    canonical_model_identity = (_canonical_network_ir_identity(network_ir)
                                if isinstance(network_ir, dict) else None)
    canonical_model_hash = (canonical_model_identity.get("network_ir_hash")
                            if isinstance(canonical_model_identity, dict) else None)
    expected_symbols = _canonical_legacy_model_symbol_populations(expected_network_ir)
    network_reactions = network_ir.get("reactions") if isinstance(network_ir, dict) else None
    network_reaction_kd = (
        [reaction.get("kd") for reaction in network_reactions]
        if (isinstance(network_reactions, list)
            and all(isinstance(reaction, dict) and "kd" in reaction
                    for reaction in network_reactions))
        else None
    )
    return (
        isinstance(model_description.get("session_id"), str)
        and bool(model_description["session_id"])
        and isinstance(network_ir, dict)
        and network_ir.get("ir_schema_version") == "bne-ir/v1.0.0"
        and isinstance(network_ir.get("reactions"), list)
        and bool(network_ir["reactions"])
        and _valid_sha256(network_ir_hash)
        and isinstance(expected_network_ir, dict)
        and _valid_sha256(expected_network_ir_hash)
        and _valid_sha256(canonical_model_hash)
        and canonical_model_hash == expected_network_ir_hash
        and network_ir_hash == canonical_model_hash
        and isinstance(artifact, dict)
        and artifact.get("artifact_schema_version") == "bne-result/v1.0.0"
        and artifact.get("kind") == "build_model"
        and isinstance(artifact_hashes, dict)
        and artifact_hashes.get("network_ir_hash") == network_ir_hash
        and isinstance(model_kd, list)
        and _same_finite_identity_multiset(model_kd, requested_kd)
        and _same_finite_identity_sequence(model_kd, network_reaction_kd)
        and isinstance(expected_symbols, dict)
        and _same_unique_string_population(
            model_species, expected_symbols["species"],
        )
        and _same_unique_string_population(
            x_symbols, expected_symbols["species"],
        )
        and _same_unique_string_population(
            free_species, expected_symbols["free_species"],
        )
        and _same_unique_string_population(
            product_species, expected_symbols["product_species"],
        )
        and _same_unique_string_population(
            q_symbols, expected_symbols["q_symbols"],
        )
        and isinstance(k_symbols, list)
        and k_symbols == expected_symbols["k_symbols"]
        and len(k_symbols) == len(model_kd)
        and len(set(q_symbols + k_symbols)) == len(q_symbols) + len(k_symbols)
    )

def _expected_simulate_2d_fixed_qk(model_description):
    q_symbols = model_description.get("q_sym")
    k_symbols = model_description.get("K_sym")
    model_kd = model_description.get("kd")
    if not (isinstance(q_symbols, list) and isinstance(k_symbols, list)
            and isinstance(model_kd, list) and len(k_symbols) == len(model_kd)
            and all(_is_finite_scan_number(value) and float(value) > 0
                    for value in model_kd)):
        return None
    return ([0.0] * len(q_symbols)) + [math.log10(float(value)) for value in model_kd]

def _matches_requested_scan_axis(values, requested_length, lower=-6.0, upper=6.0):
    if (not isinstance(values, list) or len(values) != requested_length
            or requested_length < 2):
        return False
    expected_step = (upper - lower) / (requested_length - 1)
    return all(
        _same_finite_number(value, lower + (index * expected_step))
        for index, value in enumerate(values)
    )

def _simulate_2d_response_identity_matches(scan, model_description, input1, input2,
                                           output, requested_n_grid):
    expected_fixed_qk = _expected_simulate_2d_fixed_qk(model_description)
    fixed_qk = scan.get("fixed_qK") if isinstance(scan, dict) else None
    return (
        isinstance(scan, dict)
        and _valid_sha256(scan.get("network_ir_hash"))
        and scan.get("network_ir_hash") == model_description.get("network_ir_hash")
        and scan.get("param1_symbol") == input1
        and scan.get("param2_symbol") == input2
        and scan.get("output_expr") == output
        and _matches_requested_scan_axis(scan.get("param1_values"), requested_n_grid)
        and _matches_requested_scan_axis(scan.get("param2_values"), requested_n_grid)
        and expected_fixed_qk is not None
        and isinstance(fixed_qk, list)
        and len(fixed_qk) == len(expected_fixed_qk)
        and all(_same_finite_number(actual, expected)
                for actual, expected in zip(fixed_qk, expected_fixed_qk))
    )

def _simulate_2d_identity(model_description, reactions, kd, input1, input2,
                          output, requested_n_grid, fixed_qK):
    """Build the complete, ordered identity of one effective 2D scan request."""
    model_kd = model_description.get("kd")
    if not isinstance(model_kd, list):
        model_kd = list(kd)
    network_ir = model_description.get("network_ir")
    network_ir_hash = model_description.get("network_ir_hash")
    model_identity = {
        "reactions": list(reactions),
        "kd": list(model_kd),
        "network_ir": copy.deepcopy(network_ir),
        "network_ir_hash": network_ir_hash,
        "model_content_fingerprint": _sha256_json({
            "reactions": list(reactions),
            "kd": list(model_kd),
            "network_ir": network_ir,
            "network_ir_hash": network_ir_hash,
        }),
    }

    fixed_context = None
    if isinstance(fixed_qK, list):
        qk_symbols = list(model_description.get("q_sym") or [])
        qk_symbols.extend(model_description.get("K_sym") or [])
        if len(qk_symbols) == len(fixed_qK):
            fixed_symbols = [symbol for symbol in qk_symbols if symbol not in (input1, input2)]
            fixed_values = [fixed_qK[index] for index, symbol in enumerate(qk_symbols)
                            if symbol not in (input1, input2)]
            fixed_context = {
                "basis": "log10_qK",
                "symbols": fixed_symbols,
                "values": fixed_values,
                "by_symbol": dict(zip(fixed_symbols, fixed_values)),
                "full_qK_symbols": qk_symbols,
                "full_fixed_qK": list(fixed_qK),
            }
        else:
            # Preserve the effective backend context even if an older backend
            # did not return enough symbol metadata to label every coordinate.
            fixed_context = {
                "basis": "log10_qK",
                "full_fixed_qK": list(fixed_qK),
            }

    request_identity = {
        "schema_version": "bne-simulate-2d-request-identity/v1.0.0",
        "endpoint": "/api/v1/parameter_scan_2d",
        "ordered_inputs": [input1, input2],
        "output": output,
        "model": model_identity,
        "scan": {
            "param1_min": -6.0,
            "param1_max": 6.0,
            "param2_min": -6.0,
            "param2_max": 6.0,
            "n_grid": int(requested_n_grid),
        },
    }
    if fixed_context is not None:
        request_identity["fixed_context"] = fixed_context
    request_fingerprint = _sha256_json(request_identity)
    card_identity = {
        "schema_version": "bne-simulate-2d-card-identity/v1.0.0",
        "ordered_inputs": [input1, input2],
        "output": output,
        "model": model_identity,
        "request_fingerprint": request_fingerprint,
    }
    if fixed_context is not None:
        card_identity["fixed_context"] = fixed_context
    return request_identity, request_fingerprint, card_identity

def _downsample_grid(xs, ys, grid, n=40):
    si = max(1, len(xs) // n); sj = max(1, len(ys) // n)
    gz = [[row[j] for j in range(0, len(row), sj)] for row in grid[::si]]
    return xs[::si], ys[::sj], gz

def simulate_2d(reactions, kd=None, input1=None, input2=None, observe_species=None, n_grid=48, **_):
    """THE 2-input compute tool. Builds the network and ACTUALLY SOLVES its 2-input response SURFACE
    over input1×input2 (a real heatmap), then reads the realized Boolean gate from the four corners.
    Use to design/verify logic gates and analog surfaces (incl. networks not in the atlas). A gate,
    feature, margin, and verified card are returned only for a complete finite-validity grid; a
    partial grid is returned as a gap-preserving diagnostic, or {engine_offline} if unavailable."""
    reactions = list(reactions or [])
    if not reactions:
        return {"error": "simulate_2d needs a non-empty reactions list"}
    kd = [float(x) for x in kd] if kd else [1.0] * len(reactions)
    m = E.build_model(reactions=reactions, kd=kd)
    if m.get("engine_offline"):
        return {"engine_offline": True, "error": m.get("error")}
    if m.get("error"):
        return {"error": f"build_model rejected the network: {m.get('error')}"}
    sid, q, sp, prod = m.get("session_id"), m.get("q_sym") or [], m.get("x_sym") or [], m.get("product_species") or []
    def _res(x):
        if x in q: return x
        if x and ("t" + x) in q: return "t" + x
        return None
    i1 = _res(input1) or (q[0] if q else None)
    i2 = _res(input2) or next((s for s in q if s != i1), None)
    if i1 not in q or i2 not in q or i1 == i2:
        return {"error": "need two distinct input totals", "valid_inputs": q, "species": sp}
    obs = observe_species or (prod[-1] if prod else (sp[-1] if sp else None))
    if obs not in sp:
        return {"error": f"observe_species '{obs}' is not a species", "valid_species": sp}
    canonical_request_identity = _canonical_simulate_2d_request_identity(reactions, kd)
    canonical_model_identity = _canonical_simulate_2d_model_identity(
        m, kd, canonical_request_identity,
    )
    canonical_request_hash = (canonical_request_identity.get("network_ir_hash")
                              if isinstance(canonical_request_identity, dict) else None)
    s = E.scan_2d(
        sid,
        network_ir_hash=canonical_request_hash,
        param1_symbol=i1,
        param2_symbol=i2,
        output_expr=obs,
        n_grid=int(n_grid),
    )
    if s.get("engine_offline"):
        return {"engine_offline": True, "error": s.get("error")}
    if s.get("error"):
        return {"error": f"scan_2d failed: {s.get('error')}"}
    xs, ys = s.get("param1_values") or [], s.get("param2_values") or []
    raw_grid = s.get("output_grid")
    grid, validity_grid, shape_ok = _validated_2d_surface(
        xs, ys, raw_grid, s.get("validity_grid"),
    )
    response_grid_matches_request = (
        shape_ok and len(xs) == int(n_grid) and len(ys) == int(n_grid)
    )
    response_identity_matches_request = _simulate_2d_response_identity_matches(
        s, m, i1, i2, obs, int(n_grid),
    )
    corners_valid = shape_ok and _required_2d_corners_valid(validity_grid)
    all_points_valid = shape_ok and all(
        valid is True for row in validity_grid for valid in row
    )
    complete = (s.get("partial") is False and all_points_valid
                and canonical_model_identity and response_grid_matches_request
                and response_identity_matches_request)

    request_identity, request_fingerprint, card_identity = _simulate_2d_identity(
        m, reactions, kd, i1, i2, obs, int(n_grid), s.get("fixed_qK"),
    )
    gate = bits = margin = peak = None
    if complete and corners_valid:
        gate, bits, margin = _booleanize_corners(grid)
        peak = _interior_peak(xs, ys, grid)      # only a complete surface can certify a feature

    if shape_ok:
        gx, gy, gz = _downsample_grid(xs, ys, grid, 40)
        _, _, gv = _downsample_grid(xs, ys, validity_grid, 40)
    else:
        gx, gy, gz, gv = [], [], [], []
    flat = [v for r in grid for v in r if _is_finite_scan_number(v)]
    invalid_count = (sum(valid is not True for row in validity_grid for valid in row)
                     if shape_ok else None)
    effective_partial = not complete
    incomplete_reasons = []
    if effective_partial:
        if not shape_ok:
            incomplete_reasons.append("missing_or_malformed_validity_grid")
        if s.get("partial") is not False:
            incomplete_reasons.append("backend_marked_partial")
        if shape_ok and not all_points_valid:
            incomplete_reasons.append("invalid_or_nonfinite_samples")
        if not corners_valid:
            incomplete_reasons.append("invalid_required_corner")
        if not canonical_model_identity:
            incomplete_reasons.append("missing_or_invalid_canonical_model_identity")
        if not response_grid_matches_request:
            incomplete_reasons.append("response_grid_does_not_match_request")
        if not response_identity_matches_request:
            incomplete_reasons.append("response_identity_does_not_match_request")
    result = {"family": "logic", "reactions": reactions, "kd": kd, "input1": i1, "input2": i2,
              "observe_species": obs, "realized_gate": gate, "gate_corners_00_01_10_11": bits,
              "margin_decades": margin, "interior_peak": peak,
              "verification_status": ("diagnostic_partial" if effective_partial else "complete"),
              "evidence_grade": ("current-computation-partial-diagnostic"
                                   if effective_partial else "current-computation-complete"),
              "evidence_tier": ("Diagnostic · incomplete 2-input scan"
                                if effective_partial else None),
              "evidence_warning": (("The computed surface contains invalid or incomplete evidence. "
                                    "It cannot certify a realized gate, margin, interior feature, "
                                    "or recommendation.") if effective_partial else None),
              "incomplete_reasons": incomplete_reasons,
              "partial": effective_partial, "backend_partial": s.get("partial"),
              "invalid_point_count": invalid_count, "required_corners_valid": corners_valid,
              "surface_min": round(min(flat), 3) if flat else None,
              "surface_max": round(max(flat), 3) if flat else None, "n_grid": len(xs),
              "validity_grid": validity_grid,
              "request_identity": request_identity,
              "request_fingerprint": request_fingerprint,
              "card_identity": card_identity}
    if effective_partial:
        result.update({
            "surface": {"x": gx, "y": gy, "z": gz, "validity_grid": gv,
                        "input1": i1, "input2": i2, "observe": obs},
        })
        # Partial surfaces remain available to the tool caller as an explicitly
        # labelled diagnostic, but they never enter the Agent's verified-card path.
        return result

    card = {"family": "logic", "realized_gate": gate or "surface", "inputs": [i1, i2],
            "output": obs, "margin_decades": margin, "rules": reactions, "kd": kd,
            "interior_peak": peak,
            "network_ir": copy.deepcopy(m.get("network_ir")),
            "network_ir_hash": m.get("network_ir_hash"),
            "model_identity": card_identity["model"],
            "request_identity": request_identity,
            "request_fingerprint": request_fingerprint,
            "card_identity": card_identity,
            "partial": False, "validity_grid": validity_grid,
            "verification_status": "complete",
            "evidence_grade": "current-computation-complete",
            "surface": {"x": gx, "y": gy, "z": gz, "validity_grid": gv,
                        "input1": i1, "input2": i2, "observe": obs}}
    if "fixed_context" in card_identity:
        card["fixed_context"] = card_identity["fixed_context"]
    ev = _evidence(card); card["evidence"] = ev; card["evidence_tier"] = ev["label"]
    result["evidence_tier"] = ev["label"]; result["_card"] = card
    return result

def _ro_sign_seq(profile):
    """Collapse an analytic RO profile into a compressed sign sequence (+1/-1; flats and singular
    NaN/±Inf transitions dropped; repeats collapsed) — comparable to the phenotyper's sign_seq.
    Accepts the per-regime sign/RO list (motif_profile ['+','0','-'] or exact_profile [1.0,'NaN',-1.0])
    or the arrow-joined exact_label string ('1 → 0 → -1'). Returns [] for a multi-input regime token."""
    if profile is None:
        return []
    if isinstance(profile, str):
        toks = profile.replace("->", "→").split("→")        # exact_label uses the unicode arrow
    elif isinstance(profile, (list, tuple)):
        toks = list(profile)
    else:
        return []
    out = []
    for tok in toks:
        t = str(tok).strip().strip("[]").strip()
        if "," in t:                       # multi-input regime token → not a scalar dose word
            return []
        if t in ("NaN", "nan", "missing", "", "?"):
            continue                       # singular transition — no defined sign
        if t == "+": s = 1
        elif t == "-": s = -1
        elif t == "0": s = 0
        elif t in ("+Inf", "Inf", "inf"): s = 1
        elif t == "-Inf": s = -1
        else:
            try: v = float(t)
            except ValueError: continue
            s = 1 if v > 1e-9 else (-1 if v < -1e-9 else 0)
        if s == 0:
            continue
        if not out or out[-1] != s:
            out.append(s)
    return out

def ro_behavior(reactions, kd=None, input_symbol=None, observe_species=None, **_):
    """ANALYTIC RO (reaction-order) behavior of a 1-input network — the parameter-free SISO
    enumeration of which dose-response sign patterns this TOPOLOGY can produce over ALL kd. Use it
    BEFORE hunting kd to check whether a target is even structurally possible (esp. multimodal: does
    any feasible family reverse sign ≥2 times?), and to cross-check a numeric `simulate` verdict.
    Complements `simulate` (numeric, one kd) with structure. Returns the feasible exact families with
    their RO sign sequences + n_sign_changes + robustness volume, or {engine_offline}."""
    reactions = list(reactions or [])
    if not reactions:
        return {"error": "ro_behavior needs a non-empty reactions list"}
    kd = [float(x) for x in kd] if kd else [1.0] * len(reactions)
    m = E.build_model(reactions=reactions, kd=kd)
    if m.get("engine_offline"):
        return {"engine_offline": True, "error": m.get("error")}
    if m.get("error"):
        return {"error": f"build_model rejected the network: {m.get('error')}"}
    sid, q, sp, prod = m.get("session_id"), m.get("q_sym") or [], m.get("x_sym") or [], m.get("product_species") or []
    inp = input_symbol if input_symbol in q else \
          (("t" + input_symbol) if input_symbol and ("t" + input_symbol) in q else (q[0] if q else None))
    if inp not in q:
        return {"error": f"input_symbol '{input_symbol}' is not a swept input", "valid_inputs": q}
    obs = observe_species or (prod[-1] if prod else (sp[-1] if sp else None))
    if obs not in sp:
        return {"error": f"observe_species '{obs}' is not a species", "valid_species": sp}
    bf = E.phenotype(sid, change_qK=inp, observe_x=obs, path_scope="feasible", compute_volume=True)
    if bf.get("engine_offline"):
        return {"engine_offline": True, "error": bf.get("error")}
    if bf.get("error"):
        return {"error": f"behavior_families failed: {bf.get('error')}"}
    fams = []
    for fam in (bf.get("exact_families") or []):
        seq = _ro_sign_seq(fam.get("motif_profile") or fam.get("exact_label"))
        vol = fam.get("total_volume") or {}
        fams.append({"family_idx": fam.get("family_idx"), "ro_profile": fam.get("exact_label"),
                     "motif": fam.get("motif_label"),
                     "sign_seq": seq, "n_sign_changes": max(0, len(seq) - 1),
                     "n_paths": fam.get("n_paths"),
                     "volume_mean": (round(vol["mean"], 4) if isinstance(vol, dict) and vol.get("mean") is not None else None)})
    fams.sort(key=lambda f: (-f["n_sign_changes"], -(f["volume_mean"] or 0)))
    max_nsc = max((f["n_sign_changes"] for f in fams), default=0)
    return {"family": "ro_behavior", "input_symbol": inp, "observe_species": obs,
            "feasible_paths": bf.get("feasible_paths"), "total_paths": bf.get("total_paths"),
            "n_exact_families": len(fams), "max_sign_changes": max_nsc,
            "n_multimodal_families": sum(1 for f in fams if f["n_sign_changes"] >= 2),
            "multimodal_feasible": max_nsc >= 2, "families": fams[:12],
            "note": ("analytic parameter-free SISO enumeration: which RO sign sequences this TOPOLOGY "
                     "admits over all kd. max_sign_changes>=2 ⇒ a multimodal dose-response is "
                     "STRUCTURALLY POSSIBLE here (then find a kd with simulate). max_sign_changes<2 ⇒ "
                     "multimodal is impossible for this topology, change the network. This is the "
                     "ROP-volume view; the numeric phenotype at a specific kd is the per-design arbiter.")}

_READER_PROTOS = ["bump", "broad_bump", "right_peak", "left_peak", "valley",
                  "switch_on", "switch_off", "late_dip", "double_bump", "shoulder"]

def reader_panel(prototype="bump", intent="typical", behavior_class=None, nl=None, k=6, max_reactions=None, **_):
    """Function-Space Reader: an INTENT-ROUTED, evidence-backed candidate PANEL from the frozen
    industrial atlas_root (robust→label+support primary; typical/refinement→label recall+function
    rerank; existential/discovery→function primary). `nl` (the user's behaviour description) is
    compiled to target+intent; else `prototype`+`intent`. Returns route + coverage_status. A
    PRIOR/seed list — VERIFY a pick with `simulate` before presenting, never fabricate."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "reader"))
    try:
        import reader_service as RS
    except Exception as e:
        return {"error": f"reader service unavailable: {e}"}
    res = RS.panel(nl=nl, prototype=None if nl else prototype, intent=None if nl else intent,
                   behavior_label=behavior_class, k=k, max_reactions=max_reactions)
    if isinstance(res, dict) and res.get("error"):
        return res
    cands = [{"record_id": c["record_id"], "reactions": c["reactions"],
              "input_symbol": (c.get("io") or {}).get("input_symbol"),
              "observe_species": (c.get("io") or {}).get("output_symbol"),
              "dominant_shape": c["dominant_shape"], "match_score": c["match_score"],
              "evidence_tier": c["evidence"]["evidence_tier"], "shape_support": c["evidence"]["shape_support"],
              "rop_volume": c["evidence"]["volume_mean"], "robust_path_count": c["evidence"]["robust_path_count"]}
             for c in res.get("candidates", [])]
    out = {"family": "reader_panel", "intent": res.get("query", {}).get("intent_type", intent),
           "route": res.get("route"), "coverage_status": res.get("coverage_status"),
           "pipeline": res.get("pipeline"), "panel_diversity": res.get("panel_diversity"), "candidates": cands,
           "note": "intent-routed function-space atlas panel (PRIOR ranked by curve-match + ROP evidence; "
                   "see coverage_status). VERIFY each pick with `simulate` (its reactions+input_symbol+"
                   "observe_species) before presenting; never fabricate a verified match, and if "
                   "coverage_status is rare-or-absent, say so honestly."}
    if res.get("compiled_spec"):
        out["compiled_spec"] = res["compiled_spec"]
    return out


_ROP_SHAPE_EDIT_KINDS = {
    "broaden", "separate", "widen_center", "translate_group", "linear_witness",
}
_ROP_SHAPE_MATRIX_KEYS = {"matrix", "aeq", "beq", "aineq", "bineq"}


def _rop_shape_reference(raw):
    allowed = {
        "reference_hash", "artifact_ref", "network_ir_hash", "operating_points_log10",
        "kd", "totals", "path_identity", "cell_id",
    }
    if not _agent_keys_allowed(raw, allowed):
        return None, "reference contains unsupported fields"
    for key in ("reference_hash", "network_ir_hash"):
        value = raw.get(key)
        if (not isinstance(value, str) or len(value) != 64 or
                any(char not in "0123456789abcdef" for char in value)):
            return None, f"reference.{key} must contain exactly 64 lowercase hexadecimal characters"
    artifact_ref = raw.get("artifact_ref")
    if artifact_ref is not None and (not isinstance(artifact_ref, str) or not artifact_ref.strip()):
        return None, "reference.artifact_ref must be a non-empty string"
    for key in ("path_identity", "cell_id"):
        value = raw.get(key)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            return None, f"reference.{key} must be a non-empty string"
    points = raw.get("operating_points_log10")
    if (not isinstance(points, list) or len(points) < 2 or
            any(_agent_finite_number(value) is None for value in points)):
        return None, "reference.operating_points_log10 must contain at least two finite numbers"
    kd = raw.get("kd")
    if (not isinstance(kd, list) or not kd or
            any((_agent_finite_number(value) is None or float(value) <= 0.0) for value in kd)):
        return None, "reference.kd must be a non-empty array of finite positive numbers"
    totals = raw.get("totals")
    if (not isinstance(totals, dict) or not totals or
            any((not isinstance(key, str) or not key or
                 _agent_finite_number(value) is None or float(value) <= 0.0)
                for key, value in totals.items())):
        return None, "reference.totals must contain at least one finite positive symbol value"
    return copy.deepcopy(raw), None


def _rop_shape_network(raw, reference_kd):
    if not isinstance(raw, dict):
        return None, None, "network must be an object"
    allowed = {
        "network_ir", "expected_network_ir_hash", "rules", "reactions",
        "input_symbols", "output_symbols",
    }
    if not set(raw).issubset(allowed):
        return None, None, "network contains unsupported fields"
    expected_hash = raw.get("expected_network_ir_hash")
    if expected_hash is not None and (
            not isinstance(expected_hash, str) or len(expected_hash) != 64 or
            any(char not in "0123456789abcdef" for char in expected_hash)):
        return None, None, "network.expected_network_ir_hash must contain exactly 64 lowercase hexadecimal characters"

    network_ir = raw.get("network_ir")
    if network_ir is not None:
        if not isinstance(network_ir, dict):
            return None, None, "network.network_ir must be an object"
        if expected_hash is None:
            return None, None, "a full NetworkIR requires expected_network_ir_hash"
        legacy_fields = {"rules", "reactions", "input_symbols", "output_symbols"}
        if any(key in raw for key in legacy_fields):
            return None, None, "network_ir cannot be combined with legacy network fields"
        return copy.deepcopy(network_ir), expected_hash.strip(), None

    has_rules = "rules" in raw
    has_reactions = "reactions" in raw
    if has_rules == has_reactions:
        return None, None, "legacy network requires exactly one of rules or reactions"
    reactions = raw.get("rules") if has_rules else raw.get("reactions")
    if (not isinstance(reactions, list) or not reactions or
            any(not isinstance(rule, str) or not rule.strip() for rule in reactions)):
        return None, None, "legacy network reactions must be a non-empty string array"
    if len(reference_kd) != len(reactions):
        return None, None, "reference.kd must contain one value per legacy network reaction"
    input_symbols = raw.get("input_symbols")
    output_symbols = raw.get("output_symbols")
    for name, values in (("input_symbols", input_symbols), ("output_symbols", output_symbols)):
        if (not isinstance(values, list) or not values or
                any(not isinstance(value, str) or not value.strip() for value in values) or
                len(set(values)) != len(values)):
            return None, None, f"legacy network {name} must be a non-empty unique string array"
    legacy = {
        "reactions": copy.deepcopy(reactions),
        "kd": copy.deepcopy(reference_kd),
        "input_symbols": copy.deepcopy(input_symbols),
        "output_symbols": copy.deepcopy(output_symbols),
    }
    if expected_hash is not None:
        return None, None, "expected_network_ir_hash must be null/omitted for a legacy network"
    return legacy, None, None


def _rop_shape_work_budget(raw):
    required = {"max_paths", "max_cells", "max_replays", "require_exhaustive"}
    if not _agent_keys_allowed(raw, required) or set(raw) != required:
        return None, "work_budget requires max_paths, max_cells, max_replays, and require_exhaustive"
    limits = {"max_paths": 2000, "max_cells": 10000, "max_replays": 16}
    for key, maximum in limits.items():
        value = _agent_positive_int(raw.get(key))
        if value is None or value > maximum:
            return None, f"work_budget.{key} must be an integer within [1, {maximum}]"
    if raw.get("require_exhaustive") is not True:
        return None, "work_budget.require_exhaustive must be literal true for verified optimization"
    return copy.deepcopy(raw), None


def _rop_shape_replay_policy(raw):
    required = {"input_window_log10", "sample_points", "require_complete", "store_curve", "metrics"}
    if not _agent_keys_allowed(raw, required) or set(raw) != required:
        return None, "replay requires input_window_log10, sample_points, require_complete, store_curve, and metrics"
    bounds = raw.get("input_window_log10")
    if (not isinstance(bounds, list) or len(bounds) != 2 or
            any(_agent_finite_number(value) is None for value in bounds) or
            float(bounds[0]) >= float(bounds[1]) or
            any(abs(float(value)) > 20.0 for value in bounds)):
        return None, "replay.input_window_log10 must be two increasing numbers within [-20, 20]"
    sample_points = _agent_integral_number(raw.get("sample_points"))
    if sample_points is None or not 11 <= sample_points <= 1000:
        return None, "replay.sample_points must be an integer within [11, 1000]"
    if raw.get("require_complete") is not True or raw.get("store_curve") is not True:
        return None, "verified optimization requires replay.require_complete and replay.store_curve literal true"
    metrics = raw.get("metrics")
    if (not isinstance(metrics, list) or len(metrics) != 1 or
            not _agent_keys_allowed(metrics[0], {"kind", "min_prominence_log10"}) or
            metrics[0].get("kind") != "two_peak" or
            _agent_nonnegative_finite_number(metrics[0].get("min_prominence_log10")) is None):
        return None, "replay.metrics must contain exactly one valid two_peak metric"
    return copy.deepcopy(raw), None


def _rop_shape_step(raw):
    value = _agent_integral_number(raw)
    return value if value is not None and value >= 0 else None


def _rop_shape_steps(raw, count=None):
    if not isinstance(raw, list) or (count is not None and len(raw) != count) or not raw:
        return None
    values = [_rop_shape_step(value) for value in raw]
    if any(value is None for value in values) or len(set(values)) != len(values):
        return None
    return values


def _rop_shape_linear_terms(raw):
    if not isinstance(raw, list) or not raw:
        return False
    for term in raw:
        if not _agent_keys_allowed(term, {"step", "coefficient"}) or set(term) != {"step", "coefficient"}:
            return False
        coefficient = _agent_finite_number(term.get("coefficient"))
        if _rop_shape_step(term.get("step")) is None or coefficient is None or coefficient == 0.0:
            return False
    return True


def _rop_shape_edit_intent(raw):
    if not isinstance(raw, dict) or raw.get("kind") not in _ROP_SHAPE_EDIT_KINDS:
        return None, "edit_intent.kind must be broaden, separate, widen_center, translate_group, or linear_witness"
    if any(str(key).lower() in _ROP_SHAPE_MATRIX_KEYS for key in raw):
        return None, "edit_intent must be typed intent/linear terms, not precompiled matrices"
    if not isinstance(raw.get("id"), str) or not raw["id"].strip():
        return None, "edit_intent.id must be a non-empty string"

    kind = raw["kind"]
    if kind == "broaden":
        required = {"id", "kind", "left_span_steps", "right_span_steps", "shared_magnitude"}
        valid = (
            set(raw) == required and
            _rop_shape_steps(raw.get("left_span_steps"), 2) is not None and
            _rop_shape_steps(raw.get("right_span_steps"), 2) is not None and
            raw.get("shared_magnitude") is True
        )
    elif kind == "separate":
        required = {"id", "kind", "steps", "preserve_midpoint_tolerance_log10"}
        valid = (
            set(raw) == required and
            _rop_shape_steps(raw.get("steps"), 2) is not None and
            _agent_nonnegative_finite_number(raw.get("preserve_midpoint_tolerance_log10")) is not None
        )
    elif kind == "widen_center":
        required = {"id", "kind", "steps", "anchor_step", "anchor_tolerance_log10"}
        valid = (
            set(raw) == required and
            _rop_shape_steps(raw.get("steps"), 2) is not None and
            _rop_shape_step(raw.get("anchor_step")) is not None and
            _agent_nonnegative_finite_number(raw.get("anchor_tolerance_log10")) is not None
        )
    elif kind == "translate_group":
        required = {
            "id", "kind", "group_steps", "preserve_steps",
            "preserve_tolerance_log10", "sense", "shared_shift",
        }
        valid = (
            set(raw) == required and
            _rop_shape_steps(raw.get("group_steps")) is not None and
            _rop_shape_steps(raw.get("preserve_steps")) is not None and
            _agent_nonnegative_finite_number(raw.get("preserve_tolerance_log10")) is not None and
            raw.get("sense") in ("positive", "negative") and
            raw.get("shared_shift") is True
        )
    else:
        required = {"id", "kind", "constraints", "objective"}
        constraints = raw.get("constraints")
        objective = raw.get("objective")
        valid_constraints = isinstance(constraints, list)
        if valid_constraints:
            for constraint in constraints:
                constraint_keys = {"id", "terms", "operator", "rhs_log10", "hard"}
                if (not _agent_keys_allowed(constraint, constraint_keys) or set(constraint) != constraint_keys or
                        not isinstance(constraint.get("id"), str) or not constraint["id"].strip() or
                        not _rop_shape_linear_terms(constraint.get("terms")) or
                        constraint.get("operator") not in ("<=", ">=", "=") or
                        _agent_finite_number(constraint.get("rhs_log10")) is None or
                        constraint.get("hard") is not True):
                    valid_constraints = False
                    break
        objective_keys = {"id", "terms", "sense"}
        valid_objective = (
            _agent_keys_allowed(objective, objective_keys) and set(objective) == objective_keys and
            isinstance(objective.get("id"), str) and bool(objective["id"].strip()) and
            _rop_shape_linear_terms(objective.get("terms")) and
            objective.get("sense") in ("maximize", "minimize")
        )
        valid = set(raw) == required and valid_constraints and valid_objective

    if not valid:
        return None, f"edit_intent does not match the canonical {kind} contract"
    return copy.deepcopy(raw), None


def _rop_shape_withheld(response, reason):
    out = copy.deepcopy(response) if isinstance(response, dict) else {}
    out.pop("_card", None)
    out["error"] = reason
    out["admission_status"] = "withheld"
    return out


def _rop_shape_replay_card_fields(replay):
    if not isinstance(replay, dict):
        return None, "optimization response is missing replay evidence"
    if replay.get("status") != "pass" or replay.get("complete") is not True or replay.get("pass") is not True:
        return None, "backend replay must have complete=true and pass=true"
    curve = replay.get("curve")
    if not isinstance(curve, dict):
        return None, "backend replay is missing its curve"
    xs = curve.get("param_values")
    rows = curve.get("output_traj")
    valid = curve.get("valid")
    if not isinstance(xs, list) or not isinstance(rows, list) or not isinstance(valid, list):
        return None, "backend replay curve arrays are malformed"
    if not xs or len(xs) != len(rows) or len(xs) != len(valid):
        return None, "backend replay curve arrays must be non-empty and have equal lengths"
    if curve.get("partial") is not False or any(value is not True for value in valid):
        return None, "backend replay curve must have literal valid=true for every point and partial=false"
    series = []
    for x, row in zip(xs, rows):
        if (_agent_finite_number(x) is None or not isinstance(row, list) or not row or
                _agent_finite_number(row[0]) is None):
            return None, "backend replay curve contains a non-finite or malformed point"
        series.append({"x": float(x), "y": float(row[0])})
    request = replay.get("request")
    body = request.get("body") if isinstance(request, dict) else None
    if (not isinstance(request, dict) or
            request.get("endpoint") != "/api/v1/placer_curve" or
            request.get("method") != "POST" or
            not isinstance(body, dict)):
        return None, "backend replay must contain canonical POST /api/v1/placer_curve evidence"
    rules = body.get("rules")
    kd = body.get("kd")
    totals = body.get("totals")
    if (not isinstance(rules, list) or not rules or
            any(not isinstance(rule, str) or not rule for rule in rules)):
        return None, "backend replay request is missing reaction rules"
    if (not isinstance(kd, list) or len(kd) != len(rules) or
            any((_agent_finite_number(value) is None or float(value) <= 0.0) for value in kd)):
        return None, "backend replay request has invalid Kd values"
    if not isinstance(totals, dict):
        return None, "backend replay request has invalid totals"
    input_sym = body.get("input_sym")
    output_sym = body.get("output_sym")
    if not isinstance(input_sym, str) or not input_sym or not isinstance(output_sym, str) or not output_sym:
        return None, "backend replay request is missing input/output symbols"
    return {
        "series": series,
        "rules": copy.deepcopy(rules),
        "kd": copy.deepcopy(kd),
        "totals": copy.deepcopy(totals),
        "input_symbol": input_sym,
        "output_symbol": output_sym,
    }, None


def optimize_rop_shape(network, designability_spec, reference, edit_intent,
                       minimum_parameter_margin, effect_tolerance,
                       work_budget, replay, **extra):
    """Optimize one typed shape edit through the canonical Julia endpoint.

    This adapter validates the allow-listed wire contract and card-admission
    evidence only.  It never compiles witness matrices, computes slopes, or
    derives visual metrics in Python.
    """
    if extra:
        return {"error": "optimize_rop_shape contains unsupported fields: " +
                ", ".join(sorted(str(key) for key in extra))}
    if (not isinstance(designability_spec, dict) or
            designability_spec.get("schema_version") != DESIGNABILITY_SPEC_VERSION or
            not _agent_wrapped_designability_spec_valid(designability_spec)):
        return {"error": "designability_spec must be a valid bne-designability/v1.0.0 object"}
    reference_payload, error = _rop_shape_reference(reference)
    if error:
        return {"error": error}
    network_payload, expected_hash, error = _rop_shape_network(network, reference_payload["kd"])
    if error:
        return {"error": error}
    intent_payload, error = _rop_shape_edit_intent(edit_intent)
    if error:
        return {"error": error}
    minimum_margin = _agent_nonnegative_finite_number(minimum_parameter_margin)
    tolerance = _agent_nonnegative_finite_number(effect_tolerance)
    if minimum_margin is None or tolerance is None:
        return {"error": "minimum_parameter_margin and effect_tolerance must be finite nonnegative numbers"}
    work_payload, error = _rop_shape_work_budget(work_budget)
    if error:
        return {"error": error}
    replay_payload, error = _rop_shape_replay_policy(replay)
    if error:
        return {"error": error}

    request = {
        "schema_version": ROP_SHAPE_OPTIMIZE_REQUEST_VERSION,
        "network": network_payload,
        "expected_network_ir_hash": expected_hash,
        "designability_spec": copy.deepcopy(designability_spec),
        "reference": reference_payload,
        "edit_intent": intent_payload,
        "optimization": {
            "minimum_parameter_margin": minimum_margin,
            "effect_tolerance": tolerance,
        },
        "work_budget": work_payload,
        "replay": replay_payload,
    }
    response = E.rop_shape_optimize(request)
    if not isinstance(response, dict):
        return {"error": "ROP shape optimizer returned a malformed response"}
    if response.get("engine_offline") or response.get("error"):
        return response
    if response.get("schema_version") != ROP_SHAPE_OPTIMIZATION_VERSION:
        return _rop_shape_withheld(response, "ROP shape optimizer returned an unsupported schema_version")
    if response.get("feasible") is not True:
        return _rop_shape_withheld(response, "ROP shape optimizer did not return feasible=true")
    if response.get("geometric_status") != "global_optimal_over_declared_cells":
        return _rop_shape_withheld(response, "geometric result is not global_optimal_over_declared_cells")
    if response.get("certificate_grade") != "exact-window-siso-rop-path-optimization":
        return _rop_shape_withheld(response, "optimization response has an unsupported certificate_grade")
    if not isinstance(response.get("normalized_request"), dict):
        return _rop_shape_withheld(response, "optimization response lacks its normalized request")
    coverage = response.get("coverage")
    if not isinstance(coverage, dict) or coverage.get("truncated") is not False:
        return _rop_shape_withheld(response, "optimization coverage is missing or truncated")
    selected = response.get("selected")
    if not isinstance(selected, dict) or not selected:
        return _rop_shape_withheld(response, "optimization response has no selected result")
    fixed_topology = response.get("fixed_topology")
    identity_semantics = fixed_topology.get("network_identity_semantics") \
        if isinstance(fixed_topology, dict) else None
    canonical_code = fixed_topology.get("network_canonical_code") \
        if isinstance(fixed_topology, dict) else None
    if (not isinstance(fixed_topology, dict) or
            fixed_topology.get("topology_preserved") is not True or
            not isinstance(fixed_topology.get("normalized_network"), dict) or
            not isinstance(fixed_topology.get("network_ir_hash"), str) or
            identity_semantics not in ("canonical_code_available", "positional_content_hash_only") or
            (identity_semantics == "canonical_code_available" and
             (not isinstance(canonical_code, str) or not canonical_code)) or
            (identity_semantics == "positional_content_hash_only" and canonical_code is not None)):
        return _rop_shape_withheld(response, "optimization response lacks normalized fixed-topology identity")
    if not isinstance(response.get("compiled_edit"), dict):
        return _rop_shape_withheld(response, "optimization response lacks compiled_edit provenance")
    if not isinstance(response.get("artifact"), dict):
        return _rop_shape_withheld(response, "optimization response lacks its result artifact")
    replay_fields, error = _rop_shape_replay_card_fields(response.get("replay"))
    if error:
        return _rop_shape_withheld(response, error)

    card = {
        "family": "dose_shape",
        "verdict": "verified_rop_shape_optimization",
        "rules": replay_fields["rules"],
        "kd": replay_fields["kd"],
        "totals": replay_fields["totals"],
        "input_symbol": replay_fields["input_symbol"],
        "output_symbol": replay_fields["output_symbol"],
        "n_reactions": len(replay_fields["rules"]),
        "computed_series": replay_fields["series"],
        "geometric_status": response["geometric_status"],
        "coverage": copy.deepcopy(coverage),
        "selected": copy.deepcopy(selected),
        "compiled_edit": copy.deepcopy(response.get("compiled_edit")),
        "artifact": copy.deepcopy(response.get("artifact")),
        "network": copy.deepcopy(fixed_topology.get("normalized_network")),
        "network_ir_hash": fixed_topology.get("network_ir_hash"),
        "network_canonical_code": canonical_code,
        "network_identity_semantics": identity_semantics,
        "replay": copy.deepcopy(response.get("replay")),
        "designability_spec": copy.deepcopy(designability_spec),
        "certificate_grade": response.get("certificate_grade"),
        "evidence_tier": "global ROP shape optimum over declared cells + complete backend replay",
    }
    result = copy.deepcopy(response)
    result["designability_spec"] = copy.deepcopy(designability_spec)
    result["admission_status"] = "verified_card"
    result["_card"] = card
    return result


def _design_numeric_bounds(raw, default, name):
    values = default if raw is None else raw
    if not isinstance(values, (list, tuple)) or len(values) != 2:
        raise ValueError(f"{name} must contain exactly [min, max]")
    out = [_agent_finite_number(value) for value in values]
    if any(value is None for value in out) or out[0] >= out[1]:
        raise ValueError(f"{name} must be two finite increasing numbers")
    return out


def _design_interpolate(xs, ys, x):
    if x <= xs[0]:
        return ys[0]
    if x >= xs[-1]:
        return ys[-1]
    lo, hi = 0, len(xs) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if xs[mid] <= x:
            lo = mid
        else:
            hi = mid
    width = xs[hi] - xs[lo]
    if width <= 0:
        return ys[lo]
    weight = (x - xs[lo]) / width
    return ys[lo] + weight * (ys[hi] - ys[lo])


def _design_finite_slopes(xs, ys, witnesses):
    if len(xs) < 3 or len(xs) != len(ys):
        return []
    step = max((xs[-1] - xs[0]) / max(len(xs) - 1, 1), 1e-6)
    half_width = max(2.0 * step, 0.08)
    slopes = []
    for witness in witnesses:
        left = max(xs[0], witness - half_width)
        right = min(xs[-1], witness + half_width)
        if right <= left:
            return []
        slopes.append((_design_interpolate(xs, ys, right) - _design_interpolate(xs, ys, left)) /
                      (right - left))
    return slopes


def design_from_behavior(target_program, input_window_log10=None, operating_points_log10=None,
                         kd_log10=None, total_log10=None, max_reactions=5,
                         max_screened=64, max_verified_recommendations=6,
                         max_exact_placements=6, n_points=161, **_):
    """Compile an abstract response program into a real network, parameters, and fresh curve.

    The target intentionally carries no input/output species.  The exact Design Screen searches
    candidate records first and therefore owns the candidate-specific input/readout binding.  A
    returned card is admitted only after the same selected network and Chebyshev/interior parameter
    vector are replayed through a complete finite forward scan on the live Julia engine.
    """
    if not isinstance(target_program, (list, tuple)) or not target_program:
        return {"error": "target_program must be a non-empty array of reaction-order values"}
    if len(target_program) > 16:
        return {"error": "target_program may contain at most 16 reaction-order values"}
    program = [_agent_finite_number(value) for value in target_program]
    if any(value is None or abs(value) > 8 for value in program):
        return {"error": "target_program values must be finite numbers within [-8, 8]"}
    try:
        input_bounds = _design_numeric_bounds(input_window_log10, [-5.0, 5.0], "input_window_log10")
        kd_bounds = _design_numeric_bounds(kd_log10, [-10.0, 10.0], "kd_log10")
        total_bounds = _design_numeric_bounds(total_log10, [-5.0, 5.0], "total_log10")
    except ValueError as exc:
        return {"error": str(exc)}
    if operating_points_log10 is not None:
        if not isinstance(operating_points_log10, (list, tuple)) or len(operating_points_log10) != len(program):
            return {"error": "operating_points_log10 must have one finite value per target-program step"}
        operating_points = [_agent_finite_number(value) for value in operating_points_log10]
        if any(value is None or value < input_bounds[0] or value > input_bounds[1] for value in operating_points):
            return {"error": "operating points must be finite and lie inside input_window_log10"}
        if any(a >= b for a, b in zip(operating_points, operating_points[1:])):
            return {"error": "operating_points_log10 must be strictly increasing"}
    else:
        operating_points = None
    integer_inputs = (
        max_reactions,
        max_screened,
        max_verified_recommendations,
        max_exact_placements,
        n_points,
    )
    integer_values = [_agent_integral_number(value) for value in integer_inputs]
    if any(value is None for value in integer_values):
        return {"error": "design budgets and n_points must be integers"}
    (max_reactions, max_screened, max_verified_recommendations,
     max_exact_placements, n_points) = integer_values
    if not (1 <= max_reactions <= 5 and 1 <= max_screened <= 64 and
            1 <= max_verified_recommendations <= 64 and 1 <= max_exact_placements <= 8 and
            21 <= n_points <= 1000):
        return {"error": "design budgets or n_points exceed the supported synchronous limits"}

    input_window = {"input_log10": input_bounds, "hard": True}
    if operating_points is not None:
        input_window["operating_points_log10"] = operating_points
    common_constraints = {
        "network": {"max_reactions": max_reactions, "allow_near_minimal": True},
        "parameter_bounds": {"by_class": {"kd": kd_bounds, "total": total_bounds}},
    }
    common_ranking = {
        "verified_only": True,
        "prefer": ["evidence_grade", "chebyshev_radius", "complexity"],
    }
    common_audit = {
        "unsupported": "block_if_hard",
        "path_format": "json_pointer",
        "include_supported": True,
    }

    # Phase 1 discovers a candidate-specific input/readout pair without presenting
    # a proxy card.  The final exact-window request is compiled only after this bind.
    discovery_spec = {
        "schema_version": DESIGNABILITY_SPEC_VERSION,
        "source": {
            "kind": "agent_design",
            "provenance": {
                "compiler": "design_from_behavior/v1",
                "stage": "candidate_io_discovery",
            },
        },
        "target": {"legacy_target": {"target_kind": "exact", "target": program}},
        "constraints": common_constraints,
        "candidate_budget": {
            "mode": "all_matches",
            "max_extra_species": 1,
            "max_extra_reactions": 1,
            "max_extra_mu": 1,
            "max_screened": max_screened,
            "max_verified_recommendations": 0,
            "max_recommended": 0,
            "max_near_misses": 0,
            "max_exact_placements": 0,
        },
        "ranking_policy": common_ranking,
        "audit_policy": common_audit,
    }
    discovery = E.design_screen(discovery_spec)
    if not isinstance(discovery, dict):
        return {"error": "candidate I/O discovery returned a malformed response"}
    if discovery.get("engine_offline"):
        return {"engine_offline": True, "error": discovery.get("error")}
    if discovery.get("error"):
        return {"error": f"candidate I/O discovery failed: {discovery.get('error')}"}
    if discovery.get("schema_version") != DESIGN_SCREEN_SCHEMA_VERSION:
        return {"error": "candidate I/O discovery returned an unsupported Design Screen schema_version"}
    discovery_cards = discovery.get("screened_candidates") or []
    if not isinstance(discovery_cards, list) or any(not isinstance(card, dict) for card in discovery_cards):
        return {"error": "candidate I/O discovery returned malformed screened_candidates"}
    if not discovery_cards:
        return {"error": "candidate I/O discovery found no bounded catalogue match",
                "eligible_count": discovery.get("eligible_count"),
                "evaluated_count": discovery.get("evaluated_count"),
                "truncated": discovery.get("truncated")}
    spec = screen = None
    verified = []
    io_attempts = []
    seen_io = set()
    for discovered in discovery_cards:
        discovered_input, discovered_output = discovered.get("inp"), discovered.get("out")
        io_key = (discovered_input, discovered_output)
        if not discovered_input or not discovered_output or io_key in seen_io:
            continue
        seen_io.add(io_key)
        candidate_spec = {
            "schema_version": DESIGNABILITY_SPEC_VERSION,
            "source": {
                "kind": "agent_design",
                "provenance": {
                    "compiler": "design_from_behavior/v1",
                    "io_binding": "design_screen_discovery",
                    "discovery_card_id": discovered.get("card_id"),
                    "discovery_nid": discovered.get("nid"),
                },
            },
            "target": {"behavior_spec": {
                "feature_space": "reaction_order",
                "input": discovered_input,
                "output": discovered_output,
                "program": [
                    {"kind": "reaction_order", "operator": "=", "value": value}
                    for value in program
                ],
                "input_window": input_window,
            }},
            "constraints": common_constraints,
            "candidate_budget": {
                "mode": "all_matches",
                "max_extra_species": 1,
                "max_extra_reactions": 1,
                "max_extra_mu": 1,
                "max_screened": max_screened,
                "max_verified_recommendations": max_verified_recommendations,
                "max_recommended": max_verified_recommendations,
                "max_near_misses": min(12, max_screened),
                "max_exact_placements": max_exact_placements,
            },
            "ranking_policy": common_ranking,
            "audit_policy": common_audit,
        }
        validation = E.validate_designability_spec(candidate_spec)
        if not isinstance(validation, dict):
            return {"error": "DesignabilitySpec validation returned a malformed response"}
        if validation.get("engine_offline"):
            return {"engine_offline": True, "error": validation.get("error")}
        if (validation.get("error") or validation.get("ok") is not True or
                validation.get("blocked_by_unsupported_hard_clause") is True):
            io_attempts.append({"input": discovered_input, "output": discovered_output,
                                "reason": validation.get("error") or "blocked spec"})
            continue
        candidate_screen = E.design_screen(candidate_spec)
        if not isinstance(candidate_screen, dict):
            return {"error": "final Design Screen returned a malformed response"}
        if candidate_screen.get("engine_offline"):
            return {"engine_offline": True, "error": candidate_screen.get("error")}
        if candidate_screen.get("error"):
            io_attempts.append({"input": discovered_input, "output": discovered_output,
                                "reason": candidate_screen.get("error")})
            continue
        if candidate_screen.get("schema_version") != DESIGN_SCREEN_SCHEMA_VERSION:
            return {"error": "final Design Screen returned an unsupported schema_version"}
        candidate_verified = candidate_screen.get("verified_recommendations") or []
        if (not isinstance(candidate_verified, list) or
                any(not isinstance(card, dict) for card in candidate_verified)):
            return {"error": "final Design Screen returned malformed verified_recommendations"}
        if not candidate_verified:
            io_attempts.append({"input": discovered_input, "output": discovered_output,
                                "reason": "no verified recommendation"})
            continue
        spec, screen, verified = candidate_spec, candidate_screen, candidate_verified
        break
    if not verified:
        return {"error": "no discovered candidate I/O pair produced a verified exact-window design",
                "io_attempts": io_attempts,
                "eligible_count": discovery.get("eligible_count"),
                "evaluated_count": discovery.get("evaluated_count"),
                "truncated": discovery.get("truncated")}

    selected = theta = rules = inp = out = xs = ys = series = observed_slopes = witnesses = None
    replay_failures = []
    slope_tolerance = 0.40
    for candidate in verified:
        candidate_theta = ((candidate.get("parameter_recommendation") or {}).get("theta_star") or {})
        candidate_kd = candidate_theta.get("kd") or []
        if not (candidate.get("pass") is True and
                candidate.get("screen_status") in ("verified_exact", "verified_sampled") and
                candidate_theta.get("status") == "computed" and
                candidate_theta.get("source_type") == "exact_solver" and
                candidate_theta.get("bounds_verified") is True and candidate_kd):
            replay_failures.append({"nid": candidate.get("nid"), "reason": "incomplete exact-card contract"})
            continue
        handoff = ((candidate.get("agent_handoff") or {}).get("next_request") or {})
        handoff_body = handoff.get("body") if isinstance(handoff, dict) else None
        if not (handoff.get("endpoint") == "/api/v1/placer_curve" and handoff.get("method") == "POST" and
                isinstance(handoff_body, dict)):
            replay_failures.append({"nid": candidate.get("nid"), "reason": "missing executable Placer handoff"})
            continue
        candidate_rules = handoff_body.get("rules") or []
        candidate_input, candidate_output = handoff_body.get("input_sym"), handoff_body.get("output_sym")
        handoff_kd = handoff_body.get("kd") or []
        handoff_totals = handoff_body.get("totals") or {}
        if not (candidate_rules and candidate_input == candidate.get("inp") and
                candidate_output == candidate.get("out") and handoff_kd == candidate_kd and
                handoff_totals == (candidate_theta.get("totals") or {})):
            replay_failures.append({"nid": candidate.get("nid"), "reason": "Placer handoff disagrees with exact card"})
            continue
        candidate_curve = E.placer_curve(
            rules=candidate_rules, input_sym=candidate_input, output_sym=candidate_output,
            kd=handoff_kd, totals=handoff_totals,
            param_min=input_bounds[0], param_max=input_bounds[1], n_points=n_points,
        )
        if not isinstance(candidate_curve, dict):
            replay_failures.append({"nid": candidate.get("nid"), "reason": "malformed curve response"})
            continue
        if candidate_curve.get("engine_offline"):
            return {"engine_offline": True, "error": candidate_curve.get("error")}
        if candidate_curve.get("error"):
            replay_failures.append({"nid": candidate.get("nid"),
                                    "reason": f"fresh curve failed: {candidate_curve.get('error')}"})
            continue
        candidate_xs = candidate_curve.get("param_values") or []
        candidate_rows = candidate_curve.get("output_traj") or []
        candidate_valid = candidate_curve.get("valid") or []
        if not all(isinstance(values, list) for values in (candidate_xs, candidate_rows, candidate_valid)):
            replay_failures.append({"nid": candidate.get("nid"), "reason": "malformed curve arrays"})
            continue
        candidate_ys = [(row[0] if isinstance(row, list) and row else row) for row in candidate_rows]
        complete = (len(candidate_xs) == len(candidate_ys) == len(candidate_valid) == n_points and
                    all(value is True for value in candidate_valid) and
                    all(_agent_finite_number(value) is not None for value in candidate_xs + candidate_ys) and
                    candidate_curve.get("partial") is False)
        if not complete:
            replay_failures.append({"nid": candidate.get("nid"), "reason": "incomplete/non-finite curve"})
            continue
        candidate_witnesses = candidate_theta.get("witness_input_log10") or operating_points or []
        candidate_slopes = _design_finite_slopes(candidate_xs, candidate_ys, candidate_witnesses)
        slope_flags = (len(candidate_slopes) == len(program) and
                       [abs(value - target) <= slope_tolerance for value, target in zip(candidate_slopes, program)])
        if not slope_flags or not all(slope_flags):
            replay_failures.append({"nid": candidate.get("nid"), "reason": "finite-slope replay mismatch",
                                    "observed_slopes": candidate_slopes})
            continue
        selected, theta = candidate, candidate_theta
        rules, inp, out = candidate_rules, candidate_input, candidate_output
        xs, ys = candidate_xs, candidate_ys
        observed_slopes, witnesses = candidate_slopes, candidate_witnesses
        series = [{"x": round(float(x), 6), "y": round(float(y), 6)} for x, y in zip(xs, ys)]
        break
    if selected is None:
        return {"error": "no exact recommendation passed the fresh finite-curve and local-slope replay",
                "designability_spec": spec, "replay_failures": replay_failures,
                "eligible_count": screen.get("eligible_count"),
                "evaluated_count": screen.get("evaluated_count"),
                "truncated": screen.get("truncated")}
    kd = theta.get("kd") or []
    evidence = {
        "tier": "3a+1",
        "label": "exact Design Screen + fresh finite forward verification",
        "basis": (f"{selected.get('certificate_grade')} with {len(series)}/{len(series)} finite replay points; "
                  f"all {len(program)} local slopes within ±{slope_tolerance:g}"),
    }
    card = {
        "family": "dose_shape",
        "verdict": "verified_design",
        "rules": rules,
        "kd": [float(value) for value in kd],
        "totals": theta.get("totals") or {},
        "input_symbol": inp,
        "output_symbol": out,
        "n_reactions": len(rules),
        "computed_series": series,
        "target_program": program,
        "operating_points_log10": operating_points,
        "witness_input_log10": witnesses,
        "observed_finite_slopes": [round(value, 6) for value in observed_slopes],
        "finite_slope_tolerance": slope_tolerance,
        "finite_slope_pass": True,
        "certificate_grade": selected.get("certificate_grade"),
        "evidence_grade": selected.get("evidence_grade"),
        "chebyshev_radius": (selected.get("metrics") or {}).get("chebyshev_radius"),
        "screen_coverage": {
            "eligible_count": screen.get("eligible_count"),
            "evaluated_count": screen.get("evaluated_count"),
            "truncated": screen.get("truncated"),
        },
        "designability_spec": spec,
        "designability_card": selected,
        "evidence": evidence,
        "evidence_tier": evidence["label"],
    }
    step = max(1, len(series) // 20)
    return {
        "family": "dose_shape",
        "designability_spec": spec,
        "selected_network": rules,
        "selected_input": inp,
        "selected_output": out,
        "selected_kd": card["kd"],
        "selected_totals": card["totals"],
        "certificate_grade": card["certificate_grade"],
        "chebyshev_radius": card["chebyshev_radius"],
        "eligible_count": screen.get("eligible_count"),
        "evaluated_count": screen.get("evaluated_count"),
        "truncated": screen.get("truncated"),
        "forward_points": len(series),
        "witness_input_log10": witnesses,
        "observed_finite_slopes": card["observed_finite_slopes"],
        "finite_slope_pass": True,
        "curve_sample_log10": [[point["x"], point["y"]] for point in series[::step]],
        "evidence_tier": evidence["label"],
        "_card": card,
    }

TOOLS_DISPATCH = {"corpus_overview": corpus_overview, "retrieve_atlas_seed": retrieve_atlas_seed,
                  "retrieve_logic_seed": retrieve_logic_seed, "retrieve_analog_seed": retrieve_analog_seed,
                  "retrieve_multimodal_seed": retrieve_multimodal_seed, "reader_panel": reader_panel,
                  "design_from_behavior": design_from_behavior,
                  "optimize_rop_shape": optimize_rop_shape,
                  "simulate": simulate, "simulate_2d": simulate_2d, "ro_behavior": ro_behavior}

_DOSE_CLASSES = sorted(set(ds.CLASS_MAP))
_ROP_SHAPE_STEP_TOOL_SCHEMA = {"type": "integer", "minimum": 0}
_ROP_SHAPE_STEP_PAIR_TOOL_SCHEMA = {
    "type": "array", "minItems": 2, "maxItems": 2, "uniqueItems": True,
    "items": _ROP_SHAPE_STEP_TOOL_SCHEMA,
}
_ROP_SHAPE_STEP_GROUP_TOOL_SCHEMA = {
    "type": "array", "minItems": 1, "uniqueItems": True,
    "items": _ROP_SHAPE_STEP_TOOL_SCHEMA,
}
_ROP_SHAPE_LINEAR_TERM_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "step": _ROP_SHAPE_STEP_TOOL_SCHEMA,
        "coefficient": {"type": "number", "not": {"const": 0}},
    },
    "required": ["step", "coefficient"],
    "additionalProperties": False,
}
_ROP_SHAPE_LINEAR_CONSTRAINT_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string", "minLength": 1},
        "terms": {"type": "array", "minItems": 1, "items": _ROP_SHAPE_LINEAR_TERM_TOOL_SCHEMA},
        "operator": {"type": "string", "enum": ["<=", ">=", "="]},
        "rhs_log10": {"type": "number"},
        "hard": {"const": True},
    },
    "required": ["id", "terms", "operator", "rhs_log10", "hard"],
    "additionalProperties": False,
}
_ROP_SHAPE_LINEAR_OBJECTIVE_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string", "minLength": 1},
        "terms": {"type": "array", "minItems": 1, "items": _ROP_SHAPE_LINEAR_TERM_TOOL_SCHEMA},
        "sense": {"type": "string", "enum": ["maximize", "minimize"]},
    },
    "required": ["id", "terms", "sense"],
    "additionalProperties": False,
}
_ROP_SHAPE_EDIT_INTENT_TOOL_SCHEMA = {
    "description": "One exact allow-listed edit. Program-step indices are zero-based; never send compiled matrices.",
    "oneOf": [
        {
            "type": "object",
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "kind": {"const": "broaden"},
                "left_span_steps": _ROP_SHAPE_STEP_PAIR_TOOL_SCHEMA,
                "right_span_steps": _ROP_SHAPE_STEP_PAIR_TOOL_SCHEMA,
                "shared_magnitude": {"const": True},
            },
            "required": ["id", "kind", "left_span_steps", "right_span_steps", "shared_magnitude"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "kind": {"const": "separate"},
                "steps": _ROP_SHAPE_STEP_PAIR_TOOL_SCHEMA,
                "preserve_midpoint_tolerance_log10": {"type": "number", "minimum": 0},
            },
            "required": ["id", "kind", "steps", "preserve_midpoint_tolerance_log10"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "kind": {"const": "widen_center"},
                "steps": _ROP_SHAPE_STEP_PAIR_TOOL_SCHEMA,
                "anchor_step": _ROP_SHAPE_STEP_TOOL_SCHEMA,
                "anchor_tolerance_log10": {"type": "number", "minimum": 0},
            },
            "required": ["id", "kind", "steps", "anchor_step", "anchor_tolerance_log10"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "kind": {"const": "translate_group"},
                "group_steps": _ROP_SHAPE_STEP_GROUP_TOOL_SCHEMA,
                "preserve_steps": _ROP_SHAPE_STEP_GROUP_TOOL_SCHEMA,
                "preserve_tolerance_log10": {"type": "number", "minimum": 0},
                "sense": {"type": "string", "enum": ["positive", "negative"]},
                "shared_shift": {"const": True},
            },
            "required": [
                "id", "kind", "group_steps", "preserve_steps",
                "preserve_tolerance_log10", "sense", "shared_shift",
            ],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "kind": {"const": "linear_witness"},
                "constraints": {"type": "array", "items": _ROP_SHAPE_LINEAR_CONSTRAINT_TOOL_SCHEMA},
                "objective": _ROP_SHAPE_LINEAR_OBJECTIVE_TOOL_SCHEMA,
            },
            "required": ["id", "kind", "constraints", "objective"],
            "additionalProperties": False,
        },
    ],
}
TOOLSPEC = [
    {"name": "optimize_rop_shape",
     "description": "EDIT one FIXED, referenced 1-input ROP design through the canonical live Julia optimizer. Use only after a concrete network, DesignabilitySpec v1, and pinned reference design exist. Supply a full NetworkIR+expected hash, or a complete legacy network (rules/reactions plus input/output symbols; reference.kd supplies its Kd values), one allow-listed typed edit intent, explicit optimization/work/replay policies, and no precompiled matrices. The backend alone compiles witness constraints, optimizes declared cells, measures replay features, and returns evidence. A display card is admitted only for a non-truncated global optimum with complete passing replay; otherwise report the diagnostic without inventing a design.",
     "parameters": {"type": "object", "properties": {
         "network": {
             "description": "Either {network_ir, expected_network_ir_hash} or a complete legacy network with exactly one of rules/reactions plus input_symbols and output_symbols.",
             "oneOf": [
                 {
                     "type": "object",
                     "properties": {
                         "network_ir": {"type": "object"},
                         "expected_network_ir_hash": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                     },
                     "required": ["network_ir", "expected_network_ir_hash"],
                     "additionalProperties": False,
                 },
                 {
                     "type": "object",
                     "properties": {
                         "rules": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
                         "input_symbols": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string", "minLength": 1}},
                         "output_symbols": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string", "minLength": 1}},
                     },
                     "required": ["rules", "input_symbols", "output_symbols"],
                     "additionalProperties": False,
                 },
                 {
                     "type": "object",
                     "properties": {
                         "reactions": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
                         "input_symbols": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string", "minLength": 1}},
                         "output_symbols": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"type": "string", "minLength": 1}},
                     },
                     "required": ["reactions", "input_symbols", "output_symbols"],
                     "additionalProperties": False,
                 },
             ],
         },
         "designability_spec": {"type": "object", "description": "the existing canonical bne-designability/v1.0.0 request"},
         "reference": {
             "type": "object",
             "properties": {
                 "reference_hash": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                 "artifact_ref": {"type": "string", "minLength": 1},
                 "network_ir_hash": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                 "operating_points_log10": {"type": "array", "minItems": 2, "items": {"type": "number"}},
                 "kd": {"type": "array", "minItems": 1, "items": {"type": "number", "exclusiveMinimum": 0}},
                 "totals": {"type": "object", "minProperties": 1,
                            "additionalProperties": {"type": "number", "exclusiveMinimum": 0}},
                 "path_identity": {"type": "string", "minLength": 1},
                 "cell_id": {"type": "string", "minLength": 1},
             },
             "required": ["reference_hash", "network_ir_hash", "operating_points_log10", "kd", "totals"],
             "additionalProperties": False,
         },
         "edit_intent": _ROP_SHAPE_EDIT_INTENT_TOOL_SCHEMA,
         "minimum_parameter_margin": {"type": "number", "minimum": 0},
         "effect_tolerance": {"type": "number", "minimum": 0},
         "work_budget": {
             "type": "object",
             "properties": {
                 "max_paths": {"type": "integer", "minimum": 1, "maximum": 2000},
                 "max_cells": {"type": "integer", "minimum": 1, "maximum": 10000},
                 "max_replays": {"type": "integer", "minimum": 1, "maximum": 16},
                 "require_exhaustive": {"const": True},
             },
             "required": ["max_paths", "max_cells", "max_replays", "require_exhaustive"],
             "additionalProperties": False,
         },
         "replay": {
             "type": "object",
             "properties": {
                 "input_window_log10": {
                     "type": "array", "minItems": 2, "maxItems": 2,
                     "items": {"type": "number", "minimum": -20, "maximum": 20},
                 },
                 "sample_points": {"type": "integer", "minimum": 11, "maximum": 1000},
                 "require_complete": {"const": True},
                 "store_curve": {"const": True},
                 "metrics": {
                     "type": "array", "minItems": 1, "maxItems": 1,
                     "items": {
                         "type": "object",
                         "properties": {
                             "kind": {"const": "two_peak"},
                             "min_prominence_log10": {"type": "number", "minimum": 0},
                         },
                         "required": ["kind", "min_prominence_log10"],
                         "additionalProperties": False,
                     },
                 },
             },
             "required": ["input_window_log10", "sample_points", "require_complete", "store_curve", "metrics"],
             "additionalProperties": False,
         },
     }, "required": ["network", "designability_spec", "reference", "edit_intent",
                      "minimum_parameter_margin", "effect_tolerance", "work_budget", "replay"],
        "additionalProperties": False}},
    {"name": "design_from_behavior",
     "description": "END-TO-END 1-input inverse design on the LIVE Julia engine. Translate the user's requested qualitative curve into an ordered reaction-order program (local log-log slopes such as [1,0,-1,0,1,0,-1]), without naming an input or output species. The tool validates a canonical DesignabilitySpec, searches candidate networks, binds candidate-specific I/O only after selection, computes an exact feasible-region Chebyshev/interior parameter vector, rebuilds that selected network, and returns a fresh finite forward curve. Use this first for a requested dose-response pattern or a refinement of its working-point locations. Returned cards are real designs; if no verified recommendation is found, report that bounded result honestly.",
     "parameters": {"type": "object", "properties": {
         "target_program": {"type": "array", "minItems": 1, "maxItems": 16,
                            "items": {"type": "number", "minimum": -8, "maximum": 8},
                            "description": "ordered desired local reaction orders; 0=flat, +1=rising, -1=falling"},
         "input_window_log10": {"type": "array", "minItems": 2, "maxItems": 2,
                                "items": {"type": "number"},
                                "description": "allowed log10 input window; default [-5,5]"},
         "operating_points_log10": {"type": "array", "items": {"type": "number"},
                                    "description": "optional strictly increasing check locations, one per program step; omit to let the backend distribute them"},
         "kd_log10": {"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "number"},
                      "description": "allowed log10 Kd range; default [-10,10]"},
         "total_log10": {"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "number"},
                         "description": "allowed log10 background-total range; default [-5,5]"},
         "max_reactions": {"type": "integer", "minimum": 1, "maximum": 5},
         "max_screened": {"type": "integer", "minimum": 1, "maximum": 64},
         "max_verified_recommendations": {"type": "integer", "minimum": 1, "maximum": 64},
         "max_exact_placements": {"type": "integer", "minimum": 1, "maximum": 8},
         "n_points": {"type": "integer", "minimum": 21, "maximum": 1000}},
         "required": ["target_program"], "additionalProperties": False}},
    {"name": "corpus_overview",
     "description": "Atlas PRIOR: how prevalent each behaviour is in the precomputed μ≤5 survey (robust networks per dose class, per logic gate). Use to gauge feasibility before proposing. Not an answer.",
     "parameters": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "retrieve_atlas_seed",
     "description": "Get SEED 1-input networks for one of the standard dose classes — a PRIOR / starting point to feed into `simulate`, never a verified answer. Each seed includes the reactions plus the input_symbol and observe_species the atlas used for that label; pass those to `simulate` to reproduce and verify the shape.",
     "parameters": {"type": "object", "properties": {
         "behavior_class": {"type": "string", "enum": _DOSE_CLASSES, "description": "one of the system's standard dose class labels"},
         "max_reactions": {"type": "integer"}, "min_robustness": {"type": "number"}},
         "required": ["behavior_class"], "additionalProperties": False}},
    {"name": "retrieve_logic_seed",
     "description": "Get SEED 2-input networks the atlas recorded as realising a Boolean gate — a PRIOR. Pass each seed's reactions to simulate_2d to compute its real surface and verify the gate this session.",
     "parameters": {"type": "object", "properties": {
         "gate": {"type": "string", "enum": ["AND", "OR", "NAND", "NOR", "XOR", "XNOR", "NIMPLY", "IMPLY", "NOT_A", "NOT_B", "A", "B"]}},
         "required": ["gate"], "additionalProperties": False}},
    {"name": "retrieve_analog_seed",
     "description": "Get SEED 2-input networks for an analog SURFACE target — a PRIOR. 'two_input_bump' = an interior peak (output highest at a middle spot of BOTH inputs, flat elsewhere); 'ratio_sensor' = responds to the A/B ratio; 'synergy' = AND-like coactivation. Pass each seed's reactions+input1+input2+observe_species to simulate_2d to compute and verify (simulate_2d reports interior_peak for bumps).",
     "parameters": {"type": "object", "properties": {
         "target": {"type": "string", "enum": ["two_input_bump", "ratio_sensor", "synergy", "coactivation"]}},
         "required": ["target"], "additionalProperties": False}},
    {"name": "simulate",
     "description": "RUN THE LIVE ENGINE on a network: build it, solve its 1-input dose-response over a standard log10 input window, and phenotype it. Returns the COMPUTED curve + the engine's motif/exact label + robustness volume. Use for EVERY candidate you present (atlas seed or your own design, including networks not in the atlas). When verifying a seed, pass that seed's input_symbol and observe_species. If it returns engine_offline, DO NOT fabricate — tell the user the compute engine is offline.",
     "parameters": {"type": "object", "properties": {
         "reactions": {"type": "array", "items": {"type": "string"}, "description": "reaction strings, e.g. ['A + B <-> C_A_B','A + C_A_B <-> C_A_A_B']"},
         "kd": {"type": "array", "items": {"type": "number"}, "description": "dissociation constant per reaction (default all 1.0)"},
         "input_symbol": {"type": "string", "description": "the swept input: a total-concentration symbol like 'tA' (omit to use the first total)"},
         "observe_species": {"type": "string", "description": "the observed output species, e.g. 'C_A_B' (omit to use the main product)"}},
         "required": ["reactions"], "additionalProperties": False}},
    {"name": "simulate_2d",
     "description": "RUN THE LIVE ENGINE on a TWO-INPUT network: build it and solve its 2-input response SURFACE over input1×input2. A complete finite-validity grid returns the realized Boolean gate and margin. A partial/invalid grid is diagnostic only, preserves gaps, and cannot certify a gate, feature, or candidate. engine_offline ⇒ do not fabricate.",
     "parameters": {"type": "object", "properties": {
         "reactions": {"type": "array", "items": {"type": "string"}},
         "kd": {"type": "array", "items": {"type": "number"}, "description": "dissociation constant per reaction (default all 1.0)"},
         "input1": {"type": "string", "description": "first swept input total, e.g. 'tA' (omit to use the first two totals)"},
         "input2": {"type": "string", "description": "second swept input total, e.g. 'tB'"},
         "observe_species": {"type": "string", "description": "observed output species, e.g. 'C_A_B'"}},
         "required": ["reactions"], "additionalProperties": False}},
    {"name": "retrieve_multimodal_seed",
     "description": "Get SEED 1-input networks the atlas found to produce a MULTIMODAL dose-response (response order reverses sign ≥2 times: up-down-up / down-up-down) — a PRIOR ranked by the phenotyper's multimodal fraction over the Kd prior. Call this FIRST for any oscillating / 来回穿梭 / 先增后减再增 request: these topologies are known to realise multimodal, so they are a far better starting point than a hand-guessed network. Then pass a seed's reactions+input_symbol+observe_species to `simulate` to recompute and verify it this session.",
     "parameters": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "ro_behavior",
     "description": "ANALYTIC, parameter-free RO (reaction-order) behavior of a 1-input network: the SISO enumeration of which dose-response sign patterns the TOPOLOGY can produce over ALL kd. Use it to decide whether a target is structurally possible BEFORE hunting kd — especially multimodal: it returns whether any feasible family reverses sign ≥2 times (multimodal_feasible). max_sign_changes<2 ⇒ multimodal is impossible for this network, change the topology; ≥2 ⇒ possible, then realise it with `simulate` and cross-check the sign_seq. Complements simulate (numeric, one kd) with structure.",
     "parameters": {"type": "object", "properties": {
         "reactions": {"type": "array", "items": {"type": "string"}},
         "kd": {"type": "array", "items": {"type": "number"}, "description": "dissociation constant per reaction (does not affect the analytic enumeration; default all 1.0)"},
         "input_symbol": {"type": "string", "description": "the swept input total, e.g. 'tA' (omit to use the first total)"},
         "observe_species": {"type": "string", "description": "observed output species (omit to use the main product)"}},
         "required": ["reactions"], "additionalProperties": False}},
    {"name": "reader_panel",
     "description": "FUNCTION-SPACE READER: an INTENT-ROUTED, evidence-backed candidate PANEL from the frozen industrial atlas — robust→label+support primary; typical/refinement→label recall+function rerank; existential/discovery→function primary; plus a coverage_status (well-covered / sparse / rare-or-absent). Pass EITHER `nl` (the user's behaviour description, compiled to target+intent) OR `prototype`+`intent`. Returns candidates with match_score, evidence_tier (T1/T2/T3a/T3b), shape_support, ROP volume + robust_path_count, panel diversity, route, and coverage_status. A PRIOR/seed list (like retrieve_*): VERIFY each pick with `simulate` before presenting; if coverage_status is rare-or-absent, report that honestly. Needs the atlas (env BNE_ATLAS_ROOT / BNE_PACKET_CORPUS); returns {error} if unavailable.",
     "parameters": {"type": "object", "properties": {
         "nl": {"type": "string", "description": "the user's behaviour request in natural language (preferred; compiled to target+intent+coverage)"},
         "prototype": {"type": "string", "enum": _READER_PROTOS, "description": "target dose shape (if not using nl)"},
         "intent": {"type": "string", "enum": ["existential", "typical", "robust"], "description": "existential=find any realisation (discovery/off-label), typical=medoid, robust=many θ realise it"},
         "behavior_class": {"type": "string", "description": "optional nearest vocab label to seed label-recall"},
         "k": {"type": "integer", "description": "panel size (default 6)"},
         "max_reactions": {"type": "integer", "description": "optional network-size cap"}},
         "additionalProperties": False}},
]
OPENAI_TOOLS = [{"type": "function", "function": t} for t in TOOLSPEC]
ANTHROPIC_TOOLS = [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]} for t in TOOLSPEC]

SYSTEM = """You are the Biocircuits Design Agent. You design equilibrium protein-binding reaction
networks by REASONING and by CALLING A LIVE COMPUTE ENGINE — not by reciting a database. The atlas
is a PRIOR (a map of what's known to exist and good starting points); the ENGINE (which solves the
binding equilibrium / ODE and phenotypes the result) is the source of truth. Every candidate you
present to the user MUST be backed by fresh compute from THIS session.

PRIMARY INVERSE-DESIGN PATH for a 1-input curve pattern:
- Translate the user's requested rises, falls, and flat regions into an ordered reaction-order
  program, then call `design_from_behavior`. Do not invent input/output molecule names: that tool
  searches networks first and binds candidate-specific I/O afterward.
- A cat face / cat forehead / two ears with a flat forehead / 猫猫 or M-shaped response means
  the ordered program [1, 0, -1, 0, 1, 0, -1] across a finite input window.
- If the user moves a transition or working point, preserve the program and change only
  `operating_points_log10`. If they change the qualitative pattern, change the program.
- A successful `design_from_behavior` result already includes exact Design Screen evidence plus a
  fresh finite forward curve. Present its selected network, I/O, Kd/totals, certificate, and curve.
- When the user asks to modify a PINNED existing ROP design (broaden/separate/widen the center/
  translate one group), use `optimize_rop_shape` with that design's full network identity,
  DesignabilitySpec, and reference artifact. Never invent a reference, compile matrix rows, or
  calculate replay slopes/visual metrics yourself; the canonical Julia response owns all of them.
- `optimize_rop_shape` may contribute a card only when it reports a global optimum over all declared
  cells, non-truncated coverage, and a complete passing replay. Otherwise report its returned
  diagnostic and do not present a verified shape edit.
- Never claim success from a compiled request alone. If validation, screening, or the fresh replay
  fails, explain the returned bounded failure and do not fabricate a design.

THE LOOP for any design request:
1. For a 1-input requested pattern, use `design_from_behavior`. For exploratory/manual proposals or
   unsupported families, optionally use corpus_overview / retrieve_atlas_seed / retrieve_logic_seed to get a
   starting topology and gauge feasibility. For a multimodal / oscillating target use
   `retrieve_multimodal_seed` (known-good topologies); `ro_behavior` checks whether a given topology
   can structurally produce a sign pattern before sweeping kd.
2. Propose a concrete network (reactions + kd) — a seed, or your own design. kd is a DESIGN VARIABLE,
   not a constant: do NOT just leave every kd at 1.0. Choose and VARY kd (strong↔weak binding, roughly
   1e-3…1e3 per reaction) to place and shape the requested feature — e.g. weaker/stronger Kd to shift a
   threshold, sharpen a peak/valley, set where a 2-input bump sits, or widen a plateau — and re-simulate
   to compare. Try at least one non-trivial kd set when the request asks for a specific/sharp feature.
3. COMPUTE it on the engine:
   - 1-INPUT dose-response → `simulate` (returns the curve + the phenotype shape + robustness). When
     verifying a seed, pass its input_symbol and observe_species. You do not set the sweep window.
   - 2-INPUT gate / response surface → `simulate_2d` (a complete finite-validity grid returns the real
     heatmap, realized corner gate, and margin; a partial grid is diagnostic only and verifies none
     of those features).
4. VERIFY against the request. If it doesn't match (wrong shape/gate, low robustness/margin), REFINE —
   change kd, add/alter a reaction, pick a different input/observable — and re-simulate. Iterate.
5. Present only complete engine-computed candidates, citing their computed shape/gate +
   robustness/margin. Explain partial diagnostics as incomplete and do not name a realized gate from
   them. Reply in the user's language. KD MATTERS: state the kd you used (every candidate carries its kd).

The system's STANDARD dose (1-input) class labels are exactly: monotone_activation,
activation_with_saturation, monotone_repression, repression_with_floor, thresholded_activation,
biphasic_peak, bandpass_with_plateau, biphasic_valley, multimodal. Map the user's request — in ANY
language and ANY phrasing — to the right label using YOUR OWN language understanding (the code does
not translate natural language; do not expect it to). The engine's phenotype label is the final
arbiter of which class a candidate actually realises, so if you are unsure which label fits, propose
one, simulate, and read back the engine's verdict.

`multimodal` = the output's response order changes sign ≥2 times as the single input sweeps — it goes
up then down then up (or down-up-down, or more) — i.e. ≥2 turning points, distinct from biphasic_peak
/biphasic_valley which have exactly one. Map requests like "RO 正负来回穿梭 / 先增后减再增 / oscillating
/ wiggly / multiple peaks" to `multimodal`. `simulate` returns the engine's sign_seq (e.g. [1,-1,1] =
rise-fall-rise) and n_sign_changes — cite them. Multimodal is RARE in this μ≤5 equilibrium-binding
family and usually needs extra competing complexes / binding sites — so DO NOT hand-guess a topology.
For a multimodal request, START with `retrieve_multimodal_seed`: it returns networks the atlas already
found to realise a multimodal dose-response (ranked by robustness over the Kd prior). Pick a seed,
pass its reactions+input_symbol+observe_species to `simulate`, and read back the verdict. Use
`ro_behavior` when you want the analytic, parameter-free check of which sign sequences a topology can
produce over ALL kd. If `multimodal_feasible` is false (max_sign_changes < 2) the shape
is STRUCTURALLY impossible for that network — change the topology, don't waste kd sweeps. If true,
then find a kd that realises it with `simulate` and confirm the numeric sign_seq matches one of the
analytic families (a real cross-check, not a guess). If after several genuine attempts (varying
topology AND kd) the engine never returns multimodal — and `ro_behavior` shows it is infeasible for
the topologies you tried — SAY SO PLAINLY: "this appears rare/absent in the in-scope networks I tried;
analytically max sign changes was N." DO NOT relabel a single-peak result as multimodal to please the
user. An honest "I could not realise it in scope" is a correct answer; a fabricated match is not.

HARD RULES:
- NEVER invent a network, curve, label, or robustness number. If `simulate` returns engine_offline:true,
  STOP and tell the user the compute engine is offline (and that the node Workspace server must be running);
  return no candidates.
- The atlas seeds and corpus_overview are PRIORS, explicitly unverified — never present them as the answer
  without simulating. Verify 1-input seeds with `simulate` and 2-input seeds with `simulate_2d`.
- `reader_panel` returns a FUNCTION-SPACE PRIOR panel (candidates ranked by curve-match + ROP evidence from
  the PRECOMPUTED atlas) — it is NOT verification. Treat every reader_panel candidate exactly like an atlas
  seed: its match_score, evidence_tier, and shape_support describe the precomputed phenotype, not a fresh
  result. You MUST re-verify a reader_panel pick with `simulate` (its reactions + input_symbol +
  observe_species) before presenting it; never show a reader_panel candidate or its match_score/tier as a
  verified answer. If reader_panel returns {error} (corpus unavailable), do NOT fabricate a panel — say so
  and fall back to retrieve_* or your own design.
- A 3-input / context-indexed (contextual-versatility) request: simulate_2d computes the 2-input
  surface holding the other totals at their default level. Sweeping a third (context) total across
  levels is NOT yet wired, so present the default-context surface and say plainly that the per-context
  sweep is a current limitation — never claim a context-dependent result you have not simulated.
- For greetings, thanks, or non-design chatter: reply briefly and naturally; do NOT call tools.
- Be concise and concrete. Show your reasoning about why a network produces the requested shape.
"""

NEED_KEY_MSG = ("This design agent runs on an LLM that reasons and drives the compute engine. Add an "
                "API key in the ⚙ panel (OpenAI-compatible incl. a local proxy, or Anthropic). "
                "需要在 ⚙ 面板填入一个 LLM key 才能使用。")

def _cfg_value(cfg, *keys):
    for key in keys:
        value = cfg.get(key)
        if isinstance(value, str):
            value = value.strip()
        if value:
            return value
    return None

def _norm_cfg(llm_cfg):
    c = dict(llm_cfg or {})
    env = L.llm_config_from_env()
    out = {"provider": _cfg_value(c, "provider") or "openai",
           "api_key": _cfg_value(c, "api_key", "apiKey"),
           "base_url": _cfg_value(c, "base_url", "baseUrl"),
           "model": _cfg_value(c, "model"),
           "effort": _cfg_value(c, "effort", "reasoning_effort")}
    if not out["api_key"] and c.get("key_file") and os.path.isfile(c["key_file"]):
        out["api_key"] = open(c["key_file"]).read().strip()
    if not out["api_key"] and env.get("api_key"):
        return {"provider": env.get("provider") or "openai",
                "api_key": env.get("api_key"),
                "base_url": env.get("base_url"),
                "model": env.get("model"),
                "effort": out.get("effort") or env.get("effort")}
    if not out["model"]:
        out["model"] = "gpt-5.4-mini" if out["provider"] != "anthropic" else "claude-sonnet-4-6"
    return out

def _run_openai(history, message, dispatch, cfg, max_iters=12):
    msgs = [{"role": "system", "content": SYSTEM}] + history + [{"role": "user", "content": message}]
    final = ""
    for _ in range(max_iters):
        am = L.openai_chat_tools(msgs, OPENAI_TOOLS, **cfg)
        msgs.append(am)
        tcs = am.get("tool_calls") or []
        if not tcs:
            final = am.get("content") or ""
            break
        for tc in tcs:
            fn = tc.get("function") or {}
            try: args = json.loads(fn.get("arguments") or "{}")
            except Exception: args = {}
            result = dispatch(fn.get("name"), args)
            msgs.append({"role": "tool", "tool_call_id": tc.get("id"), "content": json.dumps(result, default=str)[:9000]})
    if not final:
        final = "I kept needing tools without converging — please restate or narrow the request."
    return final, history + [{"role": "user", "content": message}, {"role": "assistant", "content": final}]

def _run_anthropic(history, message, dispatch, cfg, max_iters=12):
    msgs = history + [{"role": "user", "content": message}]
    final = ""
    for _ in range(max_iters):
        resp = L.anthropic_chat_tools(SYSTEM, msgs, ANTHROPIC_TOOLS, **cfg)
        blocks = resp.get("content") or []
        msgs.append({"role": "assistant", "content": blocks})
        tool_uses = [b for b in blocks if b.get("type") == "tool_use"]
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        if not tool_uses:
            final = text; break
        results = [{"type": "tool_result", "tool_use_id": tu.get("id"),
                    "content": json.dumps(dispatch(tu.get("name"), tu.get("input") or {}), default=str)[:9000]}
                   for tu in tool_uses]
        msgs.append({"role": "user", "content": results})
    if not final:
        final = "I kept needing tools without converging — please restate or narrow the request."
    return final, history + [{"role": "user", "content": message}, {"role": "assistant", "content": final}]

def run_turn(state, message, llm_cfg=None, top=3):
    """One conversational turn. state = {'history': [...]}. Returns {kind, reply, family, cards, info,
    state, trace_id}. Emits a replayable DesignAgentTrace (traces/traces.jsonl) every turn; cards shown
    are engine-verified results (atlas seeds are priors, not shown alone)."""
    state = state or {}
    cfg = _norm_cfg(llm_cfg)
    turn_no = sum(1 for m in (state.get("history") or []) if isinstance(m, dict) and m.get("role") == "user") + 1
    trace = {"trace_schema_version": TRACE_SCHEMA_VERSION, "trace_id": "trace_" + uuid.uuid4().hex[:12],
             "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "conversation_turn": turn_no,
             "raw_user_message": message,
             "compiler": {"provider": cfg.get("provider"), "model": cfg.get("model"),
                          "effort": cfg.get("effort"), "prompt_version": COMPILER_PROMPT_VERSION},
             "family": None, "tool_calls": [], "final_response": {}, "failure_tags": [], "user_feedback": None}
    if not cfg.get("api_key"):
        trace["final_response"] = {"kind": "need_key", "n_cards": 0, "abstained": True, "engine_offline": False}
        trace["failure_tags"] = ["no_api_key"]; _write_trace(trace)
        return {"kind": "need_key", "reply": NEED_KEY_MSG, "family": None, "cards": [], "info": {},
                "state": state, "trace_id": trace["trace_id"]}
    history = [m for m in (state.get("history") or []) if isinstance(m, dict) and m.get("content")]
    holder = {"verified": [], "family": None, "engine_offline": False, "info": {}, "designability_spec": None}
    seen = set()
    debug = os.environ.get("BNE_AGENT_DEBUG")
    def dispatch(name, args):
        fn = TOOLS_DISPATCH.get(name)
        if not fn:
            return {"error": f"unknown tool '{name}'"}
        call_args = dict(args or {})
        if name in ("retrieve_atlas_seed", "retrieve_logic_seed", "retrieve_analog_seed"):
            call_args.setdefault("top", top)
        try:
            res = fn(**call_args)
        except Exception as e:
            res = {"error": f"{type(e).__name__}: {e}"}
        if debug:
            shown = {k: v for k, v in res.items() if k != "_card"} if isinstance(res, dict) else res
            sys.stderr.write(f"[tool] {name}({json.dumps(call_args, default=str)[:180]})\n      -> {json.dumps(shown, default=str)[:500]}\n")
        status = ("engine_offline" if isinstance(res, dict) and res.get("engine_offline")
                  else "error" if isinstance(res, dict) and res.get("error") else "ok")
        trace["tool_calls"].append({"call_id": "call_%03d" % (len(trace["tool_calls"]) + 1), "tool": name,
                                    "args": {k: v for k, v in call_args.items() if k != "top"},
                                    "status": status, "summary": _tool_summary(res),
                                    "result_hash": _hash(res) if isinstance(res, dict) else None})
        if isinstance(res, dict):
            if res.get("engine_offline"):
                holder["engine_offline"] = True
            spec_payload_present, spec_payload = _agent_first_present_spec_payload(res)
            card = res.pop("_card", None) if name in (
                "simulate", "simulate_2d", "design_from_behavior", "optimize_rop_shape",
            ) else None
            designability_spec = _agent_designability_spec_from_payload(spec_payload, card)
            if designability_spec:
                holder["designability_spec"] = holder["designability_spec"] or designability_spec
                if card:
                    card["designability_spec"] = designability_spec
            elif card and spec_payload_present:
                holder["info"]["invalid_designability_spec_cards"] = holder["info"].get("invalid_designability_spec_cards", 0) + 1
                return res
            if card:
                request_fingerprint = card.get("request_fingerprint")
                if (isinstance(request_fingerprint, str)
                        and len(request_fingerprint) == 64):
                    key = ("request_fingerprint", request_fingerprint)
                else:
                    key = tuple(card.get("rules") or []) + (
                        card.get("input_symbol") or "", card.get("realized_gate") or "",
                    )
                if key not in seen:
                    seen.add(key); holder["verified"].append(card)
                    holder["family"] = card.get("family") or holder["family"] or "dose_shape"
        return res
    runner = _run_anthropic if cfg["provider"] == "anthropic" else _run_openai
    try:
        final, history = runner(history, message, dispatch, cfg)
    except Exception as e:
        trace["final_response"] = {"kind": "error", "n_cards": 0, "abstained": True, "engine_offline": holder["engine_offline"]}
        trace["failure_tags"].append("llm_backend_error"); _write_trace(trace)
        return {"kind": "error", "reply": f"LLM backend error: {type(e).__name__}: {e}",
                "family": None, "cards": [], "info": {}, "state": state, "trace_id": trace["trace_id"]}
    info = dict(holder["info"])
    if holder["engine_offline"]:
        info["engine_offline"] = True
    # Curate the cards: kd-exploration can simulate a dozen candidates; show the BEST, shape-diverse
    # few (≤6, ≤2 per shape, ranked by support/margin) rather than every exploration step.
    cards = holder["verified"]
    if len(cards) > 6:
        info["explored"] = len(cards)
        def _qual(c):
            return c.get("shape_support") if c.get("shape_support") is not None else (c.get("margin_decades") or 0)
        kept, per_shape = [], {}
        for c in sorted(cards, key=lambda c: -(_qual(c) or 0)):
            sh = c.get("dominant_shape") or c.get("realized_gate") or "?"
            if per_shape.get(sh, 0) >= 2:
                continue
            per_shape[sh] = per_shape.get(sh, 0) + 1
            kept.append(c)
            if len(kept) >= 6:
                break
        cards = kept
    # Finalize the trace: spill each shown card's full artifact, record compact summaries + failure tags.
    trace["family"] = holder["family"]
    trace["final_response"] = {"kind": "agent", "n_cards": len(cards),
                               "abstained": (not cards and not holder["engine_offline"]),
                               "engine_offline": holder["engine_offline"],
                               "cards": [_card_summary(c, _spill_card(c)) for c in cards]}
    if holder["engine_offline"]:
        trace["failure_tags"].append("engine_offline")
    elif holder["family"] and not cards:
        trace["failure_tags"].append("no_candidate")
    best = max((c.get("shape_support") for c in cards if c.get("shape_support") is not None), default=None)
    if best is not None and best < 0.5:
        trace["failure_tags"].append("weak_evidence")
    if info.get("explored"):
        trace["explored"] = info["explored"]
    _write_trace(trace)
    response = {"kind": "agent", "reply": final, "family": holder["family"], "cards": cards,
                "info": info, "state": {"history": history[-12:]}, "trace_id": trace["trace_id"]}
    if holder["designability_spec"]:
        response["designability_spec"] = holder["designability_spec"]
    return response

if __name__ == "__main__":   # CLI smoke: BNE_LLM_* env configures the key; reads stdin lines
    cfg = L.llm_config_from_env()
    st = {}
    for ln in sys.stdin:
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        r = run_turn(st, ln, cfg)
        st = r.get("state") or st
        print(f"\n> {ln}\n[{r.get('kind')}] {r.get('reply')}")
        for c in (r.get("cards") or [])[:3]:
            print("   card:", c.get("verdict"), "support", c.get("shape_support"), "rules", c.get("rules"),
                  "pts", len(c.get("computed_series") or []))
