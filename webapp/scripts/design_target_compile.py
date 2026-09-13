#!/usr/bin/env python3
"""Natural language -> editable equilibrium target. Stdlib; no search or solver calls.

The no-key path supports named mathematical shapes and reports all numerical
assumptions. The configured LLM uses the existing Design Agent transport. Neither
path claims that a compiled target is achievable by a chemical network.
"""
import copy
import json
import math
import re

import llm_compile as llm_transport

VERSION = "bne-design-target/v1.0.0"
MAX_SAMPLES = 4096
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,39}$")


class TargetCompileError(ValueError):
    def __init__(self, message, code="cannot_compile_target"):
        super().__init__(message)
        self.code = code


def _object(value, allowed, where):
    if not isinstance(value, dict) or set(value) - set(allowed):
        raise TargetCompileError(f"{where}: expected an object with supported fields only.", "invalid_target")


def _number(value, where, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise TargetCompileError(f"{where}: expected a finite number.", "invalid_target")
    if positive and value <= 0:
        raise TargetCompileError(f"{where}: expected a positive concentration or weight.", "invalid_target")
    return value


def _name(value, where):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise TargetCompileError(f"{where}: use a chemical identifier such as X, A, or AB.", "invalid_target")


def validate_target(target):
    """Reject incompatible LLM/document output before it can become graph state."""
    _object(target, ("schema_version", "description", "source", "inputs", "outputs", "samples", "validation_samples"), "target")
    if target.get("schema_version") != VERSION:
        raise TargetCompileError("Unsupported target schema version.", "invalid_target")
    if not isinstance(target.get("description"), str) or len(target["description"].encode("utf-8")) > 16000:
        raise TargetCompileError("Target description must contain at most 16000 UTF-8 bytes.", "invalid_target")
    if target.get("source") not in ("curve", "image", "trajectory", "data", "agent"):
        raise TargetCompileError("Unsupported target source.", "invalid_target")
    inputs, outputs = target.get("inputs"), target.get("outputs")
    for axes, kind in ((inputs, "inputs"), (outputs, "outputs")):
        if not isinstance(axes, list) or not 1 <= len(axes) <= 3:
            raise TargetCompileError(f"Use 1–3 {kind}.", "invalid_target")
        if any(not isinstance(axis, dict) for axis in axes):
            raise TargetCompileError(f"Each {kind} entry must be an object.", "invalid_target")
        names = [axis.get("name") for axis in axes]
        for name in names:
            if kind == "inputs":
                _name(name, kind + ".name")
            elif not isinstance(name, str) or not name.strip() or len(name.encode("utf-8")) > 80:
                raise TargetCompileError("Each output needs a nonempty label of at most 80 UTF-8 bytes.", "invalid_target")
        if len(set(names)) != len(names):
            raise TargetCompileError(f"Names within {kind} must be unique.", "invalid_target")
    for axis in inputs:
        _object(axis, ("name", "min", "max", "scale"), "input")
        lo, hi = _number(axis.get("min"), "input.min", positive=True), _number(axis.get("max"), "input.max", positive=True)
        if not 1e-8 <= lo < hi <= 1e8 or axis.get("scale") not in ("linear", "log"):
            raise TargetCompileError("Each input needs an increasing positive range and linear/log scale.", "invalid_target")
    for axis in outputs:
        _object(axis, ("name", "species", "transform", "offset", "optimize_offset", "min", "max"), "output")
        _name(axis.get("species"), "output.species")
        if axis.get("transform") not in ("linear", "log10"):
            raise TargetCompileError("Readout transform must be linear or log10.", "invalid_target")
        if not -8 <= _number(axis.get("offset"), "output.offset") <= 8:
            raise TargetCompileError("Readout offset must be between −8 and 8.", "invalid_target")
        for bound in ("min", "max"):
            if bound in axis and not -1e8 <= _number(axis[bound], "output." + bound) <= 1e8:
                raise TargetCompileError("Output display bounds must be between −1e8 and 1e8.", "invalid_target")
        if "min" in axis and "max" in axis and axis["min"] >= axis["max"]:
            raise TargetCompileError("Output min must be smaller than max.", "invalid_target")
        if not isinstance(axis.get("optimize_offset"), bool):
            raise TargetCompileError("output.optimize_offset must be Boolean.", "invalid_target")
    samples = target.get("samples")
    validation = target.get("validation_samples", [])
    if not isinstance(samples, list) or not samples or not isinstance(validation, list) or len(samples) + len(validation) > MAX_SAMPLES:
        raise TargetCompileError("A target needs 1–4096 combined training and validation samples.", "invalid_target")
    for sample in samples + validation:
        _object(sample, ("inputs", "outputs", "weight"), "sample")
        for values, axes, kind in ((sample.get("inputs"), inputs, "inputs"), (sample.get("outputs"), outputs, "outputs")):
            if not isinstance(values, list) or len(values) != len(axes):
                raise TargetCompileError(f"Sample {kind} dimensions do not match the target.", "invalid_target")
            for index, value in enumerate(values):
                _number(value, "sample." + kind, positive=kind == "inputs")
                if kind == "inputs" and not axes[index]["min"] <= value <= axes[index]["max"]:
                    raise TargetCompileError("A sample input is outside its declared range.", "invalid_target")
                if kind == "outputs" and not -1e8 <= value <= 1e8:
                    raise TargetCompileError("Output samples must be between −1e8 and 1e8.", "invalid_target")
        if not 1e-12 <= _number(sample.get("weight", 1), "sample.weight", positive=True) <= 1e12:
            raise TargetCompileError("Sample weight must be between 1e-12 and 1e12.", "invalid_target")
    return copy.deepcopy(target)


def _validate_reply(reply, message):
    _object(reply, ("target", "chemistry", "interpretation", "warnings"), "compiler reply")
    target = validate_target(reply.get("target"))
    target["description"], target["source"] = message, "agent"
    if not isinstance(reply.get("interpretation"), str) or not reply["interpretation"].strip() or len(reply["interpretation"]) > 6000:
        raise TargetCompileError("Compiler must explain the interpreted target.", "invalid_target")
    warnings = reply.get("warnings", [])
    if not isinstance(warnings, list) or len(warnings) > 24 or any(not isinstance(x, str) or len(x) > 2000 for x in warnings):
        raise TargetCompileError("Compiler warnings must be a bounded list of strings.", "invalid_target")
    result = {"target": target, "interpretation": reply["interpretation"], "warnings": warnings}
    if "chemistry" in reply:
        chemistry = reply["chemistry"]
        _object(chemistry, ("auxiliary_monomers", "max_complex_size", "max_reactions", "allow_homomers"), "chemistry")
        for key, bounds in (("auxiliary_monomers", (0, 4)), ("max_complex_size", (2, 8)), ("max_reactions", (1, 256))):
            if key in chemistry:
                value = chemistry[key]
                if isinstance(value, bool) or not isinstance(value, int) or not bounds[0] <= value <= bounds[1]:
                    raise TargetCompileError(f"chemistry.{key} must be an integer in {bounds}.", "invalid_target")
        if "allow_homomers" in chemistry and not isinstance(chemistry["allow_homomers"], bool):
            raise TargetCompileError("allow_homomers must be Boolean.", "invalid_target")
        result["chemistry"] = copy.deepcopy(chemistry)
    return result


def _readout(name, species):
    return {"name": name, "species": species, "transform": "linear", "offset": 0, "optimize_offset": False}


_NUMBER = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[-+]?\d+)?"


def _compile_local(message, prior):
    text = message.lower()
    if re.search(r"oscillat|time[- ]course|memory|hysteresis|周期振荡|随时间|记忆|迟滞", text):
        raise TargetCompileError("This optimizer fits equilibrium responses. Specify concentration inputs and static readouts; temporal dynamics need a different model.")
    patterns = (
        ("trajectory", r"circle|circular|ellipse|圆形|圆轨迹|椭圆"),
        ("bandpass", r"band[- ]?pass|bell[- ]?shap|带通|先升后降|先上升后下降|钟形"),
        ("switch", r"threshold|switch[- ]?like|sigmoid|阈值|开关|s形"),
        ("repression", r"monoton(?:e|ic)(?:ally)?\s+(?:decreas|repress)|repression|decreasing|单调下降|单调递减|抑制"),
        ("activation", r"monoton(?:e|ic)(?:ally)?\s+(?:increas|activat)|activation|increasing|单调上升|单调递增|激活"),
    )
    matches = [kind for kind, pattern in patterns if re.search(pattern, text)]
    if not matches or len(matches) > 1 and set(matches) not in ({"switch", "activation"}, {"switch", "repression"}):
        raise TargetCompileError("Describe one monotone increase/decrease, threshold switch, bandpass, or circular/elliptic trajectory; otherwise use an LLM key, drawing, image, or explicit sample data. 请明确响应形状、输入和输出。")
    shape = matches[0]
    prior = validate_target(prior) if prior is not None else None
    axis = copy.deepcopy(prior["inputs"][0]) if prior and len(prior["inputs"]) == 1 else {"name": "U" if shape == "trajectory" else "X", "min": .05, "max": 10., "scale": "log"}
    if prior and len(prior["inputs"]) != 1:
        raise TargetCompileError("The local phrase compiler supports one input. Keep your multi-input target in the data/image editor, or configure an LLM to interpret this description.")
    explicit_input = re.search(r"(?:\binput\s+|输入\s*)((?!range\b|from\b|to\b|in\b)[A-Za-z][A-Za-z0-9_]{0,31})(?=\s|[,，:：]|$)", message, re.IGNORECASE)
    if explicit_input:
        axis["name"] = explicit_input.group(1)
    range_match = re.search(r"(?:input(?:\s+range)?|输入(?:范围)?)\s*(?:[A-Za-z][A-Za-z0-9_]*\s*)?(?:from|从|为|[:：=])?\s*(" + _NUMBER + r")\s*(?:to|到|至|\.\.|,)\s*(" + _NUMBER + r")", text)
    if range_match:
        axis["min"], axis["max"] = map(float, range_match.groups())
    if re.search(r"linear(?:\s+input|\s+scale)?|线性(?:输入|坐标|采样)", text):
        axis["scale"] = "linear"
    if re.search(r"logarithmic|log(?:10)?\s+(?:input|scale)|对数", text):
        axis["scale"] = "log"
    if not all(math.isfinite(axis[key]) for key in ("min", "max")) or axis["min"] <= 0 or axis["max"] <= axis["min"]:
        raise TargetCompileError("Input totals require an increasing, strictly positive concentration range.")
    dimension = 2 if shape == "trajectory" else 1
    if prior and len(prior["outputs"]) != dimension and shape != "trajectory":
        raise TargetCompileError("This phrase defines one output. Describe each output with a configured LLM or edit the multi-output sample table.")
    species = [name for name in "ABCDEF" if name != axis["name"]]
    outputs = copy.deepcopy(prior["outputs"]) if prior and len(prior["outputs"]) == dimension else [_readout("x" if dimension == 2 else "response", species[0])]
    if len(outputs) < dimension:
        outputs.append(_readout("y", species[1]))
    low, high = outputs[0].get("min", 0.05), outputs[0].get("max", 1.0)
    output_match = re.search(r"(?:output(?:\s+range)?|输出(?:范围)?)\s*(?:from|从|为|[:：=])?\s*(" + _NUMBER + r")\s*(?:to|到|至|\.\.|,)\s*(" + _NUMBER + r")", text)
    if output_match:
        low, high = map(float, output_match.groups())
        for output in outputs:
            output["min"], output["max"] = low, high
    if not math.isfinite(low) or not math.isfinite(high) or high <= low:
        raise TargetCompileError("Output range must be finite and increasing.")
    count_match = re.search(r"(\d+)\s*(?:samples|points|个?采样点|个?点)", text)
    count = int(count_match.group(1)) if count_match else (41 if shape == "trajectory" else 25)
    if not 3 <= count <= 1024:
        raise TargetCompileError("Use 3–1024 points for a local phrase target.")
    threshold_match = re.search(r"(?:threshold|阈值)\s*(?:at|为|[:：=])?\s*(" + _NUMBER + r")", text)
    middle = .5
    if threshold_match:
        threshold = float(threshold_match.group(1))
        if not axis["min"] < threshold < axis["max"]:
            raise TargetCompileError("The threshold must lie inside the input range.")
        middle = ((math.log(threshold) - math.log(axis["min"])) / (math.log(axis["max"]) - math.log(axis["min"]))) if axis["scale"] == "log" else (threshold - axis["min"]) / (axis["max"] - axis["min"])
    steepness = 20 if re.search(r"sharp|steep|ultrasensitive|陡|超敏", text) else 12
    plateau = .45 if re.search(r"wide|broad|宽", text) else .3
    def sigmoid(value):
        return 1 / (1 + math.exp(max(-700, min(700, -value))))
    def sample(t):
        x = math.exp(math.log(axis["min"]) + (math.log(axis["max"]) - math.log(axis["min"])) * t) if axis["scale"] == "log" else axis["min"] + (axis["max"] - axis["min"]) * t
        x = min(axis["max"], max(axis["min"], x))
        if shape == "trajectory":
            radius = (high - low) / 2
            center = (low + high) / 2
            y_low, y_high = outputs[1].get("min", low), outputs[1].get("max", high)
            y_radius = (y_high - y_low) / (4 if re.search(r"ellipse|椭圆", text) else 2)
            ys = [center + radius * math.cos(2 * math.pi * t), (y_low + y_high) / 2 + y_radius * math.sin(2 * math.pi * t)]
        else:
            if shape == "bandpass":
                level = sigmoid(steepness * (t - (.5 - plateau / 2))) * sigmoid(steepness * ((.5 + plateau / 2) - t))
            elif shape == "switch":
                level = sigmoid(steepness * (t - middle))
                if "repression" in matches:
                    level = 1 - level
            else:
                level = t if shape == "activation" else 1 - t
            ys = [low + (high - low) * level]
        return {"inputs": [x], "outputs": ys, "weight": 1}
    target = {"schema_version": VERSION, "description": message, "source": "agent", "inputs": [axis], "outputs": outputs,
              "samples": [sample(i / (count - 1)) for i in range(count)],
              "validation_samples": [sample((i + .5) / (count - 1)) for i in range(count - 1)]}
    interpretation = f"{shape}: {axis['name']} in [{axis['min']:g}, {axis['max']:g}] ({axis['scale']}), {dimension} output(s) in [{low:g}, {high:g}], {count} training points."
    if shape == "trajectory":
        interpretation += " Increasing input traverses the path once counterclockwise from its rightmost point; coordinates are output concentrations."
    elif shape == "switch":
        interpretation += f" Transition at normalized input {middle:g}, logistic steepness {steepness}."
    elif shape == "bandpass":
        interpretation += f" Logistic rise/fall steepness {steepness}, centered band width {plateau:g} of the input axis."
    else:
        interpretation += " Output changes linearly along the chosen input axis."
    interpretation += " Readouts: " + "; ".join(f"{out['name']} = {out['transform']}([{out['species']}]) + {out['offset']:g}" for out in outputs) + "."
    reply = {"target": target, "interpretation": interpretation,
             "warnings": ["Local phrase interpretation: only the shape, range, sampling, and constraints listed here were compiled. Edit the target preview to specify additional details. No network feasibility has been established."]}
    max_reactions = re.search(r"(?:at most|max(?:imum)?|最多|不超过)\s*(\d+)\s*(?:reactions|个?反应)", text)
    if max_reactions:
        reply["chemistry"] = {"max_reactions": int(max_reactions.group(1))}
        reply["interpretation"] += f" Network cap: {max_reactions.group(1)} reactions."
    return _validate_reply(reply, message)


SYSTEM = """Compile a requested equilibrium binding-network design target into JSON, never a network.
Use submit_target with {target, interpretation, warnings, chemistry?}. If the request is ambiguous,
temporal, or cannot be represented, respond with {\"clarification\":\"specific missing information\"}.
Never replace an unknown request with a familiar preset. Text must actually determine the samples.
target={schema_version:'bne-design-target/v1.0.0',description:string,source:'agent',
inputs:[{name:'X',min:0.05,max:10,scale:'log'|'linear'}],
outputs:[{name:'response',species:'A',transform:'linear'|'log10',offset:0,optimize_offset:false}],
samples:[{inputs:[positive concentration],outputs:[finite target],weight:1}],validation_samples?:[same]}.
1–3 inputs, 1–3 outputs; match every sample dimension; at most 128 points for this compilation.
Input names and output species must be chemical identifiers (letter then letters/digits/underscore).
Readout = transform(species concentration) + offset, with offset in [-8,8]. Optional output min/max
are editable viewport/range metadata in [-1e8,1e8]. Samples use this transformed readout space.
Input concentrations and ranges must stay in [1e-8,1e8], output samples in [-1e8,1e8].
The engine automatically generates legal binding complexes from inputs and auxiliary monomers A,B,C,…
(excluding input names). Choose auxiliary monomer output species, or preserve valid prior readouts.
Do not return formulas, expression strings, image URLs, reactions, scripts, or API credentials.
Image field (x,y)->intensity and parametric trajectory u->(x,y) are different mappings; use the requested
mapping. State trajectory ordering, physical ranges, sampling and every assumed numerical choice.
chemistry optional keys: auxiliary_monomers 0–4, max_complex_size 2–8, max_reactions 1–256,
allow_homomers Boolean. Preserve current target's dimensional roles/readouts when refining unless
explicitly changed. Return the complete edited target, with interpretation and warnings as plain text.
Do not claim target feasibility or a minimal network; optimization and pruning occur later.
"""
_TOOL_SCHEMA = {"type": "object", "properties": {"target": {"type": "object"}, "interpretation": {"type": "string"}, "warnings": {"type": "array", "items": {"type": "string"}}, "chemistry": {"type": "object"}}, "required": ["target", "interpretation", "warnings"], "additionalProperties": False}


def _llm_reply(message, prior, cfg):
    user = "Design request:\n" + message
    if prior is not None:
        user += "\nCurrent editable target:\n" + json.dumps(validate_target(prior), ensure_ascii=False)
    for attempt in range(2):
        try:
            if cfg.get("provider") == "anthropic":
                response = llm_transport.anthropic_chat_tools(SYSTEM, [{"role": "user", "content": user}],
                    [{"name": "submit_target", "description": "Return an editable target with explicit assumptions", "input_schema": _TOOL_SCHEMA}], **cfg)
                calls = [block for block in response.get("content", []) if block.get("type") == "tool_use"]
                if calls:
                    if len(calls) != 1 or calls[0].get("name") != "submit_target":
                        raise TargetCompileError("Unexpected target compiler tool response.", "invalid_target")
                    reply = calls[0].get("input")
                else:
                    reply = json.loads("".join(block.get("text", "") for block in response.get("content", []) if block.get("type") == "text"))
            else:
                response = llm_transport.openai_chat_tools([{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
                    [{"type": "function", "function": {"name": "submit_target", "description": "Return an editable target with explicit assumptions", "parameters": _TOOL_SCHEMA}}], **cfg)
                calls = response.get("tool_calls", [])
                if calls:
                    if len(calls) != 1 or calls[0].get("function", {}).get("name") != "submit_target":
                        raise TargetCompileError("Unexpected target compiler tool response.", "invalid_target")
                    reply = json.loads(calls[0]["function"]["arguments"])
                else:
                    reply = json.loads(response.get("content") or "")
            if isinstance(reply, dict) and set(reply) == {"clarification"} and isinstance(reply["clarification"], str):
                raise TargetCompileError(reply["clarification"][:1000])
            return _validate_reply(reply, message)
        except TargetCompileError as error:
            if error.code == "cannot_compile_target":
                raise
        except (ValueError, KeyError, TypeError, AttributeError):
            pass
        user += "\nPrevious output failed structural validation. Return a complete finite target matching every axis and the exact documented schema."
    raise TargetCompileError("The configured model did not return a valid target after correction. Edit the target manually or clarify the description.", "invalid_target")


def compile_target(message, llm=None, target=None):
    if not isinstance(message, str) or not message.strip() or len(message.encode("utf-8")) > 16000:
        raise TargetCompileError("Describe the target within 16000 UTF-8 bytes.")
    if llm is not None and not isinstance(llm, dict):
        raise TargetCompileError("LLM configuration must be an object.")
    message = message.strip()
    config = dict(llm or {})
    if not config.get("api_key"):
        env = llm_transport.llm_config_from_env()
        if env.get("api_key"):
            config = env
    if config.get("api_key"):
        if config.get("provider", "openai") not in ("openai", "anthropic"):
            raise TargetCompileError("Unsupported LLM provider.")
        if config.get("provider", "openai") == "openai" and not config.get("base_url"):
            raise TargetCompileError("Configure the OpenAI-compatible Base URL in the existing LLM settings.")
        try:
            return _llm_reply(message, target, config)
        except TargetCompileError:
            raise
        except Exception:
            # Upstream errors may include request headers or echo credentials.
            raise TargetCompileError("The configured LLM could not be reached. Check its settings or use the drawing/data target editor.", "target_provider_failed") from None
    return _compile_local(message, target)
