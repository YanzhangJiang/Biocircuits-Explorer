// Physical model defaults and per-analysis overrides. Keep this module pure:
// parameter ordering comes from the built model, never from object key order.

function positiveNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) && value > 0;
}

export function modelParameterDefaults(model, totals = null) {
  const defaults = {};
  (model?.q_sym || []).forEach((symbol, index) => {
    const species = model.free_species?.[index] || String(symbol).replace(/^t/, '');
    const fitted = totals?.[species];
    defaults[symbol] = positiveNumber(fitted) ? fitted : 1;
  });
  (model?.K_sym || []).forEach((symbol, index) => {
    const kd = model.kd?.[index];
    defaults[symbol] = positiveNumber(kd) ? kd : 1;
  });
  return defaults;
}

export function resolvedModelParameters(context, overrides = {}) {
  const model = context?.model;
  if (!model) return {};
  const defaults = context.parameterDefaults || modelParameterDefaults(model, context.totals);
  return Object.fromEntries([...model.q_sym, ...model.K_sym].map(symbol => [
    symbol,
    Object.hasOwn(overrides || {}, symbol) ? overrides[symbol] : defaults[symbol],
  ]));
}

export function scanConfigWithModelParameters(config, context) {
  if (!config) return config;
  const { fixedParameterOverrides, ...request } = config;
  // Existing programmatic explicit log-qK requests keep their values. For
  // ordinary models without overrides the backend's imported-Kd default is
  // unchanged; fitted design concentrations require the full explicit vector.
  if (Object.hasOwn(config, 'fixed_qK') || !context?.model ||
      (!context.totals && !Object.keys(fixedParameterOverrides || {}).length)) return request;
  const physical = resolvedModelParameters(context, fixedParameterOverrides);
  const symbols = [...context.model.q_sym, ...context.model.K_sym];
  request.fixed_qK = symbols.map(symbol => {
    const value = physical[symbol];
    if (!positiveNumber(value) || Math.abs(Math.log10(value)) > 20) {
      throw new Error(`Fixed ${symbol} must be a positive concentration or Kd between 1e-20 and 1e20.`);
    }
    return Math.log10(value);
  });
  return request;
}
