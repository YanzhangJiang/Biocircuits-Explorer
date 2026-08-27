export function canonicalJsonValue(value) {
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) normalized[key] = canonicalJsonValue(value[key]);
  return normalized;
}

export function stableJson(value) {
  return JSON.stringify(canonicalJsonValue(value));
}

export function sameJson(left, right) {
  return stableJson(left) === stableJson(right);
}
