// Shared event-gating for delegated data-action handlers.
//
// Buttons/menu rows should fire on click; value controls should fire on their
// semantic value event. Without this guard, clicking a <select data-action>
// fires before the value changes and can destroy the field the user was editing.

const CLICK_SAFE_INPUT_TYPES = new Set(['button', 'submit', 'reset']);

export function shouldDispatchActionForEvent(eventType, actionElement) {
  const type = String(eventType || '').toLowerCase();
  if (type !== 'click') return true;

  const tag = String(actionElement?.tagName || '').toUpperCase();
  if (tag === 'SELECT' || tag === 'TEXTAREA') return false;
  if (tag === 'INPUT') {
    return CLICK_SAFE_INPUT_TYPES.has(String(actionElement.type || '').toLowerCase());
  }
  return true;
}
