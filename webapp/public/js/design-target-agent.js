// Target-only Design Agent transport. The caller owns document/node identity,
// cancellation and applying the returned editable target through a graph patch.
import { getLLMConfig } from './llm-config.js';
import { designChatRequestHeaders, designTargetCompileUrl } from './design-chat-client.js';
import { validateDesignTarget } from './design-target-adapters.js';

export async function compileDesignTarget(message, { target, signal, fetchImpl = globalThis.fetch } = {}) {
  const text = typeof message === 'string' ? message.trim() : '';
  if (!text || text.length > 12000) throw new Error('Describe the target in 1–12000 characters.');
  if (signal?.aborted) throw new DOMException('Target compilation cancelled.', 'AbortError');
  const response = await fetchImpl(designTargetCompileUrl(), {
    method: 'POST',
    headers: designChatRequestHeaders({ json: true }),
    body: JSON.stringify({ message: text, llm: getLLMConfig(), ...(target ? { target } : {}) }),
    signal,
  });
  // Some transports cannot interrupt a completed response. Honour cancellation
  // before allowing it to be returned to the current workspace owner.
  if (signal?.aborted) throw new DOMException('Target compilation cancelled.', 'AbortError');
  const result = await response.json();
  if (!response.ok) {
    const error = new Error(typeof result?.error === 'string' ? result.error : 'Target compilation failed.');
    error.code = result?.code || 'target_compile_failed';
    throw error;
  }
  if (signal?.aborted) throw new DOMException('Target compilation cancelled.', 'AbortError');
  if (result?.target?.schema_version !== 'bne-design-target/v1.0.0' ||
      !Array.isArray(result.target.inputs) || !Array.isArray(result.target.outputs) ||
      !Array.isArray(result.target.samples) || !result.target.samples.length ||
      typeof result.interpretation !== 'string' || !Array.isArray(result.warnings)) {
    throw new Error('The Design Agent returned an invalid target.');
  }
  result.target = validateDesignTarget(result.target);
  return result;
}
