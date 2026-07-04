export async function writeClipboardText(text) {
  const value = String(text ?? '');
  const nativeCopy =
    globalThis.window?.BiocircuitsExplorerNativeShell?.copyText ||
    globalThis.window?.ROPNativeShell?.copyText;

  if (typeof nativeCopy === 'function') {
    const copied = nativeCopy(value);
    if (copied === false) {
      throw new Error('Native clipboard is unavailable');
    }
    return true;
  }

  const browserCopy = globalThis.navigator?.clipboard?.writeText;
  if (typeof browserCopy === 'function') {
    await browserCopy.call(globalThis.navigator.clipboard, value);
    return true;
  }

  throw new Error('Clipboard is unavailable');
}
