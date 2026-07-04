import assert from 'node:assert/strict';

const { writeClipboardText } = await import('../public/js/native-clipboard.js');

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function resetGlobals() {
  delete global.window;
  delete global.navigator;
}

await test('uses the native macOS bridge before the browser Clipboard API', async () => {
  let nativeText = null;
  let browserUsed = false;
  global.window = {
    BiocircuitsExplorerNativeShell: {
      copyText(text) {
        nativeText = text;
        return true;
      },
    },
  };
  Object.defineProperty(global, 'navigator', {
    configurable: true,
    value: {
      clipboard: {
        writeText() {
          browserUsed = true;
        },
      },
    },
  });

  await writeClipboardText('raw output');

  assert.equal(nativeText, 'raw output');
  assert.equal(browserUsed, false);
  resetGlobals();
});

await test('falls back to the browser Clipboard API outside the native shell', async () => {
  let browserText = null;
  global.window = {};
  Object.defineProperty(global, 'navigator', {
    configurable: true,
    value: {
      clipboard: {
        async writeText(text) {
          browserText = text;
        },
      },
    },
  });

  await writeClipboardText(42);

  assert.equal(browserText, '42');
  resetGlobals();
});

await test('reports failure when no clipboard channel exists', async () => {
  global.window = {};
  Object.defineProperty(global, 'navigator', {
    configurable: true,
    value: {},
  });

  await assert.rejects(
    () => writeClipboardText('raw output'),
    /Clipboard is unavailable/,
  );
  resetGlobals();
});

console.log(`\nAll ${passed} native clipboard tests passed.`);
