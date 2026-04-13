# frontend-swift

Native macOS shell for Biocircuits Explorer.

This Xcode project is the supported native macOS shell. It reuses the repo root backend and web frontend instead of copying them into a separate app workspace.

The native shell talks to the embedded web UI through the explicit
`window.BiocircuitsExplorerWorkspaceShell` contract exposed by
[`webapp/public/app-node.js`](../webapp/public/app-node.js). Save/load actions
and workspace change notifications flow through that contract instead of
depending on the Swift layer to patch internal frontend functions.

Backend launch order:

1. `BIOCIRCUITS_EXPLORER_BACKEND_ROOT` if explicitly configured
2. In Debug builds, `webapp/server.jl` with a local Julia installation
3. Bundled or repo-local `dist/BiocircuitsExplorerBackend`
4. Source-mode fallback if no compiled backend is available

The Xcode target now includes a build phase that copies `dist/BiocircuitsExplorerBackend`
into `BiocircuitsExplorerMac.app/Contents/Resources/backend` when that bundle exists.
If the compiled backend is missing, the build still succeeds and the app falls
back to source-mode startup.

The app-managed workspace JSON files live under:

- `~/Library/Application Support/Biocircuits Explorer/Projects/`

Runtime overrides:

- `BIOCIRCUITS_EXPLORER_BACKEND_ROOT`: explicit compiled backend bundle to launch
- `BIOCIRCUITS_EXPLORER_REPO_ROOT`: explicit repo root used for source-mode fallback
- `BIOCIRCUITS_EXPLORER_PREFER_SOURCE_BACKEND`: optional `true`/`false` override for Debug-style source preference
- `BIOCIRCUITS_EXPLORER_PORT`: explicit local backend port used by the Swift shell
- `JULIA_EXECUTABLE`: explicit Julia binary for source-mode fallback
- `BIOCIRCUITS_EXPLORER_BACKEND_BUNDLE_SOURCE`: build-time source for the Xcode copy script

Legacy `ROP_*` overrides are still accepted for backward compatibility.

Local build example:

```bash
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj -scheme BiocircuitsExplorerMac -configuration Debug -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```

Build with an embedded backend bundle:

```bash
julia --project=packaging packaging/build_backend_app.jl
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj -scheme BiocircuitsExplorerMac -configuration Debug -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```
