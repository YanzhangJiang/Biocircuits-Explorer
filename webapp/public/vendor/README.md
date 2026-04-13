Place a local copy of `plotly-2.27.0.min.js` in this directory to make the
desktop build fully offline-capable.

The HTML entrypoints first attempt to load:

- `vendor/plotly-2.27.0.min.js`

and only fall back to the Plotly CDN if that local file is missing.
