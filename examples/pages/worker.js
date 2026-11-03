import iconBytes from "./icon.png";

function html(prefix) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>WDL · Pages Demo</title>
  <style>
    body {
      font-family: system-ui, -apple-system, sans-serif;
      max-width: 640px;
      margin: 4rem auto;
      padding: 0 1.5rem;
      color: #222;
      line-height: 1.6;
    }
    .hero { text-align: center; }
    .hero img {
      width: 120px; height: 120px;
      border-radius: 24px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    }
    h1 { margin-top: 1rem; }
    code {
      background: #f4f4f7;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 0.9em;
    }
    .meta {
      margin-top: 3rem;
      padding: 1rem;
      background: #f8f9fb;
      border-radius: 8px;
      font-size: 0.9em;
    }
    .meta dt { font-weight: 600; margin-top: 0.5rem; }
    .meta dd { margin: 0; color: #555; }
  </style>
</head>
<body>
  <div class="hero">
    <img src="${prefix}/icon.png" alt="workerd logo">
    <h1>Pages Worker</h1>
    <p>This HTML, its CSS, and the PNG logo are all bundled into a single
    dynamically-loaded worker. No static file server, no S3 — the runtime
    pulls the bundle from a Redis Hash and hands it to <code>workerLoader</code>.</p>
  </div>

  <div class="meta">
    <dl>
      <dt>Main module</dt>
      <dd><code>worker.js</code></dd>
      <dt>Binary module</dt>
      <dd><code>icon.png</code> (<code>import</code>-ed as <code>ArrayBuffer</code>)</dd>
      <dt>Storage</dt>
      <dd>Redis Hash <code>worker:demo:pages:v:1</code> — one field per module, PNG bytes stored raw (binary-safe RESP, no base64)</dd>
    </dl>
  </div>
</body>
</html>`;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/icon.png") {
      return new Response(iconBytes, {
        headers: {
          "content-type": "image/png",
          "cache-control": "public, max-age=3600",
        },
      });
    }

    const prefix = request.headers.get("x-worker-prefix") || "";
    return new Response(html(prefix), {
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  },
};
