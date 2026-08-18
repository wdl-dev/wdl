# JSONC hello worker

Minimal WDL smoke worker using a commented `wrangler.jsonc` manifest and one
plain-text variable.

```bash
npm install --ignore-scripts --prefix examples/hello-jsonc
wdl deploy examples/hello-jsonc --ns demo
curl -H "Host: demo.workers.local" "http://localhost:8080/hello-jsonc/"
```
