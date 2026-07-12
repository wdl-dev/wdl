// Environment constants and boot-time process.env mutations for integration
// tests. Imported by nearly every other helper; never imports sibling helpers.

import {
  ADMIN_HOST_HEADER,
  LOCAL_ADMIN_TOKEN,
  LOCAL_CONNECT_HOST,
  ROOT,
  localAssetsCdnBase,
  localControlUrl,
  resolveWdlCliBin,
} from "../../../scripts/integration-environment.js";

export { ROOT };

// Control reaches through gateway with Host: admin.test —
// gateway's admin-host short-circuit picks it up. Matches docker-compose.yml.
export const GATEWAY_HOST = LOCAL_CONNECT_HOST;
export const GATEWAY_PORT = Number(process.env.WDL_GATEWAY_HOST_PORT || 8080);
export const S3MOCK_HOST = LOCAL_CONNECT_HOST;
export const S3MOCK_PORT = Number(process.env.WDL_S3MOCK_HOST_PORT || 19500);
export const ASSETS_CDN_BASE = localAssetsCdnBase(S3MOCK_PORT);
export { ADMIN_HOST_HEADER };
export const ADMIN_TOKEN = LOCAL_ADMIN_TOKEN;
export const WDL_CLI_BIN = resolveWdlCliBin();
// Integration tests are local-compose only. Force the control env so host
// shell credentials (staging/production ADMIN_TOKEN, CONTROL_URL, WDL_NS, etc.) cannot
// leak into adminFetch() or CLI child processes and produce misleading 401s.
process.env.ADMIN_TOKEN = ADMIN_TOKEN;
export const CONTROL_URL = localControlUrl(GATEWAY_PORT);
process.env.CONTROL_URL = CONTROL_URL;
process.env.ASSETS_CDN_BASE = ASSETS_CDN_BASE;
// admin.test has no DNS on most machines — aim the socket at localhost.
process.env.CONTROL_CONNECT_HOST = LOCAL_CONNECT_HOST;
delete process.env.WDL_NS;
delete process.env.CLOUDFLARE_ENV;
