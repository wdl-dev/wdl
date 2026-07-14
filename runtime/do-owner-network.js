import { validOwnerEndpointForService } from "shared-owner-endpoint";
import { withInternalAuth } from "shared-internal-auth";

/**
 * @param {{ method: string, pathname: string }} request
 */
function allowedDoOwnerRequest({ method, pathname }) {
  return (method === "POST" && pathname === "/internal/do/invoke") ||
    (method === "GET" && pathname === "/internal/do/connect");
}

export default {
  /** @param {Request} request @param {Record<string, unknown>} env */
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!validOwnerEndpointForService(url.host, 8788, "do-runtime")) {
      return Response.json({
        error: "invalid_owner_endpoint",
        message: "Invalid Durable Object owner endpoint",
      }, { status: 400 });
    }
    if (!allowedDoOwnerRequest({ method: request.method, pathname: url.pathname })) {
      return Response.json({
        error: "invalid_owner_path",
        message: "Invalid Durable Object owner path",
      }, { status: 400 });
    }
    return await fetch(new Request(request, {
      headers: withInternalAuth(request.headers, env),
    }));
  },
};
