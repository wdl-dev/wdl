import { createTaskIdentityResolver } from "shared-task-identity";
import { validOwnerEndpointForService } from "shared-owner-endpoint";
import { D1ProtocolError } from "d1-runtime-protocol";

const resolver = createTaskIdentityResolver({
  envPrefix: "D1",
  defaultPort: 8787,
  defaultContainerName: "d1-runtime",
  serviceLabel: "D1",
  unavailableCode: "task-identity-unavailable",
  createError: (status, code, message) => new D1ProtocolError(status, code, message),
  validateEndpoint: (endpoint, port) => validOwnerEndpointForService(endpoint, port, "d1-runtime"),
});

export const {
  peekTaskIdentity,
  resetTaskIdentityForTests,
  resolveTaskIdentity,
  taskIdentityFromEcsMetadata,
  taskIdentityFromEnv,
} = resolver;
