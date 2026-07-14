import { createTaskIdentityResolver } from "shared-task-identity";
import { validOwnerEndpointForService } from "shared-owner-endpoint";
import { DoRuntimeError } from "do-runtime-protocol";

const resolver = createTaskIdentityResolver({
  envPrefix: "DO",
  defaultPort: 8788,
  defaultContainerName: "do-runtime",
  serviceLabel: "DO",
  unavailableCode: "task_identity_unavailable",
  createError: (status, code, message) => new DoRuntimeError(status, code, message),
  validateEndpoint: (endpoint, port) => validOwnerEndpointForService(endpoint, port, "do-runtime"),
});

export const {
  peekTaskIdentity,
  resetTaskIdentityForTests,
  resolveTaskIdentity,
  taskIdentityFromEcsMetadata,
  taskIdentityFromEnv,
} = resolver;
