import D1_CLIENT_SOURCE from "runtime-d1-client-source";
import D1_DATA_FIELD_SOURCE from "runtime-d1-data-field-source";
import D1_PARAMS_SOURCE from "runtime-d1-params-source";
import SQL_SPLITTER_SOURCE from "runtime-sql-splitter-source";
import D1_TRANSPORT_SOURCE from "runtime-d1-transport-source";
import R2_CLIENT_SOURCE from "runtime-r2-client-source";
import R2_UTILS_SOURCE from "runtime-r2-utils-source";
import DO_CLIENT_SOURCE from "runtime-do-client-source";
import DO_TRANSPORT_SOURCE from "runtime-do-transport-source";
import OWNER_ENDPOINT_SOURCE from "runtime-owner-endpoint-source";
import OWNER_HINT_CACHE_SOURCE from "runtime-owner-hint-cache-source";
import REQUEST_ID_SOURCE from "runtime-request-id-source";
import WORKFLOWS_CLIENT_SOURCE from "runtime-workflows-client-source";

export const RUNTIME_INJECTION_SOURCES = Object.freeze({
  d1ClientSource: D1_CLIENT_SOURCE,
  d1DataFieldSource: D1_DATA_FIELD_SOURCE,
  d1ParamsSource: D1_PARAMS_SOURCE,
  sqlSplitterSource: SQL_SPLITTER_SOURCE,
  d1TransportSource: D1_TRANSPORT_SOURCE,
  r2ClientSource: R2_CLIENT_SOURCE,
  r2UtilsSource: R2_UTILS_SOURCE,
  doClientSource: DO_CLIENT_SOURCE,
  doTransportSource: DO_TRANSPORT_SOURCE,
  ownerEndpointSource: OWNER_ENDPOINT_SOURCE,
  ownerHintCacheSource: OWNER_HINT_CACHE_SOURCE,
  requestIdSource: REQUEST_ID_SOURCE,
  workflowsClientSource: WORKFLOWS_CLIENT_SOURCE,
});
