import D1_CLIENT_SOURCE from "runtime-d1-client-source";
import D1_DATA_FIELD_SOURCE from "runtime-d1-data-field-source";
import D1_PARAMS_SOURCE from "runtime-d1-params-source";
import UTF8_SOURCE from "runtime-utf8-source";
import SQL_SPLITTER_SOURCE from "runtime-sql-splitter-source";
import R2_CLIENT_SOURCE from "runtime-r2-client-source";
import R2_UTILS_SOURCE from "runtime-r2-utils-source";
import DO_CLIENT_SOURCE from "runtime-do-client-source";
import DO_SCOPED_REQUEST_SOURCE from "runtime-do-scoped-request-source";
import REQUEST_ID_SOURCE from "runtime-request-id-source";
import WORKFLOWS_CLIENT_SOURCE from "runtime-workflows-client-source";
import AI_CLIENT_SOURCE from "runtime-ai-client-source";

export const RUNTIME_INJECTION_SOURCES = Object.freeze({
  d1ClientSource: D1_CLIENT_SOURCE,
  d1DataFieldSource: D1_DATA_FIELD_SOURCE,
  d1ParamsSource: D1_PARAMS_SOURCE,
  utf8Source: UTF8_SOURCE,
  sqlSplitterSource: SQL_SPLITTER_SOURCE,
  r2ClientSource: R2_CLIENT_SOURCE,
  r2UtilsSource: R2_UTILS_SOURCE,
  doClientSource: DO_CLIENT_SOURCE,
  doScopedRequestSource: DO_SCOPED_REQUEST_SOURCE,
  requestIdSource: REQUEST_ID_SOURCE,
  workflowsClientSource: WORKFLOWS_CLIENT_SOURCE,
  aiClientSource: AI_CLIENT_SOURCE,
});
