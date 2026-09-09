// Keep real ESM named exports explicit. Node cannot reliably infer names from
// `module.exports = nativeBinding`, which is the correct CJS compatibility
// shape for napi-rs, so consumers such as jazz-tools must not depend on its
// static CJS-export heuristic.
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const nativeBinding = require("./index.cjs");

export default nativeBinding;
export const {
  JazzServer,
  NapiDb,
  StreamingMutation,
  Subscription,
  TestJwtIssuer,
  Transport,
  Write,
  mintLocalFirstToken,
  verifyLocalFirstIdentityProof,
  nativeArtifactFingerprint,
} = nativeBinding;
