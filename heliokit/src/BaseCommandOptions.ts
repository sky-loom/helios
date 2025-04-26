import { RequestParams } from "@skyloom/helios";
import { nanoid } from "nanoid";

export interface BaseCommandOptions {
  output?: string; // JSON output file path
  dryRun?: boolean; // Don't store to database
  context?: string; // Context to use
  snapshot?: string; // Snapshot name
  debug?: boolean; // Enable debug output
  labelers?: string[]; // Array of labelers to use
}

export function toRequestParams(options: BaseCommandOptions): RequestParams {
  const params = new RequestParams();

  if (options.snapshot) {
    params.snapshotSet = options.snapshot;
  } else {
    params.snapshotSet = nanoid();
  }

  if (options.debug) {
    params.debugOutput = true;
  }

  if (options.dryRun) {
    params.dryRun = true;
  }
  if (options.labelers) {
    params.labelers = options.labelers;
  }

  return params;
}
