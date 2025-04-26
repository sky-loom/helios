import { AppBskyFeedDefs } from "@atproto/api";
import { ThreadViewPostStrict } from "../models/ThreadViewPostStrict.js";
import { Index as FlexSearchIndex } from "flexsearch";

export interface Context {
  name: string;
  posts: Map<string, AppBskyFeedDefs.PostView>;
  index: FlexSearchIndex;
  resultset: Map<string, AppBskyFeedDefs.PostView>;
  threadCache: Map<string, ThreadViewPostStrict>;
  threadFocus: Map<string, ThreadViewPostStrict>;
  labelers: string[]; // Array of labelers for this context
}
