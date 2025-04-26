import { AppBskyFeedDefs, AppBskyFeedPost } from "@atproto/api";

export interface ThreadViewPostStrict {
  post: AppBskyFeedDefs.PostView;
  record: AppBskyFeedPost.Record;
  parent: ThreadViewPostStrict | undefined;
  replies: ThreadViewPostStrict[];
}
