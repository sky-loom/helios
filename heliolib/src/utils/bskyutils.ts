import { CommitCreateEvent } from "@skyware/jetstream";
import { BskyClientManager } from "@skyloom/blueskyclientmanager";
import { AppBskyFeedDefs, AppBskyFeedPost, ComAtprotoRepoDescribeRepo, ComAtprotoRepoStrongRef } from "@atproto/api";
import { ThreadViewPostStrict } from "../models/ThreadViewPostStrict.js";
import { RequestParams } from "../models/RequestParams.js";
export function isReply(record: AppBskyFeedPost.Record) {
  if (record.reply) return true;
  return false;
}
export function SafeDid(did: string) {
  return did.replace("did:plc:", "did-plc-").replace("did:web:", "did-web-");
}
export function getReplyData(event: CommitCreateEvent<"app.bsky.feed.post">) {
  return event.commit.record.reply;
}

export function didFromUri(uri: string): string {
  // Split the string by slashes
  const parts = uri.split("/");

  // Check if the structure is valid and has the expected parts
  //if (parts.length > 2 && parts[0] === "at:" && parts[1] === "") {
  return parts[2]; // The `did` is in the third part (index 2)
  //}

  //return null; // Return null if the input doesn't match the expected structure
}
//at://${event.did}/app.bsky.feed.post/${event.commit.rkey}
export function rkeyFromUri(input: string): string {
  const segments = input.split("/");
  return segments[4];
}

function isSameReply(reply1: AppBskyFeedPost.ReplyRef, reply2: AppBskyFeedPost.ReplyRef): boolean {
  // Check if both 'parent' and 'root' are objects in both replies
  if (
    typeof reply1.parent !== "object" ||
    typeof reply2.parent !== "object" ||
    typeof reply1.root !== "object" ||
    typeof reply2.root !== "object"
  ) {
    return false;
  }

  // Compare 'cid' and 'uri' of 'parent' and 'root' in both replies
  return (
    reply1.parent.cid === reply2.parent.cid &&
    reply1.parent.uri === reply2.parent.uri &&
    reply1.root.cid === reply2.root.cid &&
    reply1.root.uri === reply2.root.uri
  );
}
export function PDSFromRepoDesc(repo: ComAtprotoRepoDescribeRepo.OutputSchema): string {
  return (repo.didDoc as { service: any[] })?.service[0].serviceEndpoint;
}
export function getDIDFromATUri(aturi: string | undefined): string | undefined {
  if (aturi == undefined) return undefined;
  return aturi.slice(5, aturi.indexOf("/", 5));
}

export interface ThreadData {
  post: AppBskyFeedDefs.PostView;
  parent: AppBskyFeedDefs.ThreadViewPost;
}
export interface MinThreadViewPost {
  uri: string;
  did: string;
  text: string;
}

export interface ThreadPostViewDB {
  post: string;
  parent: string | undefined;
  is_root: boolean;
  root_uri: string;
  replies: string[];
}
//I hate this so much.
export function ExtractPostView(
  threaddata:
    | AppBskyFeedDefs.ThreadViewPost
    | AppBskyFeedDefs.NotFoundPost
    | AppBskyFeedDefs.BlockedPost
    | { $type: string; [k: string]: unknown },
  params: RequestParams
): ThreadViewPostStrict | undefined {
  params.debugOutput && console.log("ExtractPostView");
  let retData: ThreadViewPostStrict | undefined = undefined;
  if ((threaddata as AppBskyFeedDefs.ThreadViewPost).post) {
    retData = {
      post: (threaddata as AppBskyFeedDefs.ThreadViewPost).post as AppBskyFeedDefs.PostView,
      record: ((threaddata as AppBskyFeedDefs.ThreadViewPost).post as AppBskyFeedDefs.PostView).record as AppBskyFeedPost.Record,
      replies: [],
      parent: undefined,
    };
  }
  params.debugOutput && console.log("ExtractPostView:Processing:" + (retData || "undefined"));
  if (retData?.post) {
    params.debugOutput && console.log("ExtractPostView:AT Uri: " + retData.post.uri);

    let tvpThreadData = threaddata as AppBskyFeedDefs.ThreadViewPost;
    if (tvpThreadData.replies) {
      tvpThreadData.replies.forEach((reply) => {
        params.debugOutput && console.log("ExtractPostView:Reply Type: " + reply?.$type);
        if (reply.$type === "app.bsky.feed.defs#threadViewPost") {
          let replyStrict = ExtractPostView(reply, params);
          if (replyStrict) {
            replyStrict.parent = retData; //iffy on if we should wire this up or not since it can create some recursive gotchas
            retData.replies.push(replyStrict);
          }
        }
      });
    }
    if (tvpThreadData.parent) {
      params.debugOutput && console.log("ExtractPostView:Parent Type: " + tvpThreadData.parent.$type);
      if (tvpThreadData.parent.$type === "app.bsky.feed.defs#threadViewPost") {
        let parentStrict = ExtractPostView(tvpThreadData.parent, params);
        if (parentStrict) retData.parent = parentStrict;
      }
    }
  }
  return retData;
}

//function ExtractMinimalThreadParents(thread: ThreadViewPostStrict) {}
export function ExtractMinimalThreadParents(thread: ThreadViewPostStrict): MinThreadViewPost[] {
  const result: MinThreadViewPost[] = [];

  let current: ThreadViewPostStrict | undefined = thread;

  while (current) {
    result.push({
      uri: current.post.uri,
      did: current.post.author.did,
      text: (current.post.record as AppBskyFeedPost.Record).text,
    });

    current = current.parent; // Move to the parent
  }

  return result;
}

export async function getThread(
  aturi: string,
  bskyClient: BskyClientManager,
  params: RequestParams
): Promise<ThreadViewPostStrict | undefined> {
  let data = await bskyClient.pdsgateway.app.bsky.feed.getPostThread({
    uri: aturi,
  });
  if (data.data.thread.$type === "app.bsky.feed.defs#threadViewPost") {
    return ExtractPostView(data.data.thread, params);
  }
}

export function buildATUriFromEvent(event: CommitCreateEvent<"app.bsky.feed.post">) {
  return `at://${event.did}/app.bsky.feed.post/${event.commit.rkey}`;
}

export function buildATUriFromIds(did: string, rkey: string) {
  return `at://${did}/app.bsky.feed.post/${rkey}`;
}
