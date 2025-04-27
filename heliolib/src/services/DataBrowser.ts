import { Entry } from "../models/Entry.js";
import { DataStore } from "./DataStore.js";
import { didFromUri, getDIDFromATUri, ThreadPostViewDB } from "../utils/bskyutils.js";

import { ApiWrapper } from "./ApiWrapper.js";
import { AppBskyActorDefs, AppBskyFeedDefs, AppBskyFeedPost, ComAtprotoRepoDescribeRepo } from "@atproto/api";
import { ContextService } from "./ContextService.js";
import { ThreadViewPostStrict } from "../models/ThreadViewPostStrict.js";
import { RequestParams } from "../models/RequestParams.js";

export class DataBrowser {
  private ds: DataStore;
  private aw: ApiWrapper;

  private profileCache: Map<string, Entry<AppBskyActorDefs.ProfileViewDetailed>> = new Map();
  private repoCache: Map<string, Entry<ComAtprotoRepoDescribeRepo.OutputSchema>> = new Map();
  private loadedPosts: string = "";
  private contexts: ContextService;

  constructor(ds: DataStore, aw: ApiWrapper, context: ContextService) {
    this.ds = ds;
    this.aw = aw;

    this.contexts = context;
  }

  public async AssureAvatar(profile: Entry<AppBskyActorDefs.ProfileViewDetailed>): Promise<Entry<AppBskyActorDefs.ProfileViewDetailed>> {
    if (profile.record.avatar && profile.record.avatar != "") return profile;
    var avatar = await this.ds.fetchLatestAvatar(profile.record.did);
    if (avatar) {
      profile.record.avatar = avatar;
      return profile;
    } else {
      //go grab one.  sigh.
      let pprofile = await this.aw.GetPublicProfile(profile.record.did);
      if (pprofile && pprofile.record.avatar) {
        profile.record.avatar = pprofile.record.avatar;
      }
    }
    return profile;
    //get latest from DB
  }
  public async GetProfile(did: string): Promise<Entry<AppBskyActorDefs.ProfileViewDetailed> | undefined> {
    let profile = this.profileCache.get(did);
    if (profile) return await this.AssureAvatar(profile);
    let loadprofile = await this.ds.returnAllLatestEntries<AppBskyActorDefs.ProfileViewDetailed>("profile", did, false);
    console.log("Caching Profile: " + did);
    //console.log(loadprofile);
    if (loadprofile.length > 0) {
      profile = await this.AssureAvatar(loadprofile[0]);
      this.profileCache.set(did, profile);
      return profile;
    } else {
      return undefined;
    }
  }

  //async GetThread(uri: string) {}
  //get all posts
  async GetPosts(did: string, snapshotset: string = ""): Promise<Map<string, AppBskyFeedDefs.PostView> | undefined> {
    //const entryDBs = await this.ds.fetch("entry", );
    let context = await this.contexts.GetContext(this.contexts.selectedContext);
    if (this.loadedPosts != did) {
      this.loadedPosts = did;
      const entryDBs = await this.ds.returnAllLatestEntries<AppBskyFeedPost.Record>("post", did);
      let data = context?.posts;

      if (context && data) {
        for (let entry of entryDBs) {
          data?.set(entry.identity, await this.BuildPostView(entry));
        }
        context.posts = data;
        context.resultset = data;
      }
    }
    return context?.posts;
  }
  async ListPosts(did: string): Promise<AppBskyFeedDefs.PostView[]> {
    let context = await this.contexts.GetContext(this.contexts.selectedContext);
    let entries: AppBskyFeedDefs.PostView[] = [];
    if (context) {
      for (const entry of context.posts) {
        const postdid = getDIDFromATUri(entry[0]);
        if (postdid == did) {
          entries.push(entry[1]);
        }
      }
    }
    return entries;
  }

  async GetRepoDescription(did: string, populate: boolean = true): Promise<Entry<ComAtprotoRepoDescribeRepo.OutputSchema> | undefined> {
    let repoDesc = this.repoCache.get(did);
    if (!repoDesc) {
      const repoDescResult = await this.ds.returnAllLatestEntries<ComAtprotoRepoDescribeRepo.OutputSchema>("repo_description", did);
      if (repoDescResult && repoDescResult.length > 0) {
        this.repoCache.set(did, repoDescResult[0]);
        return repoDescResult[0];
      } else if (populate) {
        repoDesc = await this.aw.GetRepoDescription(did);
        if (repoDesc) this.repoCache.set(did, repoDesc);
        return repoDesc;
      }
    } else {
      return repoDesc;
    }
    return undefined;
  }
  //get single post
  async GetPost(aturl: string, snapshotset: string = ""): Promise<AppBskyFeedDefs.PostView | undefined> {
    let context = await this.contexts.GetContext(this.contexts.selectedContext);
    var record: AppBskyFeedDefs.PostView | undefined;
    if (context) {
      record = context.posts.get(aturl);
    }
    //either the context doesn't exist, or it wasn't in it.
    if (!record) {
      var entry = await this.ds.fetchEntry<AppBskyFeedPost.Record>("post", aturl);
      if (entry) record = await this.BuildPostView(entry);
    }
    if (record) return record;
  }
  async BuildPostView(entry: Entry<AppBskyFeedPost.Record>) {
    let did = didFromUri(entry.identity);
    let author: AppBskyActorDefs.ProfileViewBasic = {
      did: did,
      handle: "",
      displayName: "",
      avatar: "",
    };
    let profile = await this.GetProfile(did);
    if (profile == undefined) {
      //while this does return, we want to also make sure it ends up in the databrowswer cache
      await this.aw.GetPublicProfile(did);
      profile = await this.GetProfile(did);
    }
    if (profile) {
      profile = await this.AssureAvatar(profile);
      author.avatar = profile.record.avatar ? profile.record.avatar : "";
      author.did = did;
      author.displayName = profile.record.displayName ? profile.record.displayName : "";
      author.handle = profile.record.handle;
      author.labels = profile.record.labels;
    }
    let postview: AppBskyFeedDefs.PostView = {
      uri: entry.identity,
      cid: "",
      record: entry.record,
      indexedAt: "",
      author: author,
    };
    return postview;
  }
  //get a thread stored by aturl - how do we want to load this?
  async GetThread(aturl: string, params: RequestParams, force: boolean = false): Promise<ThreadViewPostStrict | undefined> {
    //check DB for thread entry where is_root=true at this aturl
    //todo: don't reload thread if we're just changing focus
    params.debugOutput && console.log("DataBrowser:GetThread - aturl: " + aturl);
    let exists = await this.ds.searchById("thread_post_view", aturl, { field: "is_root", value: true });

    if (exists.length == 0 || force) {
      await this.aw.GetThread(aturl, params);
      exists = await this.ds.searchById("thread_post_view", aturl, { field: "is_root", value: true });
    }
    params.debugOutput && console.log("DataBrowser:GetThread - Exists: " + exists.length);

    if (exists && exists.length > 0) {
      let root_uri = (exists[0].data as ThreadPostViewDB).root_uri;
      params.debugOutput && console.log("DataBrowser:root uri:" + root_uri);
      var entries = await this.ds.returnAllLatestEntriesForThread(root_uri);
      let context = await this.contexts.GetContext(this.contexts.selectedContext);
      if (context) {
        context.threadCache = new Map();
        context.threadFocus = new Map();
        //load every entry in entire thread into cache
        params.debugOutput && console.log("DataBrowser:ENTRIES");
        params.debugOutput && console.log(JSON.stringify(entries, null, 2));
        for (var entry of entries) {
          //get did from key
          params.debugOutput && console.log("DataBrowser:Thread View Post Strict");
          let post = await this.BuildPostView(entry);
          let tvps: ThreadViewPostStrict = {
            post: post,
            record: entry.record,
            replies: [],
            parent: undefined,
          };
          params.debugOutput && console.log("DataBrowser:TVPS Skeleton");
          params.debugOutput && console.log(tvps);
          params.debugOutput && console.log("DataBrowser:Adding to thread cache: " + entry.identity);
          context.threadCache.set(entry.identity, tvps);
          var par = entry?.record.reply?.parent?.uri || "No Parent";
          params.debugOutput && console.log("DataBrowser:Cache Result: " + par + " from " + entry.identity);
          params.debugOutput && console.log(context.threadCache.get(par));
          params.debugOutput && console.log(context.threadCache.keys());
        }
        for (var entry of entries) {
          //wire up appropriately
          params.debugOutput && console.log("DataBrowser:Wiring Thread View Post Strict");

          if (entry.record.reply) {
            //find the entry, wire parent and child
            params.debugOutput && console.log("DataBrowser:Replies found for " + entry.identity);
            let parent = context.threadCache.get(entry.record.reply.parent.uri);
            if (!parent && !force) {
              params.debugOutput && console.log("DataBrowser:Parent not found for " + entry.record.reply.parent.uri + " - " + parent);
              //we should download it if we don't have it, but shouldn't it be our current post?
              //This likely means that the original post was downloaded as a single post, rather than a thread.
              //Lets force it
              params.debugOutput && console.log("DataBrowser:Force GetThread for " + aturl);
              await this.GetThread(aturl, params, true);
              params.debugOutput && console.log("DataBrowser:Force GetThread Complete for " + aturl);
            }
            let current = context.threadCache.get(entry.identity);
            if (!current) params.debugOutput && console.log("DataBrowser:Current not found for " + entry.identity);
            if (parent && current) {
              parent.replies.push(current);
              current.parent = parent;
            }
          }
        }
        let focus = context.threadCache.get(aturl);
        if (focus) {
          params.debugOutput && console.log("DataBrowser:Focus Post Selected - Building");
          params.debugOutput && console.log(focus);
          let focusClone = this.BuildFocusPost(focus, focus, "root", params);
          params.debugOutput && console.log("DataBrowser:Focus Post Selected - Clone");
          params.debugOutput && console.log(focus);
          context.threadFocus.set(aturl, focusClone);
        }
        return context.threadFocus.get(aturl);
        //todo, sort replies by date
      }
    }
  }
  //build a focus post from a thread post, going up and down the tree - not the root, but the post in the thread of interest
  BuildFocusPost(
    atPost: ThreadViewPostStrict,
    lastAt: ThreadViewPostStrict | undefined,
    dir: String,
    params: RequestParams
  ): ThreadViewPostStrict {
    if (dir == "root") {
      //shallow clone the focus object
      let rootClone: ThreadViewPostStrict = { ...atPost };
      //go up and down
      if (rootClone.parent) {
        params.debugOutput && console.log("DataBrowser:Focus Post:Parent found, going up");
        this.BuildFocusPost(rootClone.parent, rootClone, "up", params);
      }
      if (rootClone.replies.length > 0) {
        //"move" reply array
        let replies: ThreadViewPostStrict[] = rootClone.replies;
        //new object
        rootClone.replies = [];
        for (let child of replies) {
          params.debugOutput && console.log("DataBrowser:Focus Post:Child found, going down");
          this.BuildFocusPost(child, rootClone, "down", params);
        }
      }
      return rootClone;
    } else if (dir == "up") {
      params.debugOutput && console.log("DataBrowser:Focus Post:Processing Parent");
      //clone the parent, remove replies
      let parentClone: ThreadViewPostStrict = { ...atPost, replies: [] };
      //reset child parent to this one
      if (lastAt) lastAt.parent = parentClone;
      lastAt = parentClone;
      if (parentClone.parent) this.BuildFocusPost(parentClone.parent, parentClone, "up", params);
      return parentClone;
    } else if (dir == "down") {
      params.debugOutput && console.log("DataBrowser:Focus Post:Processing Child");
      let childClone: ThreadViewPostStrict = { ...atPost, parent: undefined, replies: [] };
      if (lastAt) lastAt.replies.push(childClone);

      let replies: ThreadViewPostStrict[] = atPost.replies;
      //atPost.replies = [];
      for (let child of replies) {
        params.debugOutput && console.log("DataBrowser:Focus Post:Child:Child found, going down");
        this.BuildFocusPost(child, childClone, "down", params);
      }
      return childClone;
    }
    return atPost;
  }
}
