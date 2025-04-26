import {
  didFromUri,
  ExtractPostView,
  getDIDFromATUri,
  isReply,
  PDSFromRepoDesc,
  rkeyFromUri,
  ThreadPostViewDB,
} from "../utils/bskyutils.js";
import { BskyClientManager } from "@skyloom/blueskyclientmanager";
import { DataStore } from "./DataStore.js";
import { Entry, EntryDB } from "../models/Entry.js";
import { HeadersMap } from "@atproto/xrpc";
import { getAndParseRecord, readCarWithRoot, verifyRepoCar } from "@atproto/repo";
import { DidPlcResolver, DidResolver, HandleResolver, MemoryCache } from "@atproto/identity";
import path from "path";
import * as fs from "fs";
import { PageResponse } from "../models/PageResponse.js";
import {
  AppBskyActorDefs,
  AppBskyActorProfile,
  AppBskyFeedLike,
  AppBskyFeedPost,
  AppBskyFeedRepost,
  AppBskyGraphBlock,
  AppBskyGraphFollow,
  AppBskyGraphFollowRecord,
  AppBskyGraphList,
  AppBskyGraphListblock,
  AppBskyGraphListblockRecord,
  AppBskyGraphListitem,
  AppBskyGraphListRecord,
  ComAtprotoRepoDescribeRepo,
} from "@atproto/api";
import { ComAtprotoRepoListRecords } from "@atproto/api";
import { ThreadViewPostStrict } from "../models/ThreadViewPostStrict.js";
import { RequestParams } from "../models/RequestParams.js";

export class ApiWrapper {
  private client: BskyClientManager;
  private datastore: DataStore;
  private recorderDid: string;
  //todo: include rate limiting controls
  constructor(client: BskyClientManager, datastore: DataStore, recorder: string) {
    this.client = client;
    this.datastore = datastore;
    this.recorderDid = recorder;
  }
  /**
   * Stores data in the datastore and creates an entry record
   *
   * @private
   * @template T - The type of data being stored
   * @param {string} tableName - The name of the table where data will be stored
   * @param {string} id - The unique identifier for the record
   * @param {T} data - The data to be stored
   * @param {string} snapshotSet - The identifier for the snapshot set this data belongs to
   * @returns {Promise<Entry<T>>} A wrapped entry containing the stored data and metadata
   */
  private async Store<T>(tableName: string, id: string, data: T, requestParams: RequestParams): Promise<Entry<T>> {
    //create the skeleton entry
    const entrydb = new EntryDB(id, "", Date.now(), this.recorderDid, id, tableName);

    //save skeleton entry
    if (!requestParams.dryRun) {
      !requestParams.dryRun && (await this.datastore.save(tableName, id, data as object, requestParams.snapshotSet));
      await this.datastore.save("entry", entrydb.identity, entrydb, requestParams.snapshotSet);
      return Entry.create<T>(entrydb, data);
    }
    //create full entry
    //dry run, so just return the entry without saving
    return new Entry<T>(entrydb, data);
  }
  /**
   * Retrieves a profile if it doesn't exist in the datastore
   *
   * @private
   * @param {string | undefined} did - The Decentralized Identifier of the profile to fill
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<void>}
   */
  private async FillProfile(did: string | undefined, params: RequestParams = new RequestParams()) {
    if (did) {
      let profile = await this.datastore.fetch<AppBskyActorDefs.ProfileViewDetailed>("profile", did);
      if (!profile) await this.GetProfile(did, params);
    }
  }
  /**
   * Retrieves followers for a specified account with pagination support
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<PageResponse<Entry<AppBskyActorDefs.ProfileViewDetailed>>>} Paginated list of followers' profiles
   */
  async GetFollowers(
    did: string,
    params: RequestParams = new RequestParams()
  ): Promise<PageResponse<Entry<AppBskyActorDefs.ProfileViewDetailed>>> {
    //need to carry rate limits into this and also return cursor somehow if needed

    let hasNextPage = true;
    let atPage = 0;
    let profiles: AppBskyActorDefs.ProfileView[] = [];
    let response: PageResponse<Entry<AppBskyActorDefs.ProfileViewDetailed>> = new PageResponse<
      Entry<AppBskyActorDefs.ProfileViewDetailed>
    >();
    response.cursor = params.cursor;
    AppBskyGraphFollowRecord;
    while (hasNextPage && atPage < params.pageCount) {
      atPage++;
      const { data } = await this.client.appview.getFollowers({
        actor: did,
        limit: 100,
        cursor: response.cursor,
      });
      const { followers, cursor } = data;
      for (let f of followers) {
        let pd = f as AppBskyActorDefs.ProfileViewDetailed;
        let epd = await this.Store<AppBskyActorDefs.ProfileViewDetailed>("profile", pd.did, pd, params);
        response.data.push(epd);
      }

      console.log(`Follower page - ${response.cursor} of page ${atPage}/${params.pageCount}`);
      //console.log(followers);
      if (cursor) {
        response.cursor = cursor; // Set cursor for the next request
      } else {
        hasNextPage = false; // No more pages
        response.cursor = undefined;
      }
    }
    return response;
  }

  /**
   * Retrieves all accounts a user follows directly from their PDS
   *
   * Fetches raw follow records and stores them in optimized relational format
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<PageResponse<Entry<AppBskyGraphFollow.Record>>>} Paginated list of follow records
   */
  async GetFollows(did: string, params: RequestParams = new RequestParams()): Promise<PageResponse<Entry<AppBskyGraphFollow.Record>>> {
    // Get repository description to determine PDS endpoint
    let repodesc = await this.datastore.fetch<ComAtprotoRepoDescribeRepo.OutputSchema>("repo_description", did);
    if (!repodesc) repodesc = (await this.GetRepoDescription(did)).record;

    const pds = PDSFromRepoDesc(repodesc);
    params.debugOutput && console.log(`Fetching follows from PDS: ${pds} for user ${did}`);

    // Initialize response
    let response = new PageResponse<Entry<AppBskyGraphFollow.Record>>();
    response.data = [];
    response.cursor = params.cursor;

    let atPage = 0;
    let hasNextPage = true;

    while (hasNextPage && atPage < params.pageCount) {
      atPage++;

      try {
        // Fetch follow records from PDS
        const result = await this.client.pdsagents.getAgent(pds).com.atproto.repo.listRecords({
          repo: did,
          collection: "app.bsky.graph.follow",
          limit: 100,
          cursor: response.cursor,
        });

        if (result.data.records) {
          for (const record of result.data.records) {
            const followRecord = record.value as AppBskyGraphFollow.Record;
            const followUri = `at://${did}/app.bsky.graph.follow/${followRecord.rkey}`;

            // Store the follow record using existing method
            const entry = await this.Store<AppBskyGraphFollow.Record>("follow", followUri, followRecord, params);

            // Add to response
            response.data.push(entry);

            // Store the relationship in the optimized table
            await this.datastore.storeFollowRelationship(
              did, // follower DID
              followRecord.subject, // followed DID
              followUri, // the follow record URI
              params.snapshotSet // snapshot set
            );

            // Optionally fetch and store the profile
            if (params.fillDetails && followRecord.subject) {
              await this.GetProfile(followRecord.subject, params);
            }
          }
        }

        params.debugOutput && console.log(`Follow page ${atPage}/${params.pageCount}, fetched ${result.data.records?.length || 0} records`);

        if (result.data.cursor) {
          response.cursor = result.data.cursor;
        } else {
          hasNextPage = false;
          response.cursor = undefined;
        }
      } catch (error) {
        console.error(`Error fetching follows for ${did}:`, error);
        hasNextPage = false;
      }
    }

    return response;
  }
  /**
   * Fetches a user's public profile using the public agent API
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Entry<AppBskyActorDefs.ProfileViewDetailed>>} The retrieved profile wrapped in an Entry
   */
  async GetPublicProfile(did: string, params: RequestParams = new RequestParams()): Promise<Entry<AppBskyActorDefs.ProfileViewDetailed>> {
    //get the profile
    const data = await this.client.publicagent.getProfile({ actor: did });
    if (params.fillDetails) {
      await this.GetRepoDescription(did);
    }
    return await this.Store<AppBskyActorDefs.ProfileViewDetailed>("profile", data.data.did, data.data, params);
  }
  /**
   * Fetches a user's profile using the PDS gateway
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Entry<AppBskyActorDefs.ProfileViewDetailed>>} The retrieved profile wrapped in an Entry
   */
  async GetProfile(did: string, params: RequestParams = new RequestParams()): Promise<Entry<AppBskyActorDefs.ProfileViewDetailed>> {
    //get the profile
    const data = await this.client.pdsgateway.getProfile({ actor: did });
    if (params.fillDetails) {
      await this.GetRepoDescription(did);
    }
    return await this.Store<AppBskyActorDefs.ProfileViewDetailed>("profile", data.data.did, data.data, params);
  }
  /**
   * Retrieves a specific post by its URI
   *
   * @param {string} uri - The URI of the post
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Entry<AppBskyFeedPost.Record>>} The retrieved post wrapped in an Entry
   */
  async GetPost(uri: string, params: RequestParams = new RequestParams()): Promise<Entry<AppBskyFeedPost.Record>> {
    const did = didFromUri(uri);
    const rkey = rkeyFromUri(uri);
    const data = await this.client.pdsgateway.getPost({
      rkey: rkey,
      repo: did,
    });
    let record = await this.Store<AppBskyFeedPost.Record>("post", uri, data.value, params);
    if (params.fillDetails) {
      await this.FillProfile(did);
      if (isReply(record.record)) {
        await this.FillProfile(getDIDFromATUri(record.record.reply?.parent.uri));
        await this.FillProfile(getDIDFromATUri(record.record.reply?.root.uri));
      }
    }
    return record;
  }
  /**
   * Gets the repository description for a specified DID
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Entry<ComAtprotoRepoDescribeRepo.OutputSchema>>} The repository description wrapped in an Entry
   */
  async GetRepoDescription(did: string, params: RequestParams = new RequestParams()) {
    var repo = await this.client.pdsgateway.com.atproto.repo.describeRepo({
      repo: did,
    });

    return await this.Store<ComAtprotoRepoDescribeRepo.OutputSchema>("repo_description", did, repo.data, params);
  }
  /**
   * Retrieves and populates profiles of accounts followed by a user
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @returns {Promise<void>}
   */
  async PopulateFollows(did: string) {
    //get follows form DB, get all their profiles
    //if params say to get more, start downloading repos? or ?  I dunno.
    //we should have an N level and a Max or Filter of some kind so we don't hit bot/follow farm accounts
  }
  /**
   * Retrieves a conversation thread for a post, including parent and child posts
   *
   * @param {string} uri - The URI of the post
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<HeadersMap>} Response headers containing rate limit and other information
   */
  async GetThread(uri: string, params: RequestParams = new RequestParams()): Promise<HeadersMap> {
    //for this request, should we use a user context or bypass?
    //get this post
    var res = await this.client.publicagent.app.bsky.feed.getPostThread({ uri: uri, depth: 1000, parentHeight: 1000 });
    //console.log(res);
    var postView = ExtractPostView(res.data.thread);
    console.log("PostView: " + JSON.stringify(postView, null, 2));
    let rooturi: string | undefined;
    if (postView?.parent) {
      rooturi = postView.parent.record.reply?.root.uri;
    } else {
      rooturi = postView?.post.uri;
    }
    //call bksy getthread
    //console.log(JSON.stringify(res.data.thread, null, 2));
    if (postView) await this.StorePostRecurse(postView, "root", rooturi ? rooturi : "", params);
    return res.headers;
  }
  /**
   * Recursively stores posts from a thread, including parent and reply posts
   *
   * @param {ThreadViewPostStrict} post - The post view to store
   * @param {string} [direction="root"] - Direction of traversal ("root", "up", or "down")
   * @param {string} root_uri - The URI of the root post in the thread
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<void>}
   */
  async StorePostRecurse(
    post: ThreadViewPostStrict,
    direction: string = "root",
    root_uri: string,
    params: RequestParams = new RequestParams()
  ) {
    //let item = parsed.record as AtPostRecord;
    console.log("Recursively storing thread posts: " + post.post.uri);
    //console.log(post);
    let record = await this.Store<AppBskyFeedPost.Record>("post", post.post.uri, post.post.record as AppBskyFeedPost.Record, params);
    //store author info
    let did = getDIDFromATUri(post.post.uri);
    if (did) {
      let children: string[] = [];
      if (post.parent && (direction == "root" || direction == "up")) {
        await this.StorePostRecurse(post.parent, "up", root_uri);
      }
      if (post.replies && (direction == "root" || direction == "down")) {
        for (const rep of post.replies) {
          await this.StorePostRecurse(rep, "down", root_uri);
          children.push(rep.post.uri);
        }
      }

      let authrecord = await this.Store<AppBskyActorDefs.ProfileViewDetailed>(
        "profile",
        did,
        post.post.author as AppBskyActorDefs.ProfileViewDetailed,
        params
      );
      let threaddata: ThreadPostViewDB = {
        post: post.post.uri,
        parent: post.parent?.post.uri,
        replies: children,
        is_root: direction == "root" ? true : false,
        root_uri: root_uri,
      };
      let threadrecord = await this.Store<ThreadPostViewDB>("thread_post_view", threaddata.post, threaddata, params);
    }
  }
  /**
   * Retrieves an entire conversation related to a post, from root to all children
   *
   * @param {string} uri - The URI of the post
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<void>}
   */
  async GetConversation(uri: string, params: RequestParams = new RequestParams()) {
    //get post from uri
    //if this has a reply, call GetThread on the root, if it does not, call getthread on this item
  }

  // private async RateLimit(headers: HeadersMap) {
  //   const remaining = parseInt(headers["ratelimit-remaining"]);
  //   const reset = parseInt(headers["ratelimit-reset"]);
  //   const limit = parseInt(headers["ratelimit-limit"]);
  //   if (remaining <= 0) {
  //     const now = Date.now(); // Get current timestamp in seconds
  //     const waitTime = reset - now;
  //     if (waitTime > 0) {
  //       console.log(`Rate limit reached. Waiting for ${waitTime / 1000} seconds...`);
  //       await this.sleep(waitTime); // Convert to milliseconds
  //     }
  //   }
  // }
  /**
   * Utility method to pause execution for a specified duration
   *
   * @private
   * @param {number} ms - The time to sleep in milliseconds
   * @returns {Promise<void>}
   */
  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
  /**
   * Resolves a Bluesky handle to a DID, caching the result
   *
   * @param {string} handle - The user handle to resolve
   * @returns {Promise<string | undefined>} The resolved DID or undefined if not found
   */
  async GetDidFromHandle(handle: string): Promise<string | undefined> {
    let did = await this.datastore.fetchDID(handle);
    //console.log("GetDidFromHandle: " + handle + " => " + did);
    if (!did) {
      const didCache = new MemoryCache();
      const resolver = new HandleResolver();
      did = await resolver.resolve(handle);
      if (did) {
        //have a legit handle/DID, lets store it in case we need it again later
        await this.GetRepoDescription(did);
      }
      return did;
    }
    return did;
  }
  /**
   * Fetches the block lists a user has subscribed to
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Entry<ComAtprotoRepoListRecords.Record[]>>} The subscribed block lists wrapped in an Entry
   */
  async GetSubcribedBlockLists(did: string, params: RequestParams = new RequestParams()) {
    //get pds entry, will auto-fill from describe repo if not exists
    let repodesc = await this.datastore.fetch<ComAtprotoRepoDescribeRepo.OutputSchema>("repo_description", did);
    if (!repodesc) repodesc = (await this.GetRepoDescription(did)).record;
    let pds = PDSFromRepoDesc(repodesc);
    let records: ComAtprotoRepoListRecords.Record[] = [];

    let cursor: string | undefined = undefined;
    let headers: HeadersMap | undefined;

    do {
      // if (headers) {
      //   await this.RateLimit(headers);
      // }
      const result = await this.client.pdsagents.getAgent(pds).com.atproto.repo.listRecords({
        repo: did,
        collection: "app.bsky.graph.listblock",
      });
      headers = result.headers;
      if (result.data.records) records.push(...result.data.records);
      cursor = result.data.cursor;
    } while (cursor);

    if (params.fillDetails) {
      for (const lst of records) {
        //await this.GetBlockList(lst.uri, params);
      }
    }
    return await this.Store<ComAtprotoRepoListRecords.Record[]>("subscribed_blocklists", did, records, params);
  }
  /* 
  export class BlobRef {
  public original: JsonBlobRef

  constructor(
    public ref: CID,
    public mimeType: string,
    public size: number,
    original?: JsonBlobRef,
  ) {
    this.original = original ?? {
      $type: 'blob',
      ref,
      mimeType,
      size,
    }
  }*/
  /**
   * Retrieves binary image data by its Content Identifier
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {string} cid - The Content Identifier of the image
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Uint8Array>} The binary image data
   */
  async GetImageBlob(did: string, cid: string, params: RequestParams = new RequestParams()): Promise<Uint8Array> {
    console.log("did: " + did);
    console.log("cid: " + cid);
    let repodesc = await this.datastore.fetch<ComAtprotoRepoDescribeRepo.OutputSchema>("repo_description", did);
    if (!repodesc) repodesc = (await this.GetRepoDescription(did)).record;
    let pds = PDSFromRepoDesc(repodesc);
    var result = await this.client.pdsagents.getAgent(pds).com.atproto.sync.getBlob({ cid: cid, did: did });
    console.log(result);
    //yay, do. things.
    return result.data;
  }
  // async GetBlockList(uri: string, params: RequestParams = new RequestParams()) {
  //   let did = getDIDFromATUri(uri);
  //   if (did) {
  //     let repodesc = await this.datastore.fetch<RepoDescription>("repo_description", did);
  //     if (!repodesc) repodesc = (await this.GetRepoDescription(did)).record;
  //     let pds = PDSFromRepoDesc(repodesc);
  //     let records: BlockListRecord[] = [];

  //     let cursor: string | undefined = undefined;
  //     let headers: HeadersMap | undefined;
  //     do {
  //       if (headers) {
  //         await this.RateLimit(headers);
  //       }
  //       const result = await this.client.pdsagents.getAgent(pds).com.atproto.repo.listRecords({
  //         repo: did,
  //         collection: "app.bsky.graph.list",
  //       });
  //       headers = result.headers;
  //       if (result.data.records) records.push(...(result.data.records as BlockListRecord[]));
  //       cursor = result.data.cursor;
  //     } while (cursor);

  //     if (params.fillDetails) {
  //       for (const lst of records) {
  //         await this.GetBlockList(lst.value.uri, params);
  //       }
  //     }
  //   }
  // }
  async GetUserBlocks(did: string, params: RequestParams = new RequestParams()) {}
  //from all individual blocks and block lists, compose into a single view
  async GetResolvedBlocks(did: string) {}
  async GetDIDHistory(did: string, params: RequestParams = new RequestParams()) {}

  /**
   * Retrieves a post from a Bluesky URL
   *
   * Parses a standard Bluesky URL (e.g., https://bsky.app/profile/handle/post/identifier)
   * to extract the handle and post ID, then resolves the handle to a DID and fetches the post
   *
   * @param {string} url - The Bluesky URL (e.g., https://bsky.app/profile/handle/post/identifier)
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<Entry<AppBskyFeedPost.Record> | undefined>} The retrieved post wrapped in an Entry, or undefined if not found
   */
  async GetPostFromURL(url: string, params: RequestParams = new RequestParams()): Promise<Entry<AppBskyFeedPost.Record> | undefined> {
    // Parse URL to extract handle and post identifier
    // Example URL: https://bsky.app/profile/holiday.reskeet.me/post/3lcqmf5vcpk2w

    try {
      // Check if it's a valid Bluesky URL
      const urlObj = new URL(url);
      if (!urlObj.hostname.includes("bsky.app") || !urlObj.pathname.startsWith("/profile/")) {
        throw new Error("Not a valid Bluesky URL");
      }

      // Extract handle and post identifier from the URL path
      const pathParts = urlObj.pathname.split("/");
      if (pathParts.length < 5 || pathParts[3] !== "post") {
        throw new Error("Invalid Bluesky post URL format");
      }

      const handle = pathParts[2];
      const postId = pathParts[4];

      // Translate handle to DID
      params.debugOutput && console.log(`Resolving handle ${handle} to DID`);
      const did = await this.GetDidFromHandle(handle);

      if (!did) {
        throw new Error(`Could not resolve handle: ${handle}`);
      }

      // Construct the post URI
      const postUri = `at://${did}/app.bsky.feed.post/${postId}`;
      params.debugOutput && console.log(`Fetching post with URI: ${postUri}`);

      // Fetch the post using the constructed URI
      return await this.GetPost(postUri, params);
    } catch (error) {
      console.error("Error retrieving post from URL:", error);
      return undefined;
    }
  }
  /**
   * Downloads and processes an entire repository from a user's PDS
   *
   * Stores various record types including posts, likes, follows, blocks, profiles, etc.
   *
   * @param {string} did - The Decentralized Identifier of the account
   * @param {RequestParams} [params=new RequestParams()] - Parameters controlling the request behavior
   * @returns {Promise<void>}
   */
  async DownloadAndStorePDS(did: string, params: RequestParams = new RequestParams()) {
    let pds = "";
    params.debugOutput && console.log(params);
    try {
      let repodesc = await this.datastore.fetch<ComAtprotoRepoDescribeRepo.OutputSchema>("repo_description", did);
      if (!repodesc) repodesc = (await this.GetRepoDescription(did)).record;
      pds = PDSFromRepoDesc(repodesc);
    } catch (err) {
      let repodesc = (await this.GetRepoDescription(did)).record;
      pds = PDSFromRepoDesc(repodesc);
    }
    const didCache = new MemoryCache();
    const resolver = new DidResolver({
      didCache: didCache,
    });
    const didDoc = await resolver.resolve(did);
    var handle = "";
    if (didDoc && didDoc.alsoKnownAs && didDoc.alsoKnownAs.length > 0) handle = didDoc.alsoKnownAs[0].replace("at://", "");

    params.debugOutput && console.log("Handle: " + handle);

    params.debugOutput && console.log("Downloading PDS Repo..." + did + " from " + pds);
    const repoResponse = await this.client.pdsagents.getAgent(pds).com.atproto.sync.getRepo({
      did,
    });
    params.debugOutput && console.log(repoResponse.headers);

    const repoBuffer = Buffer.from(repoResponse.data);
    let safedid = did.replace("did:plc:", "did-plc-").replace("did:web:", "did-web-");
    fs.writeFileSync(path.join("./", handle + "-" + safedid + "-repo.car"), repoBuffer);

    const car = await readCarWithRoot(repoBuffer);

    const repo = await verifyRepoCar(repoBuffer);
    const collections = new Set<string>();
    const lists = new Map<string, AppBskyGraphListitem.Record[]>();

    for (const write of repo.creates) {
      const parsed = await getAndParseRecord(car.blocks, write.cid);
      let rkey = write.rkey;
      let uri = `at://${did}/${write.collection}/${rkey}`;
      collections.add(write.collection);
      //console.log(uri);
      switch (write.collection) {
        case "app.bsky.graph.listitem":
          {
            let item = parsed.record as AppBskyGraphListitem.Record;
            //let record = await this.Store<ListItem>("list_item", uri, item);
            if (!lists.has(item.list)) {
              lists.set(item.list, []);
            }
            var list = lists.get(item.list);
            if (list) {
              list?.push(item);
              lists.set(item.list, list);
            }
          }
          break;
        case "app.bsky.feed.like":
          {
            let item = parsed.record as AppBskyFeedLike.Record;
            let record = await this.Store<AppBskyFeedLike.Record>("likes", uri, item, params);
          }
          break;
        case "app.bsky.feed.post":
          {
            let item = parsed.record as AppBskyFeedPost.Record;
            let record = await this.Store<AppBskyFeedPost.Record>("post", uri, item, params);
          }
          break;
        case "app.bsky.feed.repost":
          {
            let item = parsed.record as AppBskyFeedRepost.Record;
            let record = await this.Store<AppBskyFeedRepost.Record>("repost", uri, item, params);
          }
          break;
        case "app.bsky.graph.block":
          {
            let item = parsed.record as AppBskyGraphBlock.Record;
            let record = await this.Store<AppBskyGraphBlock.Record>("block", uri, item, params);
          }
          break;
        case "app.bsky.graph.follow":
          {
            let item = parsed.record as AppBskyGraphFollow.Record;
            let record = await this.Store<AppBskyGraphFollow.Record>("follow", uri, item, params);
          }
          break;
        case "app.bsky.graph.list":
          {
            let item = parsed.record as AppBskyGraphList.Record; // ListRecord
            let record = await this.Store<AppBskyGraphList.Record>("list", uri, item, params);
          }
          break;
        case "app.bsky.graph.listblock": //flags a list as one that is blocked
          {
            let item = parsed.record as AppBskyGraphListblock.Record; // ListBlockRecord
            let record = await this.Store<AppBskyGraphListblock.Record>("listblock", uri, item, params);
          }
          break;
        case "app.bsky.actor.profile":
          {
            if (didDoc && didDoc.alsoKnownAs) {
              let item = this.RepoActorProfileRecordToAppViewProfile(parsed.record as AppBskyActorProfile.Record, did, handle);
              let record = await this.Store<AppBskyActorDefs.ProfileViewDetailed>("profile", did, item, params);
            }
          }
          break;

        default:
          params.debugOutput && console.log("ERROR: " + write.collection);
      }
    }

    for (const [key, list] of lists) {
      let record = await this.Store<AppBskyGraphListitem.Record[]>("list_value", key, list, params);
    }
    params.debugOutput && console.log(collections);
  }
  /**
   * Converts a repository actor profile record to an app view profile format
   *
   * @param {AppBskyActorProfile.Record} actorProfile - The actor profile record from the repository
   * @param {string} did - The Decentralized Identifier of the account
   * @param {string} handle - The user handle
   * @returns {AppBskyActorDefs.ProfileViewDetailed} The converted profile in app view format
   */
  RepoActorProfileRecordToAppViewProfile(
    actorProfile: AppBskyActorProfile.Record,
    did: string,
    handle: string
  ): AppBskyActorDefs.ProfileViewDetailed {
    return {
      did,
      handle,
      displayName: actorProfile.displayName,
      description: actorProfile.description,
      avatar: actorProfile.avatar ? actorProfile.avatar.ref.$link : undefined,
      banner: actorProfile.banner ? actorProfile.banner.ref.$link : undefined,
      labels: undefined,

      /*
      //from when we used a custom type, will need reworked
      actorProfile.labels
        ? actorProfile.labels. .values.map((label) => ({
            src: did,
            uri: `at://${did}/app.bsky.actor.profile/self`, // Adjust the URI based on your requirements
            val: label.val,
            cts: "1970-01-01T00:00:00.000Z", // Placeholder for creation timestamp
          }))
        : undefined,*/
      pinnedPost: actorProfile.pinnedPost ? { cid: actorProfile.pinnedPost.cid, uri: actorProfile.pinnedPost.uri } : undefined,
      // Additional fields can be set as required, for example: followersCount: 0,
      // Placeholder value followsCount: 0,
      // Placeholder value postsCount: 0,
      // Placeholder value
      // Other fields like associated, joinedViaStarterPack, indexedAt, createdAt, viewer can be added as needed.
    };
  }
}
