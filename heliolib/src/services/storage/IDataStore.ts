import { Entry, EntryDB } from "../../models/Entry.js";
import { AppBskyActorDefs, AppBskyFeedPost } from "@atproto/api";

export interface StorageConfig {
  type: "postgresql" | "flatfile" | "sqlite";
  postgresConfig?: {
    connectionString: string;
  };
  flatfileConfig?: {
    baseDir: string;
  };
  sqliteConfig?: {
    filename: string;
  };
}

export interface DBEntry {
  id: string;
  created_at: number;
  version: string;
  snapshotset: string;
}

export interface FollowerInfo {
  followerDid: string;
  followUri: string;
}

export interface SnapshotInfo {
  id: string;
  createdAt: string;
}

export interface IDataStore {
  // Core data methods
  save<T>(
    recordType: string,
    id: string,
    data: T,
    snapshotset: string,
    version?: string
  ): Promise<{ id: string; version: string; hash: string }>;

  fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined>;

  fetchEntry<T>(recordType: string, id: string, version?: string): Promise<Entry<T> | undefined>;

  searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]>;

  // Profile methods
  fetchLatestAvatar(did: string): Promise<string>;

  fetchDID(handle: string): Promise<string | undefined>;

  // Collection methods
  returnAllLatestEntries<T>(recordType: string, did: string, atID?: boolean): Promise<Entry<T>[]>;

  returnAllLatestEntriesForThread(root_aturl: string): Promise<Entry<AppBskyFeedPost.Record>[]>;

  // Thread methods
  getThread(uri: string): Promise<any[]>;

  // Follow relationship methods
  storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void>;

  getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<FollowerInfo>>;

  getFollowerProfilesForDid(targetDid: string, snapshotSet?: string): Promise<Array<Entry<AppBskyActorDefs.ProfileViewDetailed>>>;

  // Snapshot management
  listSnapshots(): Promise<Array<SnapshotInfo>>;

  deleteSnapshot(snapshotId: string): Promise<boolean>;

  exportSnapshot(snapshotId: string): Promise<object | null>;

  importSnapshot(snapshotId: string, snapshotData: any): Promise<boolean>;

  createSnapshot(name?: string): Promise<string>;

  duplicateSnapshot(sourceSnapshotId: string, targetSnapshotId?: string): Promise<string | null>;

  compareSnapshots(snapshotId1: string, snapshotId2: string): Promise<object>;
}
