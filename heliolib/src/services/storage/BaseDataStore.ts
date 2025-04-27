import { nanoid } from "nanoid";
import crypto from "crypto";
import { Entry, EntryDB } from "../../models/Entry.js";
import { IDataStore, StorageConfig, SnapshotInfo } from "./IDataStore.js";
import { AppBskyActorDefs, AppBskyFeedPost } from "@atproto/api";

/**
 * Base implementation with common functionality for all data store types
 */
export abstract class BaseDataStore implements IDataStore {
  protected config: StorageConfig;

  constructor(config: StorageConfig) {
    this.config = config;
  }

  /**
   * Sanitizes a name for safe use in database operations
   */
  protected sanitizeName(name: string): string {
    return name.replace(/[^a-zA-Z0-9_]/g, "_");
  }

  /**
   * Creates a hash for a data object, optionally based on a previous hash
   */
  protected createHash(data: any, previousDataHash?: string): string {
    if (previousDataHash) {
      return crypto
        .createHash("sha256")
        .update(previousDataHash + JSON.stringify(data))
        .digest("hex");
    } else {
      return crypto.createHash("sha256").update(JSON.stringify(data)).digest("hex");
    }
  }

  /**
   * Abstract methods that must be implemented by specific storage classes
   */
  abstract save<T>(
    recordType: string,
    id: string,
    data: T,
    snapshotset: string,
    version?: string
  ): Promise<{ id: string; version: string; hash: string }>;

  abstract fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined>;

  abstract fetchEntry<T>(recordType: string, id: string, version?: string): Promise<Entry<T> | undefined>;

  abstract searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]>;

  abstract fetchLatestAvatar(did: string): Promise<string>;

  abstract fetchDID(handle: string): Promise<string | undefined>;

  abstract returnAllLatestEntries<T>(recordType: string, did: string, atID?: boolean): Promise<Entry<T>[]>;

  abstract returnAllLatestEntriesForThread(root_aturl: string): Promise<Entry<AppBskyFeedPost.Record>[]>;

  abstract getThread(uri: string): Promise<any[]>;

  abstract storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void>;

  abstract getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<{ followerDid: string; followUri: string }>>;

  abstract getFollowerProfilesForDid(targetDid: string, snapshotSet?: string): Promise<Array<Entry<AppBskyActorDefs.ProfileViewDetailed>>>;

  abstract listSnapshots(): Promise<Array<SnapshotInfo>>;

  abstract deleteSnapshot(snapshotId: string): Promise<boolean>;

  abstract exportSnapshot(snapshotId: string): Promise<object | null>;

  abstract importSnapshot(snapshotId: string, snapshotData: any): Promise<boolean>;

  abstract createSnapshot(name?: string): Promise<string>;

  abstract duplicateSnapshot(sourceSnapshotId: string, targetSnapshotId?: string): Promise<string | null>;

  /**
   * Compare two snapshots - this implementation can be shared across all storage types
   * but can be overridden for optimization
   */
  async compareSnapshots(snapshotId1: string, snapshotId2: string): Promise<object> {
    const snapshot1 = await this.exportSnapshot(snapshotId1);
    const snapshot2 = await this.exportSnapshot(snapshotId2);

    if (!snapshot1 || !snapshot2) {
      throw new Error("One or both snapshots don't exist");
    }

    const comparison: any = {
      snapshot1: { id: snapshotId1, createdAt: (snapshot1 as any).createdAt },
      snapshot2: { id: snapshotId2, createdAt: (snapshot2 as any).createdAt },
      differences: {},
    };

    // Compare tables
    const allTables = new Set([...Object.keys((snapshot1 as any).tables), ...Object.keys((snapshot2 as any).tables)]);

    for (const table of allTables) {
      const tables1 = (snapshot1 as any).tables[table] || [];
      const tables2 = (snapshot2 as any).tables[table] || [];

      const records1Map = new Map(tables1.map((r: any) => [r.id || r._id, r]));
      const records2Map = new Map(tables2.map((r: any) => [r.id || r._id, r]));

      const allIds = new Set([...records1Map.keys(), ...records2Map.keys()]);

      const tableDiff = {
        onlyIn1: [] as string[],
        onlyIn2: [] as string[],
        inBothButDifferent: [] as string[],
      };

      for (const id of allIds) {
        const record1 = records1Map.get(id);
        const record2 = records2Map.get(id);

        if (!record1) {
          tableDiff.onlyIn2.push(id as string);
        } else if (!record2) {
          tableDiff.onlyIn1.push(id as string);
        } else if (JSON.stringify(record1) !== JSON.stringify(record2)) {
          tableDiff.inBothButDifferent.push(id as string);
        }
      }

      // Only add table to differences if there are actual differences
      if (tableDiff.onlyIn1.length > 0 || tableDiff.onlyIn2.length > 0 || tableDiff.inBothButDifferent.length > 0) {
        comparison.differences[table] = tableDiff;
      }
    }

    return comparison;
  }
}
