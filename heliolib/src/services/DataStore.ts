import { IDataStore, StorageConfig } from "./storage/IDataStore.js";
import { DataStoreFactory } from "./storage/DataStoreFactory.js";

/**
 * DataStore class acts as a facade for the underlying storage implementations
 * It maintains backward compatibility with the original API
 */
export class DataStore implements IDataStore {
  private dataStore: IDataStore;

  /**
   * Create a new DataStore instance
   *
   * @param config The storage configuration
   */
  constructor(config: StorageConfig) {
    this.dataStore = DataStoreFactory.createDataStore(config);
  }

  /**
   * Close connections if the underlying store supports it
   */
  async close(): Promise<void> {
    if ("close" in this.dataStore) {
      await (this.dataStore as any).close();
    }
  }

  // Delegate all methods to the underlying data store implementation

  async save<T>(
    recordType: string,
    id: string,
    data: T,
    snapshotset: string,
    version?: string
  ): Promise<{ id: string; version: string; hash: string }> {
    return this.dataStore.save(recordType, id, data, snapshotset, version);
  }

  async fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined> {
    return this.dataStore.fetch(recordType, id, version);
  }

  async fetchEntry<T>(recordType: string, id: string, version?: string): Promise<any> {
    return this.dataStore.fetchEntry(recordType, id, version);
  }

  async searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    return this.dataStore.searchById(recordType, idPattern, fieldFilter);
  }

  async fetchLatestAvatar(did: string): Promise<string> {
    return this.dataStore.fetchLatestAvatar(did);
  }

  async fetchDID(handle: string): Promise<string | undefined> {
    return this.dataStore.fetchDID(handle);
  }

  async returnAllLatestEntries<T>(recordType: string, did: string, atID: boolean = true): Promise<any[]> {
    return this.dataStore.returnAllLatestEntries(recordType, did, atID);
  }

  async returnAllLatestEntriesForThread(root_aturl: string): Promise<any[]> {
    return this.dataStore.returnAllLatestEntriesForThread(root_aturl);
  }

  async getThread(uri: string): Promise<any[]> {
    return this.dataStore.getThread(uri);
  }

  async storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void> {
    return this.dataStore.storeFollowRelationship(followerDid, followedDid, followUri, snapshotSet);
  }

  async getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<{ followerDid: string; followUri: string }>> {
    return this.dataStore.getFollowersForDid(targetDid, snapshotSet);
  }

  async getFollowerProfilesForDid(targetDid: string, snapshotSet?: string): Promise<any[]> {
    return this.dataStore.getFollowerProfilesForDid(targetDid, snapshotSet);
  }

  async listSnapshots(): Promise<Array<{ id: string; createdAt: string }>> {
    return this.dataStore.listSnapshots();
  }

  async deleteSnapshot(snapshotId: string): Promise<boolean> {
    return this.dataStore.deleteSnapshot(snapshotId);
  }

  async exportSnapshot(snapshotId: string): Promise<object | null> {
    return this.dataStore.exportSnapshot(snapshotId);
  }

  async importSnapshot(snapshotId: string, snapshotData: any): Promise<boolean> {
    return this.dataStore.importSnapshot(snapshotId, snapshotData);
  }

  async createSnapshot(name?: string): Promise<string> {
    return this.dataStore.createSnapshot(name);
  }

  async duplicateSnapshot(sourceSnapshotId: string, targetSnapshotId?: string): Promise<string | null> {
    return this.dataStore.duplicateSnapshot(sourceSnapshotId, targetSnapshotId);
  }

  async compareSnapshots(snapshotId1: string, snapshotId2: string): Promise<object> {
    return this.dataStore.compareSnapshots(snapshotId1, snapshotId2);
  }
}
