import { BaseDataStore } from "./BaseDataStore.js";
import { StorageConfig, SnapshotInfo } from "./IDataStore.js";
import { Entry, EntryDB } from "../../models/Entry.js";
import { AppBskyActorDefs, AppBskyFeedPost, ComAtprotoRepoDescribeRepo } from "@atproto/api";
import { promises as fs } from "fs";
import path from "path";
import { nanoid } from "nanoid";
import { ThreadPostViewDB } from "../../utils/bskyutils.js";

/**
 * Interface representing stored data in the data store with metadata
 * This provides a common structure across all implementations
 */
export interface StorageData<T> {
  /** The original data stored by the user */
  data: T;

  /** Hash of the data for integrity verification */
  hash: string;

  /** ISO timestamp when the record was created */
  createdAt: string;

  /** ISO timestamp when the record was last modified */
  modifiedAt: string;

  /** Original ID of the record */
  id: string;

  /** Filesystem-safe ID (used by file-based implementations) */
  safeId?: string;

  /** Version identifier for the record */
  version: string;

  /** Snapshot set this record belongs to */
  snapshotset: string;

  /** Filesystem-safe snapshot set ID (used by file-based implementations) */
  safeSnapshotset?: string;
}

/**
 * Flatfile implementation of DataStore
 */
export class FlatfileDataStore extends BaseDataStore {
  private baseDir: string;

  constructor(config: StorageConfig) {
    super(config);

    if (config.type !== "flatfile" || !config.flatfileConfig) {
      throw new Error("Flatfile configuration is required for flatfile storage.");
    }

    this.baseDir = config.flatfileConfig.baseDir;
  }

  /**
   * Sanitize an ID for safe use as a filename
   * Replaces problematic characters with safe alternatives
   */
  private sanitizeId(id: string): string {
    return id.replace(/[\\/:*?"<>|]/g, "_");
  }

  /**
   * Save data to the filesystem
   */
  async save<T>(
    recordType: string,
    id: string,
    data: T,
    snapshotset: string,
    version?: string
  ): Promise<{ id: string; version: string; hash: string }> {
    const newVersion = version || nanoid();
    const createdAt = new Date().toISOString();
    const modifiedAt = createdAt;
    let hash = "";

    // Sanitize IDs for filesystem safety
    const safeId = this.sanitizeId(id);
    const safeSnapshotset = this.sanitizeId(snapshotset);

    // Create directories
    const folderPath = path.join(this.baseDir, recordType);
    await fs.mkdir(folderPath, { recursive: true });

    // Create snapshots directory and metadata if it doesn't exist
    const snapshotsDir = path.join(this.baseDir, "snapshots");
    await fs.mkdir(snapshotsDir, { recursive: true });

    const snapshotDir = path.join(snapshotsDir, safeSnapshotset);
    await fs.mkdir(snapshotDir, { recursive: true });

    // Create snapshot metadata file if it doesn't exist
    const snapshotMetaPath = path.join(snapshotDir, "meta.json");
    try {
      await fs.access(snapshotMetaPath);
    } catch {
      await fs.writeFile(
        snapshotMetaPath,
        JSON.stringify(
          {
            id: snapshotset, // Store original ID
            safeId: safeSnapshotset,
            createdAt: createdAt,
            modifiedAt: modifiedAt,
          },
          null,
          2
        )
      );
    }

    // Calculate hash based on previous data if it exists
    const resultPath = path.join(folderPath, `${safeId}-latest.json`);
    try {
      const previousDataStr = await fs.readFile(resultPath, "utf-8");
      const previousData = JSON.parse(previousDataStr);
      const previousDataHash = this.createHash(previousData);
      hash = this.createHash(data, previousDataHash);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
      hash = this.createHash(data);
    }

    const extendedData: StorageData<T> = {
      data,
      hash,
      createdAt,
      modifiedAt,
      id, // Store the original ID for reference
      safeId, // Store sanitized ID
      version: newVersion,
      snapshotset,
      safeSnapshotset,
    };

    // Save versioned file
    const filePath = path.join(folderPath, `${safeId}-${newVersion}.json`);
    await fs.writeFile(filePath, JSON.stringify(extendedData, null, 2));

    // Save latest version
    await fs.writeFile(resultPath, JSON.stringify(extendedData, null, 2));

    // Save to snapshot directory
    const snapshotRecordDir = path.join(snapshotDir, recordType);
    await fs.mkdir(snapshotRecordDir, { recursive: true });

    const snapshotFilePath = path.join(snapshotRecordDir, `${safeId}.json`);
    await fs.writeFile(snapshotFilePath, JSON.stringify(extendedData, null, 2));

    return { id, version: newVersion, hash };
  }

  /**
   * Fetch data from the filesystem
   */
  async fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined> {
    const folderPath = path.join(this.baseDir, recordType);
    const safeId = this.sanitizeId(id);

    let filePath: string;
    if (version) {
      filePath = path.join(folderPath, `${safeId}-${version}.json`);
    } else {
      filePath = path.join(folderPath, `${safeId}-latest.json`);
    }

    try {
      const data = await fs.readFile(filePath, "utf-8");
      return JSON.parse(data).data as T;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        throw error;
      }
      return undefined;
    }
  }

  /**
   * Fetch an entry with its metadata
   */
  async fetchEntry<T>(recordType: string, id: string, version?: string): Promise<Entry<T> | undefined> {
    const data = await this.fetch<T>(recordType, id, version);

    if (!data) {
      return undefined;
    }

    // Attempt to find the entry metadata
    try {
      const entryFolderPath = path.join(this.baseDir, "entry");
      const safeId = this.sanitizeId(id);
      const entryFilePath = path.join(entryFolderPath, `${safeId}-latest.json`);

      const entryDataStr = await fs.readFile(entryFilePath, "utf-8");
      const entryData = JSON.parse(entryDataStr) as EntryDB;

      if (entryData) {
        return await Entry.create<T>(entryData, data);
      }
    } catch (error) {
      console.error(`Error fetching entry metadata for ${id}:`, error);
    }

    return undefined;
  }

  /**
   * Search for records by ID pattern
   */
  async searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    const folderPath = path.join(this.baseDir, recordType);

    try {
      // Read all files in the directory
      const files = await fs.readdir(folderPath);

      // Find all files that match the ID pattern and get the latest versions
      const latestVersions = new Map<string, string>();

      for (const file of files) {
        // Skip non-JSON files
        if (!file.endsWith(".json")) continue;

        // Parse ID from filename (format: safeId-version.json or safeId-latest.json)
        const match = file.match(/^(.+?)-([^-]+)\.json$/);
        if (match) {
          const [, fileSafeId, fileVersion] = match;

          try {
            // Read the file to get the original ID for pattern matching
            const filePath = path.join(folderPath, file);
            const content = JSON.parse(await fs.readFile(filePath, "utf-8"));

            // Check if original ID matches pattern
            if (content.id && content.id.includes(idPattern)) {
              // If this is a "latest" file or we haven't seen this ID yet
              if (fileVersion === "latest" || !latestVersions.has(fileSafeId)) {
                latestVersions.set(fileSafeId, file);
              }
            }
          } catch (error) {
            console.error(`Error reading file ${file}:`, error);
          }
        }
      }

      // Read each file and apply field filter if provided
      const results = [];

      for (const [, filename] of latestVersions) {
        const filePath = path.join(folderPath, filename);
        const content = JSON.parse(await fs.readFile(filePath, "utf-8"));

        if (!fieldFilter || content[fieldFilter.field] === fieldFilter.value) {
          results.push(content);
        }
      }

      return results;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        // Directory doesn't exist
        return [];
      }
      throw error;
    }
  }

  /**
   * Fetch the latest avatar for a DID
   */
  async fetchLatestAvatar(did: string): Promise<string> {
    try {
      const profile = await this.fetch<any>("profile", did);
      return profile?.avatar || "";
    } catch (error) {
      console.error("Error fetching avatar:", error);
      return "";
    }
  }

  /**
   * Fetch DID for a handle
   */
  async fetchDID(handle: string): Promise<string | undefined> {
    const formatted = "at://" + handle;

    try {
      // Try to find the repo description file that has this handle in alsoKnownAs
      const repoDescriptions = await this.searchById("repo_description", "", { field: "didDoc.alsoKnownAs", value: [formatted] });

      if (repoDescriptions.length > 0) {
        // Sort by modifiedAt to get the latest
        repoDescriptions.sort((a, b) => {
          return new Date(b.modifiedAt).getTime() - new Date(a.modifiedAt).getTime();
        });

        return repoDescriptions[0].did;
      }
    } catch (error) {
      console.error("Error fetching DID:", error);
    }

    return undefined;
  }

  /**
   * Return all latest entries for a DID and record type
   */
  async returnAllLatestEntries<T>(recordType: string, did: string, atID: boolean = true): Promise<Entry<T>[]> {
    const idPattern = atID ? `at://${did}/` : did;
    const records = await this.searchById(recordType, idPattern);
    //console.log(`Found ${records.length} records for ${recordType} with ID pattern "${idPattern}"`);
    //console.log(records);
    const entries: Entry<T>[] = [];

    for (const record of records) {
      try {
        const entryFolderPath = path.join(this.baseDir, "entry");
        const safeId = record.safeId || this.sanitizeId(record.id);
        const entryFilePath = path.join(entryFolderPath, `${safeId}-latest.json`);

        const entryDataStr = await fs.readFile(entryFilePath, "utf-8");
        const entryData = JSON.parse(entryDataStr).data as EntryDB;
        //console.log(`Loading entry metadata for ${record.id} from ${entryFilePath}`);
        //console.log(entryData);
        if (entryData) {
          const entry = await Entry.create<T>(entryData, record.data as T);
          //console.log(entry);
          entries.push(entry);
        }
      } catch (error) {
        // Skip entries with missing metadata
        console.error(`Error loading entry metadata for ${record.id}:`, error);
      }
    }
    //console.log(entries);
    return entries;
  }

  /**
   * Return all latest entries for a thread
   */
  async returnAllLatestEntriesForThread(root_aturl: string): Promise<Entry<AppBskyFeedPost.Record>[]> {
    try {
      // Find all thread_post_view records that refer to this root URI
      const threadPostViewFolder = path.join(this.baseDir, "thread_post_view");
      const files = await fs.readdir(threadPostViewFolder);

      const postIds: string[] = [];

      // Find post IDs for this thread
      for (const file of files) {
        if (file.endsWith("-latest.json")) {
          const filePath = path.join(threadPostViewFolder, file);
          const content = JSON.parse(await fs.readFile(filePath, "utf-8"));

          if (content.data?.root_uri === root_aturl) {
            postIds.push(content.id);
          }
        }
      }

      // Get the actual posts
      const entries: Entry<AppBskyFeedPost.Record>[] = [];

      for (const postId of postIds) {
        const post = await this.fetch<AppBskyFeedPost.Record>("post", postId);

        if (post) {
          try {
            const entryFolderPath = path.join(this.baseDir, "entry");
            const safeId = this.sanitizeId(postId);
            const entryFilePath = path.join(entryFolderPath, `${safeId}-latest.json`);

            const entryDataStr = await fs.readFile(entryFilePath, "utf-8");
            const entryData = JSON.parse(entryDataStr) as EntryDB;

            if (entryData) {
              const entry = await Entry.create<AppBskyFeedPost.Record>(entryData, post);
              entries.push(entry);
            }
          } catch (error) {
            // Skip entries with missing metadata
            console.error(`Error loading entry metadata for ${postId}:`, error);
          }
        }
      }

      return entries;
    } catch (error) {
      console.error("Error fetching thread entries:", error);
      return [];
    }
  }

  /**
   * Get a thread
   * Note: This is a simplified implementation that doesn't match the recursive SQL query exactly
   */
  async getThread(uri: string): Promise<any[]> {
    try {
      // Find the thread post view for this URI
      const safeUri = this.sanitizeId(uri);
      const threadPostView = await this.fetch<ThreadPostViewDB>("thread_post_view", uri);

      if (!threadPostView) {
        return [];
      }

      // Initialize result with the current post
      const result = [threadPostView];

      // Get parent posts recursively
      let currentParentUri = threadPostView?.parent;
      while (currentParentUri) {
        const parentView = await this.fetch<ThreadPostViewDB>("thread_post_view", currentParentUri);
        if (parentView) {
          result.push(parentView);
          currentParentUri = parentView?.parent;
        } else {
          break;
        }
      }

      // Get replies recursively (breadth-first)
      const processReplies = async (postView: any) => {
        if (!postView.data?.replies || !Array.isArray(postView.data.replies)) {
          return;
        }

        for (const replyUri of postView.data.replies) {
          const replyView = await this.fetch<ThreadPostViewDB>("thread_post_view", replyUri);
          if (replyView) {
            result.push(replyView);
            // Process this reply's replies
            await processReplies(replyView);
          }
        }
      };

      await processReplies(threadPostView);

      return result;
    } catch (error) {
      console.error("Error getting thread:", error);
      return [];
    }
  }

  /**
   * Create and maintain a follow relationships file
   */
  private async getFollowRelationshipsFilePath(): Promise<string> {
    const folderPath = path.join(this.baseDir, "follow_relationships");
    await fs.mkdir(folderPath, { recursive: true });
    return path.join(folderPath, "relationships.json");
  }

  /**
   * Read follow relationships from file
   */
  private async readFollowRelationships(): Promise<
    Array<{
      followerDid: string;
      followedDid: string;
      followUri: string;
      snapshotset: string;
      createdAt: string;
    }>
  > {
    const filePath = await this.getFollowRelationshipsFilePath();

    try {
      const content = await fs.readFile(filePath, "utf-8");
      return JSON.parse(content);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        // File doesn't exist yet
        return [];
      }
      throw error;
    }
  }

  /**
   * Write follow relationships to file
   */
  private async writeFollowRelationships(
    relationships: Array<{
      followerDid: string;
      followedDid: string;
      followUri: string;
      snapshotset: string;
      createdAt: string;
    }>
  ): Promise<void> {
    const filePath = await this.getFollowRelationshipsFilePath();
    await fs.writeFile(filePath, JSON.stringify(relationships, null, 2));
  }

  /**
   * Store a follow relationship
   */
  async storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void> {
    const relationships = await this.readFollowRelationships();

    // Check if relationship already exists
    const existingIndex = relationships.findIndex(
      (r) => r.followerDid === followerDid && r.followedDid === followedDid && r.snapshotset === snapshotSet
    );

    if (existingIndex >= 0) {
      // Update existing relationship
      relationships[existingIndex] = {
        followerDid,
        followedDid,
        followUri,
        snapshotset: snapshotSet,
        createdAt: new Date().toISOString(),
      };
    } else {
      // Add new relationship
      relationships.push({
        followerDid,
        followedDid,
        followUri,
        snapshotset: snapshotSet,
        createdAt: new Date().toISOString(),
      });
    }

    await this.writeFollowRelationships(relationships);
  }

  /**
   * Get followers for a DID
   */
  async getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<{ followerDid: string; followUri: string }>> {
    const relationships = await this.readFollowRelationships();

    // Filter relationships
    let filtered = relationships.filter((r) => r.followedDid === targetDid);

    if (snapshotSet) {
      filtered = filtered.filter((r) => r.snapshotset === snapshotSet);
    }

    // Sort by created date (newest first)
    filtered.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

    // Map to required format
    return filtered.map((r) => ({
      followerDid: r.followerDid,
      followUri: r.followUri,
    }));
  }

  /**
   * Get follower profiles for a DID
   */
  async getFollowerProfilesForDid(targetDid: string, snapshotSet?: string): Promise<Array<Entry<AppBskyActorDefs.ProfileViewDetailed>>> {
    // First get all follower DIDs
    const followers = await this.getFollowersForDid(targetDid, snapshotSet);

    // Then fetch the profile for each follower
    const followerProfiles: Array<Entry<AppBskyActorDefs.ProfileViewDetailed>> = [];

    for (const follower of followers) {
      const entry = await this.fetchEntry<AppBskyActorDefs.ProfileViewDetailed>("profile", follower.followerDid);

      if (entry) {
        followerProfiles.push(entry);
      }
    }

    return followerProfiles;
  }

  /**
   * List all available snapshots
   */
  async listSnapshots(): Promise<Array<SnapshotInfo>> {
    try {
      const snapshotsDir = path.join(this.baseDir, "snapshots");

      // Check if snapshots directory exists
      try {
        await fs.access(snapshotsDir);
      } catch {
        return [];
      }

      // Read snapshot directories
      const dirents = await fs.readdir(snapshotsDir, { withFileTypes: true });
      const snapshotDirs = dirents.filter((dirent) => dirent.isDirectory()).map((dirent) => dirent.name);

      const snapshots = [];
      for (const safeDirName of snapshotDirs) {
        const metaFile = path.join(snapshotsDir, safeDirName, "meta.json");

        // Check if meta file exists
        try {
          await fs.access(metaFile);
          const metaContent = await fs.readFile(metaFile, "utf8");
          const meta = JSON.parse(metaContent);
          snapshots.push({
            id: meta.id || safeDirName, // Use original ID if available
            createdAt: meta.createdAt,
          });
        } catch {
          // If no meta file, use folder creation time
          const stats = await fs.stat(path.join(snapshotsDir, safeDirName));
          snapshots.push({
            id: safeDirName,
            createdAt: stats.birthtime.toISOString(),
          });
        }
      }

      // Sort by creation date (newest first)
      return snapshots.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
    } catch (error) {
      console.error("Error listing snapshots:", error);
      return [];
    }
  }

  /**
   * Delete a snapshot and all associated data
   */
  async deleteSnapshot(snapshotId: string): Promise<boolean> {
    try {
      const safeSnapshotId = this.sanitizeId(snapshotId);
      const snapshotDir = path.join(this.baseDir, "snapshots", safeSnapshotId);

      // Check if snapshot directory exists
      try {
        await fs.access(snapshotDir);
      } catch {
        return false;
      }

      // Remove the snapshot directory and all contents
      await fs.rm(snapshotDir, { recursive: true, force: true });
      return true;
    } catch (error) {
      console.error("Error deleting snapshot:", error);
      return false;
    }
  }

  /**
   * Export a snapshot to a portable format
   */
  async exportSnapshot(snapshotId: string): Promise<object | null> {
    try {
      const safeSnapshotId = this.sanitizeId(snapshotId);
      const snapshotDir = path.join(this.baseDir, "snapshots", safeSnapshotId);

      // Check if snapshot directory exists
      try {
        await fs.access(snapshotDir);
      } catch {
        return null;
      }

      const snapshotData: any = {
        id: snapshotId, // Original ID
        tables: {},
      };

      // Get metadata
      const metaFile = path.join(snapshotDir, "meta.json");
      try {
        await fs.access(metaFile);
        const metaContent = await fs.readFile(metaFile, "utf8");
        const meta = JSON.parse(metaContent);
        snapshotData.createdAt = meta.createdAt;
      } catch {
        // No metadata available
      }

      // Get all subdirectories (tables)
      const dirents = await fs.readdir(snapshotDir, { withFileTypes: true });
      const tableDirs = dirents.filter((dirent) => dirent.isDirectory() && dirent.name !== "meta").map((dirent) => dirent.name);

      // For each table, read all files (records)
      for (const table of tableDirs) {
        const tableDir = path.join(snapshotDir, table);
        const files = await fs.readdir(tableDir);
        const jsonFiles = files.filter((file) => file.endsWith(".json"));

        snapshotData.tables[table] = [];

        for (const file of jsonFiles) {
          const filePath = path.join(tableDir, file);
          const fileContent = await fs.readFile(filePath, "utf8");
          const fileData = JSON.parse(fileContent);

          // Make sure we use original ID, not safe ID
          if (fileData.id) {
            fileData.id = fileData.id; // Original ID
          }

          snapshotData.tables[table].push(fileData);
        }
      }

      return snapshotData;
    } catch (error) {
      console.error("Error exporting snapshot:", error);
      return null;
    }
  }

  /**
   * Import a snapshot from a portable format
   */
  async importSnapshot(snapshotId: string, snapshotData: any): Promise<boolean> {
    if (!snapshotData || !snapshotData.tables) {
      console.error("Invalid snapshot data format");
      return false;
    }

    try {
      const safeSnapshotId = this.sanitizeId(snapshotId);
      const snapshotsDir = path.join(this.baseDir, "snapshots");
      await fs.mkdir(snapshotsDir, { recursive: true });

      const snapshotDir = path.join(snapshotsDir, safeSnapshotId);
      await fs.mkdir(snapshotDir, { recursive: true });

      // Create metadata file
      const metaFile = path.join(snapshotDir, "meta.json");
      await fs.writeFile(
        metaFile,
        JSON.stringify(
          {
            id: snapshotId, // Original ID
            safeId: safeSnapshotId, // Safe ID for files
            createdAt: snapshotData.createdAt || new Date().toISOString(),
            importedAt: new Date().toISOString(),
          },
          null,
          2
        )
      );

      // Import data for each table
      for (const [tableName, records] of Object.entries(snapshotData.tables)) {
        const tableDir = path.join(snapshotDir, tableName);
        await fs.mkdir(tableDir, { recursive: true });

        // Write each record to a file
        for (const record of records as any[]) {
          const originalId = record.id || record._id || `import-${nanoid()}`;
          const safeId = this.sanitizeId(originalId);

          // Add safe ID to record data
          const recordWithSafeId = {
            ...record,
            id: originalId, // Ensure original ID is preserved
            safeId, // Add sanitized ID for filesystem operations
          };

          const filePath = path.join(tableDir, `${safeId}.json`);
          await fs.writeFile(filePath, JSON.stringify(recordWithSafeId, null, 2));
        }
      }

      return true;
    } catch (error) {
      console.error("Error importing snapshot:", error);
      return false;
    }
  }

  /**
   * Create a new snapshot with an optional name
   */
  async createSnapshot(name?: string): Promise<string> {
    const snapshotId = name || nanoid();
    const safeSnapshotId = this.sanitizeId(snapshotId);
    const createdAt = new Date().toISOString();

    const snapshotsDir = path.join(this.baseDir, "snapshots");
    await fs.mkdir(snapshotsDir, { recursive: true });

    const snapshotDir = path.join(snapshotsDir, safeSnapshotId);
    await fs.mkdir(snapshotDir, { recursive: true });

    const metaFile = path.join(snapshotDir, "meta.json");
    await fs.writeFile(
      metaFile,
      JSON.stringify(
        {
          id: snapshotId,
          safeId: safeSnapshotId,
          createdAt,
          name: name || snapshotId,
        },
        null,
        2
      )
    );

    return snapshotId;
  }

  /**
   * Duplicate a snapshot with a new ID
   */
  async duplicateSnapshot(sourceSnapshotId: string, targetSnapshotId?: string): Promise<string | null> {
    const newSnapshotId = targetSnapshotId || `copy-${sourceSnapshotId}-${nanoid()}`;

    // Export then import is the simplest way to duplicate
    const exportedData = await this.exportSnapshot(sourceSnapshotId);
    if (!exportedData) {
      return null;
    }

    const success = await this.importSnapshot(newSnapshotId, exportedData);
    return success ? newSnapshotId : null;
  }
}
