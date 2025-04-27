import { BaseDataStore } from "./BaseDataStore.js";
import { StorageConfig, SnapshotInfo } from "./IDataStore.js";
import { Entry, EntryDB } from "../../models/Entry.js";
import { AppBskyActorDefs, AppBskyFeedPost, ComAtprotoRepoDescribeRepo } from "@atproto/api";
import { nanoid } from "nanoid";
import sqlite3 from "sqlite3";
import { open, Database } from "sqlite";
import path from "path";
import { promises as fs } from "fs";

/**
 * SQLite implementation of DataStore
 */
export class SQLiteDataStore extends BaseDataStore {
  private db: Database | null = null;
  private dbPromise: Promise<Database>;
  private filename: string;

  constructor(config: StorageConfig) {
    super(config);

    if (config.type !== "sqlite" || !config.sqliteConfig) {
      throw new Error("SQLite configuration is required for SQLite storage.");
    }

    this.filename = config.sqliteConfig.filename;
    this.dbPromise = this.initializeDatabase();
  }

  /**
   * Initialize the SQLite database
   */
  private async initializeDatabase(): Promise<Database> {
    // Ensure directory exists
    const dir = path.dirname(this.filename);
    await fs.mkdir(dir, { recursive: true });

    const db = await open({
      filename: this.filename,
      driver: sqlite3.Database,
    });

    // Enable foreign keys
    await db.exec("PRAGMA foreign_keys = ON");

    // Create core tables
    await db.exec(`
      CREATE TABLE IF NOT EXISTS snapshotsets (
        snapshotset TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        modified_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS follow_relationships (
        follower_did TEXT NOT NULL,
        followed_did TEXT NOT NULL,
        follow_uri TEXT NOT NULL,
        snapshotset TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (follower_did, followed_did, snapshotset),
        FOREIGN KEY (snapshotset) REFERENCES snapshotsets(snapshotset)
      );
    `);

    this.db = db;
    return db;
  }

  /**
   * Get the database instance, initializing if necessary
   */
  private async getDb(): Promise<Database> {
    if (!this.db) {
      this.db = await this.dbPromise;
    }
    return this.db;
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }

  /**
   * Ensure a table exists with the required fields
   */
  private async ensureTableExists(tableName: string): Promise<void> {
    const db = await this.getDb();

    await db.exec(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id TEXT NOT NULL,
        snapshotset TEXT NOT NULL,
        version TEXT NOT NULL,
        data JSON NOT NULL,
        created_at TEXT NOT NULL,
        modified_at TEXT NOT NULL,
        hash TEXT,
        PRIMARY KEY (id, version),
        FOREIGN KEY (snapshotset) REFERENCES snapshotsets(snapshotset)
      )
    `);

    // Create indices for faster lookups
    await db.exec(`
      CREATE INDEX IF NOT EXISTS idx_${tableName}_id ON ${tableName}(id);
      CREATE INDEX IF NOT EXISTS idx_${tableName}_snapshotset ON ${tableName}(snapshotset);
    `);
  }

  /**
   * Save data to the SQLite database
   */
  async save<T>(
    recordType: string,
    id: string,
    data: T,
    snapshotset: string,
    version?: string
  ): Promise<{ id: string; version: string; hash: string }> {
    const db = await this.getDb();
    const newVersion = version || nanoid();
    const createdAt = new Date().toISOString();
    const modifiedAt = createdAt;
    let hash = "";

    const tableName = this.sanitizeName(recordType);

    // Begin transaction
    await db.exec("BEGIN TRANSACTION");

    try {
      // Ensure snapshotset exists
      const snapshotExists = await db.get("SELECT 1 FROM snapshotsets WHERE snapshotset = ?", [snapshotset]);

      if (!snapshotExists) {
        await db.run("INSERT INTO snapshotsets (snapshotset, created_at, modified_at) VALUES (?, ?, ?)", [
          snapshotset,
          createdAt,
          modifiedAt,
        ]);
      }

      // Ensure table exists
      await this.ensureTableExists(tableName);

      // Get previous data to calculate hash
      const previous = await db.get(`SELECT data FROM ${tableName} WHERE id = ? ORDER BY modified_at DESC LIMIT 1`, [id]);

      if (previous) {
        const previousDataHash = this.createHash(JSON.parse(previous.data));
        hash = this.createHash(data, previousDataHash);
      } else {
        hash = this.createHash(data);
      }

      // Add hash to data
      const extendedData = { ...(data as object), hash };

      // Insert new record
      await db.run(
        `INSERT INTO ${tableName} (id, snapshotset, version, data, created_at, modified_at, hash) 
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [id, snapshotset, newVersion, JSON.stringify(extendedData), createdAt, modifiedAt, hash]
      );

      // Commit transaction
      await db.exec("COMMIT");

      return { id, version: newVersion, hash };
    } catch (error) {
      console.error(`Error saving ${recordType} with id ${id}:`, error);
      // Rollback on error
      await db.exec("ROLLBACK");
      throw error;
    }
  }

  /**
   * Fetch data from the SQLite database
   */
  async fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined> {
    const db = await this.getDb();
    const tableName = this.sanitizeName(recordType);

    try {
      // Check if table exists
      const tableExists = await db.get("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [tableName]);

      if (!tableExists) {
        return undefined;
      }

      let result;

      if (version) {
        result = await db.get(`SELECT data FROM ${tableName} WHERE id = ? AND version = ?`, [id, version]);
      } else {
        result = await db.get(`SELECT data FROM ${tableName} WHERE id = ? ORDER BY modified_at DESC LIMIT 1`, [id]);
      }

      if (result) {
        return typeof result.data === "string" ? (JSON.parse(result.data) as T) : (result.data as T);
      }

      return undefined;
    } catch (error) {
      console.error(`Error fetching ${recordType} with id ${id}:`, error);
      return undefined;
    }
  }

  /**
   * Fetch an entry with its metadata
   */
  async fetchEntry<T>(recordType: string, id: string, version?: string): Promise<Entry<T> | undefined> {
    const db = await this.getDb();
    const tableName = this.sanitizeName(recordType);

    try {
      // Check if table exists
      const tableExists = await db.get("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [tableName]);

      if (!tableExists) {
        return undefined;
      }

      let row;

      if (version) {
        row = await db.get(`SELECT * FROM ${tableName} WHERE id = ? AND version = ?`, [id, version]);
      } else {
        row = await db.get(`SELECT * FROM ${tableName} WHERE id = ? ORDER BY modified_at DESC LIMIT 1`, [id]);
      }

      if (!row) {
        return undefined;
      }

      // Parse data if it's a string
      const data = typeof row.data === "string" ? (JSON.parse(row.data) as T) : (row.data as T);

      // Get entry metadata
      const entryRow = await db.get("SELECT * FROM entry WHERE id = ? AND snapshotset = ? LIMIT 1", [row.id, row.snapshotset]);

      if (!entryRow) {
        return undefined;
      }

      // Parse EntryDB data if needed
      const entryData = typeof entryRow.data === "string" ? (JSON.parse(entryRow.data) as EntryDB) : (entryRow.data as EntryDB);

      // If no data in entry table, create from the row data
      if (!entryData) {
        const reconstructedEntry = new EntryDB(
          entryRow.record || JSON.stringify(data),
          entryRow.linked_to || "",
          entryRow.recorded_at || Date.now(),
          entryRow.recorder || "unknown",
          entryRow.identity || id,
          entryRow.record_type || recordType,
          entryRow.record_version || version
        );

        return await Entry.create<T>(reconstructedEntry, data);
      }

      return await Entry.create<T>(entryData, data);
    } catch (error) {
      console.error(`Error fetching entry for ${recordType} with id ${id}:`, error);
      return undefined;
    }
  }

  /**
   * Search for records by ID pattern
   */
  async searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    const tableName = this.sanitizeName(recordType);
    const db = await this.getDb();

    let query: string;
    let values: any[];

    if (fieldFilter) {
      query = `SELECT * FROM ${tableName} WHERE id LIKE ? AND JSON_EXTRACT(data, '$.' || ?) = ? ORDER BY modified_at DESC`;
      values = [idPattern, fieldFilter.field, fieldFilter.value];
    } else {
      query = `SELECT * FROM ${tableName} WHERE id LIKE ? ORDER BY modified_at DESC`;
      values = [idPattern];
    }

    const rows = await db.all(query, values);

    // Parse the JSON data column for each row before returning
    return rows.map((row) => {
      if (row.data && typeof row.data === "string") {
        try {
          row.data = JSON.parse(row.data);
        } catch (e) {
          // Handle parsing error if needed
          console.error(`Failed to parse JSON data for row with id ${row.id}:`, e);
        }
      }
      return row;
    });
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
    const db = await this.getDb();
    const formatted = "at://" + handle;

    try {
      // Check if repo_description table exists
      const tableExists = await db.get("SELECT name FROM sqlite_master WHERE type='table' AND name='repo_description'");

      if (!tableExists) {
        return undefined;
      }

      // This query is simplistic because SQLite's JSON support varies by version
      const rows = await db.all("SELECT data FROM repo_description ORDER BY modified_at DESC");

      // Search in rows for alsoKnownAs containing the handle
      for (const row of rows) {
        const data = typeof row.data === "string" ? JSON.parse(row.data) : row.data;

        if (data?.didDoc?.alsoKnownAs && Array.isArray(data.didDoc.alsoKnownAs) && data.didDoc.alsoKnownAs.includes(formatted)) {
          return data.did;
        }
      }

      return undefined;
    } catch (error) {
      console.error("Error fetching DID:", error);
      return undefined;
    }
  }

  /**
   * Return all latest entries for a DID and record type
   */
  async returnAllLatestEntries<T>(recordType: string, did: string, atID: boolean = true): Promise<Entry<T>[]> {
    const db = await this.getDb();
    const tableName = this.sanitizeName(recordType);
    const pattern = atID ? `at://${did}/%` : did;

    try {
      // Check if table exists
      const tableExists = await db.get("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [tableName]);

      if (!tableExists) {
        return [];
      }

      // SQLite doesn't support window functions in all versions, so we'll do this in two steps
      // First get the latest version for each ID
      const latestQuery = `
        SELECT id, MAX(modified_at) AS max_modified_at 
        FROM ${tableName} 
        WHERE id LIKE ?
        GROUP BY id
      `;
      const latestRows = await db.all(latestQuery, [pattern]);

      if (latestRows.length === 0) {
        return [];
      }

      // Now fetch each record with its latest timestamp
      const entries: Entry<T>[] = [];

      for (const row of latestRows) {
        const recordRow = await db.get(`SELECT * FROM ${tableName} WHERE id = ? AND modified_at = ?`, [row.id, row.max_modified_at]);

        if (recordRow) {
          // Get entry metadata
          const entryRow = await db.get("SELECT * FROM entry WHERE id = ? AND snapshotset = ? LIMIT 1", [
            recordRow.id,
            recordRow.snapshotset,
          ]);

          if (entryRow) {
            const data = typeof recordRow.data === "string" ? (JSON.parse(recordRow.data) as T) : (recordRow.data as T);

            const entryData = typeof entryRow.data === "string" ? (JSON.parse(entryRow.data) as EntryDB) : (entryRow.data as EntryDB);

            // If no data in entry table, create from the row data
            if (!entryData) {
              const reconstructedEntry = new EntryDB(
                entryRow.record || JSON.stringify(data),
                entryRow.linked_to || "",
                entryRow.recorded_at || Date.now(),
                entryRow.recorder || "unknown",
                entryRow.identity || recordRow.id,
                entryRow.record_type || recordType,
                entryRow.record_version || recordRow.version
              );

              const entry = await Entry.create<T>(reconstructedEntry, data);
              entries.push(entry);
            } else {
              const entry = await Entry.create<T>(entryData, data);
              entries.push(entry);
            }
          }
        }
      }

      return entries;
    } catch (error) {
      console.error(`Error fetching entries for ${recordType}:`, error);
      return [];
    }
  }

  /**
   * Return all latest entries for a thread
   * Note: This is a simplified implementation compared to PostgreSQL
   */
  async returnAllLatestEntriesForThread(root_aturl: string): Promise<Entry<AppBskyFeedPost.Record>[]> {
    const db = await this.getDb();

    try {
      // Check if thread_post_view table exists
      const tableExists = await db.get("SELECT name FROM sqlite_master WHERE type='table' AND name='thread_post_view'");

      if (!tableExists) {
        return [];
      }

      // Find all post IDs in the thread
      const postIdsQuery = `
        SELECT id FROM thread_post_view 
        WHERE json_extract(data, '$.root_uri') = ?
      `;
      const postIdRows = await db.all(postIdsQuery, [root_aturl]);

      if (postIdRows.length === 0) {
        return [];
      }

      const entries: Entry<AppBskyFeedPost.Record>[] = [];

      // Get each post and its entry
      for (const { id } of postIdRows) {
        // Get latest version of the post
        const postRow = await db.get(`SELECT * FROM post WHERE id = ? ORDER BY modified_at DESC LIMIT 1`, [id]);

        if (postRow) {
          // Get entry metadata
          const entryRow = await db.get("SELECT * FROM entry WHERE id = ? AND snapshotset = ? LIMIT 1", [postRow.id, postRow.snapshotset]);

          if (entryRow) {
            const data =
              typeof postRow.data === "string"
                ? (JSON.parse(postRow.data) as AppBskyFeedPost.Record)
                : (postRow.data as AppBskyFeedPost.Record);

            const entryData = typeof entryRow.data === "string" ? (JSON.parse(entryRow.data) as EntryDB) : (entryRow.data as EntryDB);

            if (!entryData) {
              const reconstructedEntry = new EntryDB(
                entryRow.record || JSON.stringify(data),
                entryRow.linked_to || "",
                entryRow.recorded_at || Date.now(),
                entryRow.recorder || "unknown",
                entryRow.identity || postRow.id,
                entryRow.record_type || "post",
                entryRow.record_version || postRow.version
              );

              const entry = await Entry.create<AppBskyFeedPost.Record>(reconstructedEntry, data);
              entries.push(entry);
            } else {
              const entry = await Entry.create<AppBskyFeedPost.Record>(entryData, data);
              entries.push(entry);
            }
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
   * Note: This is a simplified implementation that doesn't use recursive queries
   */
  async getThread(uri: string): Promise<any[]> {
    const db = await this.getDb();

    try {
      // Check if thread_post_view table exists
      const tableExists = await db.get("SELECT name FROM sqlite_master WHERE type='table' AND name='thread_post_view'");

      if (!tableExists) {
        return [];
      }

      // Get the thread view for this URI
      const threadView = await db.get("SELECT * FROM thread_post_view WHERE id = ?", [uri]);

      if (!threadView) {
        return [];
      }

      // Parse data if needed
      const threadData = typeof threadView.data === "string" ? JSON.parse(threadView.data) : threadView.data;

      // Initialize result with current post
      const result = [{ ...threadView, data: threadData }];

      // Function to get post parents recursively
      const getParents = async (parentUri: string, visited = new Set<string>()) => {
        if (!parentUri || visited.has(parentUri)) {
          return;
        }

        visited.add(parentUri);

        const parent = await db.get("SELECT * FROM thread_post_view WHERE id = ?", [parentUri]);

        if (parent) {
          const parentData = typeof parent.data === "string" ? JSON.parse(parent.data) : parent.data;

          result.push({ ...parent, data: parentData });

          if (parentData.parent) {
            await getParents(parentData.parent, visited);
          }
        }
      };

      // Function to get post replies recursively
      const getReplies = async (postUri: string, visited = new Set<string>()) => {
        if (!postUri || visited.has(postUri)) {
          return;
        }

        visited.add(postUri);

        const post = await db.get("SELECT * FROM thread_post_view WHERE id = ?", [postUri]);

        if (post) {
          const postData = typeof post.data === "string" ? JSON.parse(post.data) : post.data;

          if (postData.replies && Array.isArray(postData.replies)) {
            for (const replyUri of postData.replies) {
              const reply = await db.get("SELECT * FROM thread_post_view WHERE id = ?", [replyUri]);

              if (reply) {
                const replyData = typeof reply.data === "string" ? JSON.parse(reply.data) : reply.data;

                result.push({ ...reply, data: replyData });
                await getReplies(replyUri, visited);
              }
            }
          }
        }
      };

      // Get parents and replies
      if (threadData.parent) {
        await getParents(threadData.parent);
      }

      await getReplies(uri);

      return result;
    } catch (error) {
      console.error("Error getting thread:", error);
      return [];
    }
  }

  /**
   * Store a follow relationship
   */
  async storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void> {
    const db = await this.getDb();

    try {
      // Ensure snapshotset exists
      const snapshotExists = await db.get("SELECT 1 FROM snapshotsets WHERE snapshotset = ?", [snapshotSet]);

      if (!snapshotExists) {
        const now = new Date().toISOString();
        await db.run("INSERT INTO snapshotsets (snapshotset, created_at, modified_at) VALUES (?, ?, ?)", [snapshotSet, now, now]);
      }

      // Insert or update follow relationship
      await db.run(
        `
        INSERT INTO follow_relationships 
        (follower_did, followed_did, follow_uri, snapshotset, created_at) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (follower_did, followed_did, snapshotset) 
        DO UPDATE SET follow_uri = ?, created_at = ?
      `,
        [followerDid, followedDid, followUri, snapshotSet, new Date().toISOString(), followUri, new Date().toISOString()]
      );
    } catch (error) {
      console.error("Error storing follow relationship:", error);
      throw error;
    }
  }

  /**
   * Get followers for a DID
   */
  async getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<{ followerDid: string; followUri: string }>> {
    const db = await this.getDb();

    try {
      let sql = `
        SELECT follower_did, follow_uri
        FROM follow_relationships
        WHERE followed_did = ?
      `;
      const params: any[] = [targetDid];

      if (snapshotSet) {
        sql += " AND snapshotset = ?";
        params.push(snapshotSet);
      }

      sql += " ORDER BY created_at DESC";

      const rows = await db.all(sql, params);

      return rows.map((row) => ({
        followerDid: row.follower_did,
        followUri: row.follow_uri,
      }));
    } catch (error) {
      console.error("Error getting followers:", error);
      return [];
    }
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
    const db = await this.getDb();

    try {
      const rows = await db.all(`
        SELECT 
          snapshotset as id, 
          created_at as createdAt
        FROM 
          snapshotsets 
        ORDER BY 
          created_at DESC
      `);

      return rows;
    } catch (error) {
      console.error("Error listing snapshots:", error);
      return [];
    }
  }

  /**
   * Delete a snapshot and all associated data
   */
  async deleteSnapshot(snapshotId: string): Promise<boolean> {
    const db = await this.getDb();

    try {
      // Begin transaction
      await db.exec("BEGIN TRANSACTION");

      // Get all tables
      const tableRows = await db.all(`
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name != 'sqlite_sequence'
      `);

      // Delete from all tables that have a snapshotset column
      for (const { name } of tableRows) {
        // Check if this table has a snapshotset column
        const columnInfo = await db.all(`PRAGMA table_info(${name})`);
        const hasSnapshotset = columnInfo.some((col) => col.name === "snapshotset");

        if (hasSnapshotset && name !== "snapshotsets") {
          await db.run(`DELETE FROM ${name} WHERE snapshotset = ?`, [snapshotId]);
        }
      }

      // Finally delete the snapshot itself
      const result = await db.run("DELETE FROM snapshotsets WHERE snapshotset = ?", [snapshotId]);

      // Commit transaction
      await db.exec("COMMIT");

      return result.changes ? result.changes > 0 : false;
    } catch (error) {
      // Rollback on error
      await db.exec("ROLLBACK");
      console.error("Error deleting snapshot:", error);
      return false;
    }
  }

  /**
   * Export a snapshot to a portable format
   */
  async exportSnapshot(snapshotId: string): Promise<object | null> {
    const db = await this.getDb();

    try {
      // Check if snapshot exists
      const snapshot = await db.get("SELECT * FROM snapshotsets WHERE snapshotset = ?", [snapshotId]);

      if (!snapshot) {
        return null;
      }

      const snapshotData: any = {
        id: snapshotId,
        createdAt: snapshot.created_at,
        tables: {},
      };

      // Get all tables
      const tableRows = await db.all(`
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name != 'sqlite_sequence' AND name != 'snapshotsets'
      `);

      // For each table, check if it has a snapshotset column and export records
      for (const { name } of tableRows) {
        // Check if this table has a snapshotset column
        const columnInfo = await db.all(`PRAGMA table_info(${name})`);
        const hasSnapshotset = columnInfo.some((col) => col.name === "snapshotset");

        if (hasSnapshotset) {
          const records = await db.all(`SELECT * FROM ${name} WHERE snapshotset = ?`, [snapshotId]);

          // Process records to handle JSON data fields
          const processedRecords = records.map((record) => {
            const processed = { ...record };

            // Parse JSON fields if they're strings
            if (typeof processed.data === "string") {
              try {
                processed.data = JSON.parse(processed.data);
              } catch {
                /* Leave as is if not valid JSON */
              }
            }

            return processed;
          });

          if (processedRecords.length > 0) {
            snapshotData.tables[name] = processedRecords;
          }
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

    const db = await this.getDb();
    const createdAt = snapshotData.createdAt || new Date().toISOString();

    try {
      // Begin transaction
      await db.exec("BEGIN TRANSACTION");

      // Create snapshot entry
      await db.run(
        `INSERT INTO snapshotsets (snapshotset, created_at, modified_at) 
         VALUES (?, ?, ?) 
         ON CONFLICT (snapshotset) DO NOTHING`,
        [snapshotId, createdAt, createdAt]
      );

      // Import data for each table
      for (const [tableName, records] of Object.entries(snapshotData.tables)) {
        await this.ensureTableExists(tableName);

        // Insert records
        for (const record of records as any[]) {
          // Prepare data to insert
          const id = record.id || record._id || `import-${nanoid()}`;

          // Convert any data field to JSON string if it's an object
          let dataToInsert = record.data;
          if (dataToInsert && typeof dataToInsert === "object") {
            dataToInsert = JSON.stringify(dataToInsert);
          }

          // Insert with minimal fields to ensure compatibility
          await db.run(
            `INSERT INTO ${tableName} (
              id, snapshotset, version, data, created_at, modified_at, hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id, version) DO UPDATE SET
              data = excluded.data,
              modified_at = excluded.modified_at,
              hash = excluded.hash`,
            [
              id,
              snapshotId,
              record.version || nanoid(),
              dataToInsert,
              record.created_at || new Date().toISOString(),
              record.modified_at || new Date().toISOString(),
              record.hash || this.createHash(record.data || {}),
            ]
          );
        }
      }

      // Commit transaction
      await db.exec("COMMIT");
      return true;
    } catch (error) {
      // Rollback on error
      await db.exec("ROLLBACK");
      console.error("Error importing snapshot:", error);
      return false;
    }
  }

  /**
   * Create a new snapshot with an optional name
   */
  async createSnapshot(name?: string): Promise<string> {
    const db = await this.getDb();
    const snapshotId = name || nanoid();
    const createdAt = new Date().toISOString();

    try {
      await db.run(
        `INSERT INTO snapshotsets (snapshotset, created_at, modified_at)
         VALUES (?, ?, ?)
         ON CONFLICT (snapshotset) DO NOTHING`,
        [snapshotId, createdAt, createdAt]
      );

      return snapshotId;
    } catch (error) {
      console.error("Error creating snapshot:", error);
      throw error;
    }
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
