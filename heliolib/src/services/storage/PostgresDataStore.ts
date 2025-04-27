import { BaseDataStore } from "./BaseDataStore.js";
import { StorageConfig, SnapshotInfo } from "./IDataStore.js";
import { Entry, EntryDB } from "../../models/Entry.js";
import { AppBskyActorDefs, AppBskyFeedPost, ComAtprotoRepoDescribeRepo } from "@atproto/api";
import pkg from "pg";
import { nanoid } from "nanoid";

/**
 * PostgreSQL implementation of DataStore
 */
export class PostgresDataStore extends BaseDataStore {
  private client: pkg.Client;

  constructor(config: StorageConfig) {
    super(config);

    if (config.type !== "postgresql" || !config.postgresConfig) {
      throw new Error("PostgreSQL configuration is required for PostgreSQL storage.");
    }

    this.client = new pkg.Client({
      connectionString: config.postgresConfig.connectionString,
    });
    this.client.connect();
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    await this.client.end();
  }

  /**
   * Save data to the PostgreSQL database
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

    const tableName = this.sanitizeName(recordType);

    // Ensure snapshotsets table exists and has an entry for this snapshotset
    await this.client.query(
      `CREATE TABLE IF NOT EXISTS snapshotsets (
        snapshotset TEXT NOT NULL,                    
        created_at TIMESTAMP NOT NULL,
        modified_at TIMESTAMP NOT NULL,
        PRIMARY KEY (snapshotset)
      )`
    );

    const sresult = await this.client.query(
      `SELECT snapshotset FROM snapshotsets WHERE snapshotset = $1 ORDER BY modified_at DESC LIMIT 1`,
      [snapshotset]
    );

    if (sresult.rowCount == 0) {
      await this.client.query(`INSERT INTO snapshotsets (snapshotset, created_at, modified_at) VALUES ($1, $2, $3)`, [
        snapshotset,
        createdAt,
        modifiedAt,
      ]);
    }

    // Ensure the table for this record type exists
    await this.client.query(
      `CREATE TABLE IF NOT EXISTS ${tableName} (
        id TEXT NOT NULL,
        snapshotset TEXT NOT NULL,
        version TEXT NOT NULL,
        data JSONB NOT NULL,
        created_at TIMESTAMP NOT NULL,
        modified_at TIMESTAMP NOT NULL,
        hash TEXT,
        PRIMARY KEY (id, version)
      )`
    );

    // Get previous data if it exists to calculate hash
    const result = await this.client.query(`SELECT data FROM ${tableName} WHERE id = $1 ORDER BY modified_at DESC LIMIT 1`, [id]);

    if (result.rows.length > 0) {
      const previousDataHash = this.createHash(result.rows[0].data);
      hash = this.createHash(data, previousDataHash);
    } else {
      hash = this.createHash(data);
    }

    const extendedData = { ...data, hash };

    // Insert the new record
    await this.client.query(
      `INSERT INTO ${tableName} (id, snapshotset, version, data, created_at, modified_at, hash) 
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [id, snapshotset, newVersion, extendedData, createdAt, modifiedAt, hash]
    );

    return { id, version: newVersion, hash };
  }

  /**
   * Fetch data from the PostgreSQL database
   */
  async fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined> {
    const tableName = this.sanitizeName(recordType);

    if (!version) {
      // Fetch the most recent version based on modified_at
      const result = await this.client.query(`SELECT data FROM ${tableName} WHERE id = $1 ORDER BY modified_at DESC LIMIT 1`, [id]);
      return (result.rows[0]?.data as T) || undefined;
    } else {
      const result = await this.client.query(`SELECT data FROM ${tableName} WHERE id = $1 AND version = $2`, [id, version]);
      return (result.rows[0]?.data as T) || undefined;
    }
  }

  /**
   * Fetch an entry with its metadata
   */
  async fetchEntry<T>(recordType: string, id: string, version?: string): Promise<Entry<T> | undefined> {
    const tableName = this.sanitizeName(recordType);
    let resultRecord: T | undefined;
    let row: any;

    if (!version) {
      // Fetch the most recent version based on modified_at
      const result = await this.client.query(`SELECT * FROM ${tableName} WHERE id = $1 ORDER BY modified_at DESC LIMIT 1`, [id]);
      row = result.rows[0];
      resultRecord = (result.rows[0]?.data as T) || undefined;
    } else {
      const result = await this.client.query(`SELECT * FROM ${tableName} WHERE id = $1 AND version = $2`, [id, version]);
      row = result.rows[0];
      resultRecord = (result.rows[0]?.data as T) || undefined;
    }

    if (resultRecord && row) {
      // Get entry metadata
      const query = `SELECT * FROM entry WHERE id = $1 AND snapshotset = $2 LIMIT 1`;
      const result = await this.client.query(query, [row.id, row.snapshotset]);

      const data = result.rows[0]?.data as EntryDB;
      if (data) {
        return await Entry.create<T>(data, resultRecord);
      }
    }

    return undefined;
  }

  /**
   * Search for records by ID pattern
   */
  async searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    const tableName = this.sanitizeName(recordType);

    const query = fieldFilter
      ? `SELECT * FROM ${tableName} WHERE id LIKE $1 AND data->>$2 = $3 ORDER BY modified_at DESC`
      : `SELECT * FROM ${tableName} WHERE id LIKE $1 ORDER BY modified_at DESC`;

    const values = fieldFilter ? [idPattern, fieldFilter.field, fieldFilter.value] : [idPattern];

    const result = await this.client.query(query, values);
    return result.rows;
  }

  /**
   * Fetch the latest avatar for a DID
   */
  async fetchLatestAvatar(did: string): Promise<string> {
    const query = `
      SELECT data->>'avatar' AS avatar 
      FROM profile 
      WHERE id = $1 AND data->>'avatar' IS NOT NULL
      ORDER BY modified_at DESC
      LIMIT 1
    `;

    try {
      const result = await this.client.query(query, [did]);
      if (result.rows && result.rows.length > 0) {
        return result.rows[0].avatar || "";
      }
    } catch (err) {
      console.error("Error executing query", err);
    }

    return "";
  }

  /**
   * Fetch DID for a handle
   */
  async fetchDID(handle: string): Promise<string | undefined> {
    const formatted = "at://" + handle;

    const result = await this.client.query(
      `SELECT data FROM repo_description 
       WHERE data->'didDoc'->'alsoKnownAs' @> $1 
       ORDER BY modified_at DESC LIMIT 1`,
      [`["${formatted}"]`]
    );

    if (result.rows && result.rowCount && result.rowCount > 0) {
      return (result.rows[0].data as ComAtprotoRepoDescribeRepo.OutputSchema).did;
    }

    return undefined;
  }

  /**
   * Return all latest entries for a DID and record type
   */
  async returnAllLatestEntries<T>(recordType: string, did: string, atID: boolean = true): Promise<Entry<T>[]> {
    const tableName = this.sanitizeName(recordType);
    const query = `
      SELECT p1.* 
      FROM ${tableName} p1 
      JOIN (
        SELECT id, MAX(modified_at) AS max_modified_at 
        FROM ${tableName} 
        WHERE id LIKE $1 
        GROUP BY id
      ) p2 
      ON p1.id = p2.id AND p1.modified_at = p2.max_modified_at
    `;

    const id = atID ? "at://" + did + "/%" : did;
    const result = await this.client.query(query, [id]);

    let retVals: Entry<T>[] = [];

    // Get entry for each result
    for (const row of result.rows) {
      const query = `SELECT * FROM entry WHERE id = $1 AND snapshotset = $2 LIMIT 1`;
      const result = await this.client.query(query, [row.id, row.snapshotset]);

      const data = result.rows[0]?.data as EntryDB;
      if (data) {
        let entry = await Entry.create<T>(data, row.data as T);
        retVals.push(entry);
      }
    }

    return retVals;
  }

  /**
   * Return all latest entries for a thread
   */
  async returnAllLatestEntriesForThread(root_aturl: string): Promise<Entry<AppBskyFeedPost.Record>[]> {
    const query = `
      SELECT *
      FROM post
      WHERE (id, modified_at) IN (
        SELECT id, MAX(modified_at)
        FROM post
        WHERE id IN (
          SELECT id
          FROM thread_post_view
          WHERE data ->> 'root_uri' = $1
        )
        GROUP BY id
      )
    `;

    const result = await this.client.query(query, [root_aturl]);
    let retVals: Entry<AppBskyFeedPost.Record>[] = [];

    // Get entry for each result
    for (const row of result.rows) {
      const query = `SELECT * FROM entry WHERE id = $1 AND snapshotset = $2 LIMIT 1`;
      const result = await this.client.query(query, [row.id, row.snapshotset]);

      const data = result.rows[0]?.data as EntryDB;
      if (data) {
        let entry = await Entry.create<AppBskyFeedPost.Record>(data, row.data as AppBskyFeedPost.Record);
        retVals.push(entry);
      }
    }

    return retVals;
  }

  /**
   * Get a thread
   */
  async getThread(uri: string): Promise<any[]> {
    const query = `
      WITH RECURSIVE parent_hierarchy AS (
        -- Base case: Start with the given post
        SELECT 
          tpv.id,
          tpv.data,
          tpv.data ->> 'post' AS post_uri,
          tpv.data ->> 'parent' AS parent_uri,
          tpv.data -> 'replies' AS replies
        FROM 
          thread_post_view tpv
        WHERE 
          tpv.id = $1
        
        UNION ALL
        
        -- Recursive case: Find all parent posts
        SELECT 
          tpv.id, 
          tpv.data,
          tpv.data ->> 'post' AS post_uri,
          tpv.data ->> 'parent' AS parent_uri,
          tpv.data -> 'replies' AS replies
        FROM 
          thread_post_view tpv
        JOIN 
          parent_hierarchy ph ON tpv.id = ph.parent_uri
      ),
      reply_hierarchy AS (
        -- Base case: Start with the given post
        SELECT 
          tpv.id, 
          tpv.data,
          tpv.data ->> 'post' AS post_uri,
          tpv.data ->> 'parent' AS parent_uri,
          tpv.data -> 'replies' AS replies
        FROM 
          thread_post_view tpv
        WHERE 
          tpv.id = $1
        
        UNION ALL
        
        -- Recursive case: Find all replies
        SELECT 
          tpv.id, 
          tpv.data,
          tpv.data ->> 'post' AS post_uri,
          tpv.data ->> 'parent' AS parent_uri,
          tpv.data -> 'replies' AS replies
        FROM 
          thread_post_view tpv,
          reply_hierarchy rh,
          jsonb_array_elements_text(rh.replies) as reply
        WHERE 
          tpv.id = reply
      )
      SELECT * FROM parent_hierarchy
      UNION
      SELECT * FROM reply_hierarchy
    `;

    const result = await this.client.query(query, [uri]);
    return result.rows;
  }

  /**
   * Ensures the follow_relationships table exists
   */
  private async ensureFollowRelationshipsTable(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS follow_relationships (
        follower_did TEXT NOT NULL,
        followed_did TEXT NOT NULL,
        follow_uri TEXT NOT NULL,
        snapshotset TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (follower_did, followed_did, snapshotset)
      );
      
      CREATE INDEX IF NOT EXISTS idx_follow_rel_follower ON follow_relationships(follower_did);
      CREATE INDEX IF NOT EXISTS idx_follow_rel_followed ON follow_relationships(followed_did);
    `);
  }

  /**
   * Store a follow relationship
   */
  async storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void> {
    await this.ensureFollowRelationshipsTable();

    await this.client.query(
      `INSERT INTO follow_relationships (
        follower_did, followed_did, follow_uri, snapshotset, created_at
      ) VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (follower_did, followed_did, snapshotset) 
      DO UPDATE SET follow_uri = $3, created_at = NOW()`,
      [followerDid, followedDid, followUri, snapshotSet]
    );
  }

  /**
   * Get followers for a DID
   */
  async getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<{ followerDid: string; followUri: string }>> {
    let query = `
      SELECT follower_did, follow_uri
      FROM follow_relationships
      WHERE followed_did = $1
    `;

    let params = [targetDid];

    // If a specific snapshot set is requested, add it to the query
    if (snapshotSet) {
      query += ` AND snapshotset = $2`;
      params.push(snapshotSet);
    }

    // Option to get latest followers based on created_at timestamp
    query += ` ORDER BY created_at DESC`;

    try {
      const result = await this.client.query(query, params);
      return result.rows.map((row) => ({
        followerDid: row.follower_did,
        followUri: row.follow_uri,
      }));
    } catch (error) {
      console.error("Error querying followers:", error);
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
    try {
      const query = `
        SELECT 
          snapshotset as id, 
          created_at as "createdAt"
        FROM 
          snapshotsets 
        ORDER BY 
          created_at DESC
      `;

      const result = await this.client.query(query);
      return result.rows;
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
      // Begin transaction
      await this.client.query("BEGIN");

      // Delete the snapshot - cascading deletes should handle associated records
      const result = await this.client.query("DELETE FROM snapshotsets WHERE snapshotset = $1", [snapshotId]);

      // Clean up any orphaned records
      // (handles cases where foreign keys might not be properly set up)
      const tables = await this.getTableNames();
      for (const table of tables) {
        if (table !== "snapshotsets") {
          await this.client.query(`DELETE FROM ${table} WHERE snapshotset = $1`, [snapshotId]);
        }
      }

      await this.client.query("COMMIT");
      return result.rowCount !== null && result.rowCount > 0;
    } catch (error) {
      await this.client.query("ROLLBACK");
      console.error("Error deleting snapshot:", error);
      return false;
    }
  }

  /**
   * Export a snapshot to a portable format
   */
  async exportSnapshot(snapshotId: string): Promise<object | null> {
    try {
      // Check if snapshot exists
      const snapshotCheck = await this.client.query("SELECT * FROM snapshotsets WHERE snapshotset = $1", [snapshotId]);

      if (snapshotCheck.rowCount === 0) {
        return null;
      }

      const snapshotData: any = {
        id: snapshotId,
        createdAt: snapshotCheck.rows[0].created_at,
        tables: {},
      };

      // Get all tables
      const tables = await this.getTableNames();

      // For each table, export records associated with this snapshot
      for (const table of tables) {
        if (table !== "snapshotsets") {
          const records = await this.client.query(`SELECT * FROM ${table} WHERE snapshotset = $1`, [snapshotId]);

          snapshotData.tables[table] = records.rows;
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

    const createdAt = snapshotData.createdAt || new Date().toISOString();

    try {
      // Begin transaction
      await this.client.query("BEGIN");

      // Create snapshot entry
      await this.client.query(
        "INSERT INTO snapshotsets (snapshotset, created_at, modified_at) VALUES ($1, $2, $2) ON CONFLICT (snapshotset) DO NOTHING",
        [snapshotId, createdAt]
      );

      // Import data for each table
      for (const [tableName, records] of Object.entries(snapshotData.tables)) {
        const sanitizedTableName = this.sanitizeName(tableName);

        // Ensure table exists
        await this.ensureTableExists(sanitizedTableName);

        // Insert records
        for (const record of records as any[]) {
          // Construct query dynamically based on record fields
          const fields = Object.keys(record).filter((k) => k !== "id"); // Exclude id as it's handled separately
          const placeholders = fields.map((_, i) => `$${i + 3}`).join(", ");
          const fieldNames = fields.map((f) => this.sanitizeName(f)).join(", ");

          const query = `
            INSERT INTO ${sanitizedTableName} 
            (id, snapshotset, ${fieldNames}) 
            VALUES ($1, $2, ${placeholders})
            ON CONFLICT (id, snapshotset) DO UPDATE 
            SET ${fields.map((f, i) => `${this.sanitizeName(f)} = $${i + 3}`).join(", ")}
          `;

          const values = [record.id || record._id || `import-${nanoid()}`, snapshotId, ...fields.map((f) => record[f])];

          await this.client.query(query, values);
        }
      }

      await this.client.query("COMMIT");
      return true;
    } catch (error) {
      await this.client.query("ROLLBACK");
      console.error("Error importing snapshot:", error);
      return false;
    }
  }

  /**
   * Get a list of all tables in the database
   */
  private async getTableNames(): Promise<string[]> {
    const query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
    `;

    const result = await this.client.query(query);
    return result.rows.map((row) => row.table_name);
  }

  /**
   * Ensure a table exists with the required fields for snapshot storage
   */
  private async ensureTableExists(tableName: string): Promise<void> {
    const query = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id TEXT NOT NULL,
        snapshotset TEXT NOT NULL,
        version TEXT,
        data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        hash TEXT,
        PRIMARY KEY (id, snapshotset)
      )
    `;

    await this.client.query(query);
  }

  /**
   * Create a new snapshot with an optional name
   */
  async createSnapshot(name?: string): Promise<string> {
    const snapshotId = name || nanoid();
    const createdAt = new Date().toISOString();

    await this.client.query(
      `INSERT INTO snapshotsets (snapshotset, created_at, modified_at) 
       VALUES ($1, $2, $2)
       ON CONFLICT (snapshotset) DO NOTHING`,
      [snapshotId, createdAt]
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

/*

  --get ALL blocks for the user, will only need the subject here
  --also modify to return only uniques, with the latest...UGH we need a way to group stuff by when we gathered
SELECT
  l.id AS list_id,
  li.data->'subject' AS subject,
  'list_item' AS source
FROM
  public.list l
JOIN
  public.list_item li
ON
  li.data->>'list' = l.id
WHERE
  l.id LIKE 'at://did:plc:%'
  AND l.data->>'purpose' = 'app.bsky.graph.defs#modlist'

UNION ALL

-- Query for blocks
SELECT
  null AS list_id,
  b.data->'subject' AS subject,
  'blocks' AS source
FROM
  public.block b
WHERE
  b.id LIKE 'at://did:plc:%'

  */
