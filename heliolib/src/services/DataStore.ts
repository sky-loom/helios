import { nanoid } from "nanoid";
import { promises as fs } from "fs";
import path from "path";
import pkg, { QueryResult } from "pg";
import crypto from "crypto";
import { Entry, EntryDB } from "../models/Entry.js";
import { AppBskyActorDefs, AppBskyFeedPost, ComAtprotoRepoDescribeRepo } from "@atproto/api";

type StorageConfig = {
  type: "postgresql" | "flatfile";
  postgresConfig?: {
    connectionString: string;
  };
  flatfileConfig?: {
    baseDir: string;
  };
};

interface DBEntry {
  id: string;
  created_at: number;
  version: string;
  snapshotset: string;
}

export class DataStore {
  private config: StorageConfig;
  private client!: pkg.Client;
  constructor(config: StorageConfig) {
    this.config = config;
    if (config.type === "flatfile" && !config.flatfileConfig) {
      throw new Error("Flatfile configuration is required for flatfile storage.");
    }

    if (config.type === "postgresql" && config.postgresConfig) {
      this.client = new pkg.Client({
        connectionString: config.postgresConfig?.connectionString,
      });
      //console.log("Connecting to PostgreSQL database..." + config.postgresConfig?.connectionString);
      this.client.connect();
      //throw new Error("PostgreSQL configuration is required for PostgreSQL storage.");
    }
  }

  async save(
    recordType: string,
    id: string,
    data: object,
    snapshotset: string,
    version?: string
  ): Promise<{ id: string; version: string; hash: string }> {
    const newVersion = version || nanoid();
    const createdAt = new Date().toISOString();
    const modifiedAt = createdAt;
    let hash = "";

    if (this.config.type === "postgresql") {
      const tableName = this.sanitizeName(recordType);
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

      const result = await this.client.query(`SELECT data FROM ${tableName} WHERE id = $1 ORDER BY modified_at DESC LIMIT 1`, [id]);

      if (result.rows.length > 0) {
        const previousDataHash = crypto.createHash("sha256").update(JSON.stringify(result.rows[0].data)).digest("hex");
        hash = crypto
          .createHash("sha256")
          .update(previousDataHash + JSON.stringify(data))
          .digest("hex");
      } else {
        hash = crypto.createHash("sha256").update(JSON.stringify(data)).digest("hex");
      }

      const extendedData = { ...data, hash };

      await this.client.query(
        `INSERT INTO ${tableName} (id, snapshotset, version, data, created_at, modified_at, hash) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [id, snapshotset, newVersion, extendedData, createdAt, modifiedAt, hash]
      );
    } else if (this.config.type === "flatfile") {
      const baseDir = this.config.flatfileConfig!.baseDir;
      const folderPath = path.join(baseDir, recordType);
      await fs.mkdir(folderPath, { recursive: true });

      const resultPath = path.join(folderPath, `${id}-latest.json`);
      try {
        const previousData = JSON.parse(await fs.readFile(resultPath, "utf-8"));
        const previousDataHash = crypto.createHash("sha256").update(JSON.stringify(previousData)).digest("hex");
        hash = crypto
          .createHash("sha256")
          .update(previousDataHash + JSON.stringify(data))
          .digest("hex");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
          throw error;
        }
        hash = crypto.createHash("sha256").update(JSON.stringify(data)).digest("hex");
      }

      const extendedData = { ...data, hash, createdAt, modifiedAt };
      const filePath = path.join(folderPath, `${id}-${newVersion}.json`);

      await fs.writeFile(filePath, JSON.stringify(extendedData, null, 2));
      await fs.writeFile(resultPath, JSON.stringify(extendedData, null, 2));
    }

    return { id, version: newVersion, hash };
  }
  async fetchLatestAvatar(did: string): Promise<string> {
    const query = ` SELECT data->>'avatar' AS avatar FROM profile WHERE 
id = '$1'
data->>'avatar' IS NOT NULL
ORDER BY modified_at DESC
LIMIT 1`;
    try {
      const result = await this.client.query(query, [did]);
      if (result.rows && result.rows.length > 0) {
        return JSON.parse(result.rows[0]?.avatar) as string;
      }
      console.log(result.rows);
    } catch (err) {
      console.error("Error executing query");
    }
    return "";
  }
  //todo: finish this
  async fetchListItems(list: string) {
    const query = ` SELECT * FROM list_item WHERE data->>'list' = $1 `;
    try {
      const result = await this.client.query(query, [list]);
      console.log(result.rows);
    } catch (err) {
      console.error("Error executing query");
    }
  }
  async fetchDID(handle: string): Promise<string | undefined> {
    if (this.config.type === "postgresql") {
      const formatted = "at://" + handle;

      const result = await this.client.query(
        `SELECT data FROM repo_description WHERE data->'didDoc'->'alsoKnownAs' @> $1 ORDER BY modified_at DESC LIMIT 1`,
        [`["${formatted}"]`]
      );
      if (result.rows && result.rowCount && result.rowCount > 0) {
        //console.log(result.rows[0].data);
        //console.log((result.rows[0].data as ComAtprotoRepoDescribeRepo.OutputSchema).did);
        return (result.rows[0].data as ComAtprotoRepoDescribeRepo.OutputSchema).did;
      }
    }
    return undefined;
  }
  async fetch<T>(recordType: string, id: string, version?: string): Promise<T | undefined> {
    if (this.config.type === "postgresql") {
      const tableName = this.sanitizeName(recordType);

      if (!version) {
        // Fetch the most recent version based on modified_at
        const result = await this.client.query(`SELECT data FROM ${tableName} WHERE id = $1 ORDER BY modified_at DESC LIMIT 1`, [id]);
        return result.rows[0]?.data || null;
      } else {
        const result = await this.client.query(`SELECT data FROM ${tableName} WHERE id = $1 AND version = $2`, [id, version]);
        return (result.rows[0]?.data as T) || undefined;
      }
    } else if (this.config.type === "flatfile") {
      const baseDir = this.config.flatfileConfig!.baseDir;
      const folderPath = path.join(baseDir, recordType);

      let filePath = path.join(folderPath, `${id}-${version || "latest"}.json`);

      if (!version) {
        // Use the 'latest' version if no version is provided
        filePath = path.join(folderPath, `${id}-latest.json`);
      }

      try {
        const data = await fs.readFile(filePath, "utf-8");
        return (JSON.parse(data) as T) || undefined;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
          throw error;
        }
        return undefined;
      }
    }

    return undefined;
  }
  async fetchEntry<T>(recordType: string, id: string, version?: string): Promise<Entry<T> | undefined> {
    let resultRecord: T | undefined;
    let row: any;
    if (this.config.type === "postgresql") {
      const tableName = this.sanitizeName(recordType);

      if (!version) {
        // Fetch the most recent version based on modified_at
        const result = await this.client.query(`SELECT * FROM ${tableName} WHERE id = $1 ORDER BY modified_at DESC LIMIT 1`, [id]);
        row = result.rows[0];
        resultRecord = result.rows[0]?.data || null;
      } else {
        const result = await this.client.query(`SELECT * FROM ${tableName} WHERE id = $1 AND version = $2`, [id, version]);
        row = result.rows[0];
        resultRecord = (result.rows[0]?.data as T) || undefined;
      }

      if (resultRecord && row) {
        //get entry
        const dbentry = row as DBEntry;
        //removed version, they don't match
        const query = `SELECT * FROM entry WHERE id = $1 AND snapshotset = $2 LIMIT 1`;
        const result = await this.client.query(query, [dbentry.id, dbentry.snapshotset]);
        console.log(result.rows);
        const data = result.rows[0]?.data as EntryDB;
        if (data) {
          //console.log("yay!");
          let entry = await Entry.create<T>(data, resultRecord);
          return entry;
        }
      }
    }

    return undefined;
  }
  async searchById(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    if (this.config.type === "postgresql") {
      return this.searchInPostgres(recordType, idPattern, fieldFilter);
    } else {
      return this.searchInFlatfile(recordType, idPattern, fieldFilter);
    }
  }

  private async getLatestSnapshotSet() {
    /* WITH unique_snapshotsets AS (
  SELECT DISTINCT snapshotset
  FROM public.post
)
SELECT s.snapshotset, s.created_at, s.modified_at
FROM unique_snapshotsets u
JOIN public.snapshotsets s
ON u.snapshotset = s.snapshotset
ORDER BY s.modified_at DESC
LIMIT 1;
 */
  }

  private async searchInPostgres(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    const tableName = this.sanitizeName(recordType);
    const query = fieldFilter
      ? `SELECT * FROM ${tableName} WHERE id LIKE $1 AND data->>$2 = $3`
      : `SELECT * FROM ${tableName} WHERE id LIKE $1`;
    const values = fieldFilter ? [idPattern, fieldFilter.field, fieldFilter.value] : [idPattern];

    const result = await this.client.query(query, values);
    return result.rows;
  }
  public async returnAllLatestEntries<T>(recordType: string, did: string, atID: boolean = true): Promise<Entry<T>[]> {
    const tableName = this.sanitizeName(recordType);
    const query = `SELECT p1.* FROM ${tableName} p1 JOIN ( SELECT id, MAX(modified_at) AS max_modified_at FROM ${tableName} WHERE id LIKE $1 GROUP BY id ) p2 ON p1.id = p2.id AND p1.modified_at = p2.max_modified_at;`;
    const id = atID ? "at://" + did + "/%" : did;
    const result = await this.client.query(query, [id]);
    let retVals: Entry<T>[] = [];
    //console.log(JSON.stringify(result.rows, null, 2));
    //get each entry
    for (const row of result.rows) {
      const dbentry = row as DBEntry;
      //removed version, they don't match
      const query = `SELECT * FROM entry WHERE id = $1 AND snapshotset = $2 LIMIT 1`;
      const result = await this.client.query(query, [dbentry.id, dbentry.snapshotset]);
      //console.log(result.rows);
      const data = result.rows[0]?.data as EntryDB;
      if (data) {
        //console.log("yay!");
        let entry = await Entry.create<T>(data, row.data as T);
        retVals.push(entry);
      }
    }
    return retVals;
  }
  public async returnAllLatestEntriesForThread(root_aturl: string): Promise<Entry<AppBskyFeedPost.Record>[]> {
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
);
`;
    const result = await this.client.query(query, [root_aturl]);
    let retVals: Entry<AppBskyFeedPost.Record>[] = [];
    //console.log(JSON.stringify(result.rows, null, 2));
    //get each entry
    for (const row of result.rows) {
      const dbentry = row as DBEntry;
      //removed version, they don't match
      const query = `SELECT * FROM entry WHERE id = $1 AND snapshotset = $2 LIMIT 1`;
      const result = await this.client.query(query, [dbentry.id, dbentry.snapshotset]);
      //console.log(result.rows);
      const data = result.rows[0]?.data as EntryDB;
      if (data) {
        //console.log("yay!");
        let entry = await Entry.create<AppBskyFeedPost.Record>(data, row.data as AppBskyFeedPost.Record);
        retVals.push(entry);
      }
    }
    return retVals;
  }
  public async searchInPostgresByAtDID(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    const tableName = this.sanitizeName(recordType);
    const query = fieldFilter
      ? `SELECT * FROM ${tableName} WHERE id LIKE 'at://$1/%' AND data->>$2 = $3`
      : `SELECT * FROM ${tableName} WHERE id LIKE 'at://$1/%'`;
    const values = fieldFilter ? [idPattern, fieldFilter.field, fieldFilter.value] : [idPattern];

    const result = await this.client.query(query, values);
    return result.rows;
  }

  private async searchInFlatfile(recordType: string, idPattern: string, fieldFilter?: { field: string; value: any }): Promise<any[]> {
    const indexPath = path.join(this.config.flatfileConfig!.baseDir, "index.json");
    const index = JSON.parse(await fs.readFile(indexPath, "utf-8"));

    const matchingIds = Object.keys(index).filter((id) => id.includes(idPattern));
    const results = [];

    for (const id of matchingIds) {
      const filePath = index[id];
      const content = JSON.parse(await fs.readFile(filePath, "utf-8"));

      if (!fieldFilter || content[fieldFilter.field] === fieldFilter.value) {
        results.push(content);
      }
    }

    return results;
  }

  public async getThread(uri: string) {
    const query = `WITH RECURSIVE parent_hierarchy AS (
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
SELECT * FROM reply_hierarchy;`;

    const result = await this.client.query(query, [uri]);
    return result.rows;
  }
  /**
   * Creates the follow_relationships table if it doesn't exist
   *
   * @private
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
   * Stores a follow relationship in the optimized table
   *
   * @public
   * @param {string} followerDid - The DID of the follower
   * @param {string} followedDid - The DID of the followed account
   * @param {string} followUri - The URI of the follow record
   * @param {string} snapshotSet - The snapshot set identifier
   */
  public async storeFollowRelationship(followerDid: string, followedDid: string, followUri: string, snapshotSet: string): Promise<void> {
    await this.ensureFollowRelationshipsTable();
    await this.client.query(
      `
    INSERT INTO follow_relationships (
      follower_did, followed_did, follow_uri, snapshotset, created_at
    ) VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (follower_did, followed_did, snapshotset) 
    DO UPDATE SET follow_uri = $3, created_at = NOW()
  `,
      [followerDid, followedDid, followUri, snapshotSet]
    );
  }
  /**
   * Retrieves all accounts that follow a specified user DID
   *
   * @param {string} targetDid - The DID of the user whose followers we want to find
   * @param {string} [snapshotSet] - Optional snapshot set to restrict results to
   * @returns {Promise<Array<{followerDid: string, followUri: string}>>} Array of follower DIDs and follow record URIs
   */
  async getFollowersForDid(targetDid: string, snapshotSet?: string): Promise<Array<{ followerDid: string; followUri: string }>> {
    if (this.config.type !== "postgresql") {
      throw new Error("This method is only implemented for PostgreSQL storage");
    }

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
   * Retrieves full profile entries for followers of a specified user
   *
   * @param {string} targetDid - The DID of the user whose followers we want to find
   * @param {string} [snapshotSet] - Optional snapshot set to restrict results to
   * @returns {Promise<Array<Entry<AppBskyActorDefs.ProfileViewDetailed>>>} Array of follower profile entries
   */
  async getFollowerProfilesForDid(targetDid: string, snapshotSet?: string): Promise<Array<Entry<AppBskyActorDefs.ProfileViewDetailed>>> {
    if (this.config.type !== "postgresql") {
      throw new Error("This method is only implemented for PostgreSQL storage");
    }

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

  // Add these methods to the DataStore class in datastore.ts

  /**
   * List all available snapshots
   *
   * @returns {Promise<Array<{id: string, createdAt: string}>>} List of snapshots with creation timestamps
   */
  async listSnapshots(): Promise<Array<{ id: string; createdAt: string }>> {
    if (this.config.type === "postgresql") {
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
    } else if (this.config.type === "flatfile") {
      try {
        // For flatfile storage, we need to parse directory structure
        const baseDir = this.config.flatfileConfig!.baseDir;
        const snapshotsDir = path.join(baseDir, "snapshots");

        // Check if snapshots directory exists
        if (
          !(await fs
            .access(snapshotsDir)
            .then(() => true)
            .catch(() => false))
        ) {
          return [];
        }

        // Read snapshot directories
        const dirents = await fs.readdir(snapshotsDir, { withFileTypes: true });
        const snapshotDirs = dirents.filter((dirent) => dirent.isDirectory()).map((dirent) => dirent.name);

        const snapshots = [];
        for (const id of snapshotDirs) {
          const metaFile = path.join(snapshotsDir, id, "meta.json");

          // Check if meta file exists
          const metaExists = await fs
            .access(metaFile)
            .then(() => true)
            .catch(() => false);

          if (metaExists) {
            const metaContent = await fs.readFile(metaFile, "utf8");
            const meta = JSON.parse(metaContent);
            snapshots.push({
              id,
              createdAt: meta.createdAt,
            });
          } else {
            // If no meta file, use folder creation time
            const stats = await fs.stat(path.join(snapshotsDir, id));
            snapshots.push({
              id,
              createdAt: stats.birthtime.toISOString(),
            });
          }
        }

        return snapshots.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
      } catch (error) {
        console.error("Error listing snapshots:", error);
        return [];
      }
    }

    return [];
  }

  /**
   * Delete a snapshot and all associated data
   *
   * @param {string} snapshotId - ID of the snapshot to delete
   * @returns {Promise<boolean>} Whether the deletion was successful
   */
  async deleteSnapshot(snapshotId: string): Promise<boolean> {
    if (this.config.type === "postgresql") {
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
    } else if (this.config.type === "flatfile") {
      try {
        const baseDir = this.config.flatfileConfig!.baseDir;
        const snapshotDir = path.join(baseDir, "snapshots", snapshotId);

        // Check if snapshot directory exists
        const dirExists = await fs
          .access(snapshotDir)
          .then(() => true)
          .catch(() => false);
        if (!dirExists) {
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

    return false;
  }

  /**
   * Export a snapshot to a portable format
   *
   * @param {string} snapshotId - ID of the snapshot to export
   * @returns {Promise<object | null>} Exported snapshot data or null if not found
   */
  async exportSnapshot(snapshotId: string): Promise<object | null> {
    if (this.config.type === "postgresql") {
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
    } else if (this.config.type === "flatfile") {
      try {
        const baseDir = this.config.flatfileConfig!.baseDir;
        const snapshotDir = path.join(baseDir, "snapshots", snapshotId);

        // Check if snapshot directory exists
        const dirExists = await fs
          .access(snapshotDir)
          .then(() => true)
          .catch(() => false);
        if (!dirExists) {
          return null;
        }

        const snapshotData: any = {
          id: snapshotId,
          tables: {},
        };

        // Get metadata
        const metaFile = path.join(snapshotDir, "meta.json");
        const metaExists = await fs
          .access(metaFile)
          .then(() => true)
          .catch(() => false);

        if (metaExists) {
          const metaContent = await fs.readFile(metaFile, "utf8");
          const meta = JSON.parse(metaContent);
          snapshotData.createdAt = meta.createdAt;
        }

        // Get all subdirectories (tables)
        const dirents = await fs.readdir(snapshotDir, { withFileTypes: true });
        const tableDirs = dirents.filter((dirent) => dirent.isDirectory()).map((dirent) => dirent.name);

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
            snapshotData.tables[table].push(fileData);
          }
        }

        return snapshotData;
      } catch (error) {
        console.error("Error exporting snapshot:", error);
        return null;
      }
    }

    return null;
  }

  /**
   * Import a snapshot from a portable format
   *
   * @param {string} snapshotId - ID to assign to the imported snapshot
   * @param {object} snapshotData - Snapshot data to import
   * @returns {Promise<boolean>} Whether the import was successful
   */
  async importSnapshot(snapshotId: string, snapshotData: any): Promise<boolean> {
    if (!snapshotData || !snapshotData.tables) {
      console.error("Invalid snapshot data format");
      return false;
    }

    const createdAt = snapshotData.createdAt || new Date().toISOString();

    if (this.config.type === "postgresql") {
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
    } else if (this.config.type === "flatfile") {
      try {
        const baseDir = this.config.flatfileConfig!.baseDir;
        const snapshotDir = path.join(baseDir, "snapshots", snapshotId);

        // Create snapshot directory
        await fs.mkdir(snapshotDir, { recursive: true });

        // Create metadata file
        const metaFile = path.join(snapshotDir, "meta.json");
        await fs.writeFile(
          metaFile,
          JSON.stringify({
            createdAt,
            importedAt: new Date().toISOString(),
          })
        );

        // Import data for each table
        for (const [tableName, records] of Object.entries(snapshotData.tables)) {
          const tableDir = path.join(snapshotDir, tableName);
          await fs.mkdir(tableDir, { recursive: true });

          // Write each record to a file
          for (const record of records as any[]) {
            const id = record.id || record._id || `import-${nanoid()}`;
            const filePath = path.join(tableDir, `${id}.json`);
            await fs.writeFile(filePath, JSON.stringify(record, null, 2));
          }
        }

        return true;
      } catch (error) {
        console.error("Error importing snapshot:", error);
        return false;
      }
    }

    return false;
  }

  /**
   * Get a list of all tables in the database
   *
   * @private
   * @returns {Promise<string[]>} List of table names
   */
  private async getTableNames(): Promise<string[]> {
    if (this.config.type === "postgresql") {
      const query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
    `;

      const result = await this.client.query(query);
      return result.rows.map((row) => row.table_name);
    }

    return [];
  }

  /**
   * Ensure a table exists with the required fields for snapshot storage
   *
   * @private
   * @param {string} tableName - Name of the table to create
   */
  private async ensureTableExists(tableName: string): Promise<void> {
    if (this.config.type === "postgresql") {
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
  }

  /**
   * Create a new snapshot with an optional name
   *
   * @param {string} [name] - Optional name for the snapshot, defaults to a generated ID
   * @returns {Promise<string>} The ID of the created snapshot
   */
  async createSnapshot(name?: string): Promise<string> {
    const snapshotId = name || nanoid();
    const createdAt = new Date().toISOString();

    if (this.config.type === "postgresql") {
      await this.client.query(
        `INSERT INTO snapshotsets (snapshotset, created_at, modified_at) 
       VALUES ($1, $2, $2)
       ON CONFLICT (snapshotset) DO NOTHING`,
        [snapshotId, createdAt]
      );
    } else if (this.config.type === "flatfile") {
      const baseDir = this.config.flatfileConfig!.baseDir;
      const snapshotDir = path.join(baseDir, "snapshots", snapshotId);

      await fs.mkdir(snapshotDir, { recursive: true });

      const metaFile = path.join(snapshotDir, "meta.json");
      await fs.writeFile(
        metaFile,
        JSON.stringify({
          createdAt,
          name: name || snapshotId,
        })
      );
    }

    return snapshotId;
  }

  /**
   * Duplicate a snapshot with a new ID
   *
   * @param {string} sourceSnapshotId - ID of the snapshot to copy
   * @param {string} [targetSnapshotId] - Optional ID for the new snapshot
   * @returns {Promise<string | null>} The ID of the created snapshot or null if the source doesn't exist
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

  /**
   * Compare two snapshots and return differences
   *
   * @param {string} snapshotId1 - First snapshot ID
   * @param {string} snapshotId2 - Second snapshot ID
   * @returns {Promise<object>} Object detailing the differences between snapshots
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
  //todo: flat files are gonna need some work
  // async addToFlatfile(id: string, data: any): Promise<void> {
  //   const baseDir = this.config.flatfileConfig!.baseDir;
  //   const filePath = path.join(baseDir, `${id}.json`);
  //   const indexPath = path.join(baseDir, "index.json");

  //   let index = {};
  //   try {
  //     index = JSON.parse(await fs.readFile(indexPath, "utf-8"));
  //   } catch {
  //     // Index file might not exist yet
  //   }

  //   index[id] = filePath;

  //   await fs.writeFile(filePath, JSON.stringify(data));
  //   await fs.writeFile(indexPath, JSON.stringify(index));
  // }
  private sanitizeName(name: string): string {
    return name.replace(/[^a-zA-Z0-9_]/g, "_");
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
}
