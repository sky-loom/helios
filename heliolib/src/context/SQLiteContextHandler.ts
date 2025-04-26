import sqlite3 from "sqlite3";
import { open, Database } from "sqlite";
import { ContextPersistenceHandler } from "./ContextPersistenceHandler.js";
import path from "path";
import fs from "fs";
import { Context } from "./Context.js";
import { Index as FlexSearchIndex } from "flexsearch";
// SQLite implementation
export class SQLiteContextHandler implements ContextPersistenceHandler {
  private db: Database | null = null;
  private dbPath: string;

  constructor(dbPath?: string) {
    this.dbPath = dbPath || path.join(process.cwd(), "bsky-context.db");
  }

  async initialize(): Promise<void> {
    // Ensure the directory exists
    const dbDir = path.dirname(this.dbPath);
    if (!fs.existsSync(dbDir)) {
      fs.mkdirSync(dbDir, { recursive: true });
    }

    // Initialize SQLite database
    this.db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database,
    });

    // Create tables if they don't exist
    await this.db.exec(`
        CREATE TABLE IF NOT EXISTS contexts (
          name TEXT PRIMARY KEY,
          labelers TEXT DEFAULT '[]',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS posts (
          context_name TEXT,
          uri TEXT,
          post_data TEXT,
          PRIMARY KEY (context_name, uri),
          FOREIGN KEY (context_name) REFERENCES contexts(name) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS search_indices (
          context_name TEXT PRIMARY KEY,
          index_data TEXT,
          FOREIGN KEY (context_name) REFERENCES contexts(name) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS thread_cache (
          context_name TEXT,
          uri TEXT,
          thread_data TEXT,
          PRIMARY KEY (context_name, uri),
          FOREIGN KEY (context_name) REFERENCES contexts(name) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS thread_focus (
          context_name TEXT,
          uri TEXT,
          focus_data TEXT,
          PRIMARY KEY (context_name, uri),
          FOREIGN KEY (context_name) REFERENCES contexts(name) ON DELETE CASCADE
        );
      `);
  }

  async saveContext(context: Context): Promise<void> {
    if (!this.db) throw new Error("Database not initialized");

    // Begin transaction
    await this.db.exec("BEGIN TRANSACTION");

    try {
      // Save context metadata
      await this.db.run(
        `INSERT INTO contexts (name, labelers, updated_at) 
           VALUES (?, ?, CURRENT_TIMESTAMP)
         ON CONFLICT(name) DO UPDATE SET updated_at = CURRENT_TIMESTAMP, labelers = ?`,
        context.name,
        JSON.stringify(context.labelers || []),
        JSON.stringify(context.labelers || [])
      );

      // Save posts
      for (const [uri, post] of context.posts.entries()) {
        await this.db.run(
          "INSERT OR REPLACE INTO posts (context_name, uri, post_data) VALUES (?, ?, ?)",
          context.name,
          uri,
          JSON.stringify(post)
        );
      }
      const exportedData: Record<string, string> = {};

      await context.index.export(async (key, data) => {
        exportedData[key] = data;
      });

      // Save search index
      await this.db.run(
        "INSERT OR REPLACE INTO search_indices (context_name, index_data) VALUES (?, ?)",
        context.name,
        JSON.stringify(exportedData)
      );

      // Save thread cache
      for (const [uri, thread] of context.threadCache.entries()) {
        await this.db.run(
          "INSERT OR REPLACE INTO thread_cache (context_name, uri, thread_data) VALUES (?, ?, ?)",
          context.name,
          uri,
          JSON.stringify(thread)
        );
      }

      // Save thread focus
      for (const [uri, focus] of context.threadFocus.entries()) {
        await this.db.run(
          "INSERT OR REPLACE INTO thread_focus (context_name, uri, focus_data) VALUES (?, ?, ?)",
          context.name,
          uri,
          JSON.stringify(focus)
        );
      }

      // Commit transaction
      await this.db.exec("COMMIT");
    } catch (error) {
      // Rollback on error
      await this.db.exec("ROLLBACK");
      throw error;
    }
  }

  async loadContext(name: string): Promise<Context | undefined> {
    if (!this.db) throw new Error("Database not initialized");

    // Check if context exists
    const contextData = await this.db.get("SELECT name, labelers FROM contexts WHERE name = ?", name);
    if (!contextData) return undefined;

    // Create context object
    const context: Context = {
      name,
      posts: new Map(),
      index: new FlexSearchIndex(),
      resultset: new Map(),
      threadCache: new Map(),
      threadFocus: new Map(),
      labelers: JSON.parse(contextData.labelers || "[]"), // Parse labelers from JSON
    };

    // Load posts
    const posts = await this.db.all("SELECT uri, post_data FROM posts WHERE context_name = ?", name);
    for (const post of posts) {
      context.posts.set(post.uri, JSON.parse(post.post_data));
    }

    // Load search index
    const indexData = await this.db.get("SELECT index_data FROM search_indices WHERE context_name = ?", name);
    if (indexData?.index_data) {
      const indexObj = JSON.parse(indexData.index_data);
      context.index.import("default", indexObj);
    }

    // Load thread cache
    const threadCache = await this.db.all("SELECT uri, thread_data FROM thread_cache WHERE context_name = ?", name);
    for (const thread of threadCache) {
      context.threadCache.set(thread.uri, JSON.parse(thread.thread_data));
    }

    // Load thread focus
    const threadFocus = await this.db.all("SELECT uri, focus_data FROM thread_focus WHERE context_name = ?", name);
    for (const focus of threadFocus) {
      context.threadFocus.set(focus.uri, JSON.parse(focus.focus_data));
    }

    // Set resultset to same as posts initially
    context.resultset = new Map(context.posts);

    return context;
  }

  async listContexts(): Promise<string[]> {
    if (!this.db) throw new Error("Database not initialized");

    const contexts = await this.db.all("SELECT name FROM contexts ORDER BY updated_at DESC");
    return contexts.map((c) => c.name);
  }

  async deleteContext(name: string): Promise<boolean> {
    if (!this.db) throw new Error("Database not initialized");

    await this.db.run("DELETE FROM contexts WHERE name = ?", name);
    return true;
  }

  async close(): Promise<void> {
    if (this.db) {
      await this.db.close();
      this.db = null;
    }
  }
}
