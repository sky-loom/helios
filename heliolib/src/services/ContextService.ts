// contextservice.ts
import { Index as FlexSearchIndex } from "flexsearch";
import { ContextPersistenceHandler } from "../context/ContextPersistenceHandler.js";
import { Context } from "../context/Context.js";
import { SQLiteContextHandler } from "../context/SQLiteContextHandler.js";
import { InMemoryContextHandler } from "../context/InMemoryContextHandler.js";
//import { Context, ContextPersistenceHandler, InMemoryContextHandler, SQLiteContextHandler } from "./contextpersistence.js";

export class ContextService {
  private persistenceHandler: ContextPersistenceHandler;
  public selectedContext: string = "default";
  private currentContext?: Context;

  constructor(useSQLite: boolean = false, dbPath?: string) {
    if (useSQLite) {
      this.persistenceHandler = new SQLiteContextHandler(dbPath);
    } else {
      this.persistenceHandler = new InMemoryContextHandler();
    }
  }

  async initialize(): Promise<void> {
    await this.persistenceHandler.initialize();
  }

  async GetContext(name: string = "default"): Promise<Context> {
    if (this.currentContext?.name === name) {
      return this.currentContext;
    }

    // Try to load from persistence
    const loadedContext = await this.persistenceHandler.loadContext(name);
    if (loadedContext) {
      this.currentContext = loadedContext;
      return loadedContext;
    }

    // Create new context if it doesn't exist
    const newContext: Context = {
      name: name,
      posts: new Map(),
      index: new FlexSearchIndex(),
      resultset: new Map(),
      threadCache: new Map(),
      threadFocus: new Map(),
      labelers: [], // Initialize with an empty array of labelers
    };

    // Save the new context
    await this.persistenceHandler.saveContext(newContext);
    this.currentContext = newContext;
    return newContext;
  }

  async SaveContext(): Promise<void> {
    if (this.currentContext) {
      await this.persistenceHandler.saveContext(this.currentContext);
    }
  }

  async ListContexts(): Promise<string[]> {
    return await this.persistenceHandler.listContexts();
  }

  async DeleteContext(name: string): Promise<boolean> {
    if (this.currentContext?.name === name) {
      this.currentContext = undefined;
    }
    return await this.persistenceHandler.deleteContext(name);
  }

  async SwitchContext(name: string): Promise<Context> {
    // Save current context if needed
    if (this.currentContext) {
      await this.persistenceHandler.saveContext(this.currentContext);
    }

    // Load the requested context
    this.selectedContext = name;
    return await this.GetContext(name);
  }
}
