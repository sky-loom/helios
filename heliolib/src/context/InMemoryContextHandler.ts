import { Context } from "./Context.js";
import { ContextPersistenceHandler } from "./ContextPersistenceHandler.js";

//default non-persistent context handler
export class InMemoryContextHandler implements ContextPersistenceHandler {
  private contexts: Map<string, Context> = new Map();

  async saveContext(context: Context): Promise<void> {
    this.contexts.set(context.name, context);
  }
  //  loadContext(name: string): Promise<Context | undefined>;
  async loadContext(name: string): Promise<Context | undefined> {
    return this.contexts.get(name);
  }

  async listContexts(): Promise<string[]> {
    return Array.from(this.contexts.keys());
  }

  async deleteContext(name: string): Promise<boolean> {
    return this.contexts.delete(name);
  }

  async initialize(): Promise<void> {
    // Nothing to initialize for in-memory storage
  }
}
