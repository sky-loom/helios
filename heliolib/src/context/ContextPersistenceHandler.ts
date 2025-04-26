import { Context } from "./Context.js";

export interface ContextPersistenceHandler {
  saveContext(context: Context): Promise<void>;
  loadContext(name: string): Promise<Context | undefined>;
  listContexts(): Promise<string[]>;
  deleteContext(name: string): Promise<boolean>;
  initialize(): Promise<void>;
}
