import { IDataStore, StorageConfig } from "./IDataStore.js";
import { PostgresDataStore } from "./PostgresDataStore.js";
import { FlatfileDataStore } from "./FlatfileDataStore.js";
import { SQLiteDataStore } from "./SQLiteDataStore.js";

/**
 * Factory class for creating DataStore instances based on configuration
 */
export class DataStoreFactory {
  /**
   * Create a DataStore instance based on the provided configuration
   *
   * @param config The storage configuration
   * @returns An instance of IDataStore
   */
  static createDataStore(config: StorageConfig): IDataStore {
    switch (config.type) {
      case "postgresql":
        if (!config.postgresConfig) {
          throw new Error("PostgreSQL configuration is required for PostgreSQL storage");
        }
        return new PostgresDataStore(config);

      case "flatfile":
        if (!config.flatfileConfig) {
          throw new Error("Flatfile configuration is required for flatfile storage");
        }
        return new FlatfileDataStore(config);

      case "sqlite":
        if (!config.sqliteConfig) {
          throw new Error("SQLite configuration is required for SQLite storage");
        }
        return new SQLiteDataStore(config);

      default:
        throw new Error(`Unsupported storage type: ${config.type}`);
    }
  }
}
