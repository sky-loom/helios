import { DataStore } from "../services/DataStore.js";

/**
 * EntryDB represents the database storage model for entries.
 * This class contains metadata about records stored from Bluesky,
 * allowing for historical snapshots and relationship tracking.
 */
export class EntryDB {
  /**
   * The project this record belongs to
   * Note: This could be used to organize entries into logical groups
   */
  project: string = "";

  /**
   * List of references to other entries this entry is linked to
   * Enables tracking relationships between different records
   */
  linkedTo: string[] = [];

  /**
   * Timestamp when this record was captured from Bluesky
   * Allows for tracking the exact time when this snapshot was taken
   */
  recordedAt: number;

  /**
   * DID of the entity that recorded this data
   * Tracks the identity of who/what captured this data
   */
  recorder: string;

  /**
   * The actual record data stored as a string
   * Contains the serialized version of the Bluesky data
   */
  record: string;

  /**
   * URI or DID that uniquely identifies this record
   * Serves as the primary key for the record
   */
  identity: string;

  /**
   * Type of record (e.g. "profile", "post", "like")
   * Used for categorizing different types of Bluesky data
   */
  recordType: string;

  /**
   * Optional version identifier for this record
   * Enables versioning to track changes over time
   */
  recordVersion?: string;

  /**
   * Creates a new EntryDB instance
   *
   * @param record - The record data as a string
   * @param linkedTo - Optional relation to another record
   * @param recordedAt - Timestamp when this record was captured
   * @param recorder - DID of the entity that recorded this data
   * @param identity - URI or DID that identifies this record
   * @param recordType - Type of record
   * @param recordVersion - Optional version identifier
   */
  constructor(
    record: string,
    linkedTo: string,
    recordedAt: number,
    recorder: string,
    identity: string,
    recordType: string,
    recordVersion?: string
  ) {
    // Initialize linkedTo as an array with the provided link if not empty
    if (linkedTo !== "") this.linkedTo.push(linkedTo);

    this.recordedAt = recordedAt;
    this.recorder = recorder;
    this.record = record;
    this.identity = identity;
    this.recordType = recordType;
    this.recordVersion = recordVersion;
  }
}

/**
 * Entry is a generic wrapper class for data models from Bluesky.
 * It's designed to capture snapshots of data over time, track changes,
 * and maintain context about who requested the data and for which project.
 *
 * @template T - The type of the Bluesky data model being wrapped (e.g. Profile, Post)
 */
export class Entry<T> {
  /**
   * The project this record belongs to
   * Groups entries into logical collections for research or analysis purposes
   */
  project: string = "";

  /**
   * List of references to other entries this entry is linked to
   * Tracks relationships between different Bluesky records
   */
  linkedTo: string[] = [];

  /**
   * Timestamp when this record was captured from Bluesky
   * Critical for understanding when this data snapshot was taken
   */
  recordedAt: number;

  /**
   * DID of the entity that recorded this data
   * Identifies who or what system captured this snapshot
   */
  recorder: string;

  /**
   * The actual Bluesky data model
   * Contains the structured data from Bluesky (e.g. a Profile or Post)
   */
  record!: T;

  /**
   * Type of record (e.g. "profile", "post", "like")
   * Categorizes the data for easier querying and organization
   */
  recordType: string;

  /**
   * URI or DID that uniquely identifies this record
   * Acts as the primary key for the record
   */
  identity: string;

  /**
   * Optional version identifier for this record
   * Enables tracking changes to the same record over time
   */
  recordVersion?: string;

  /**
   * Reference to the data store, if used for lazy loading
   * Enables fetching the actual record data when needed
   */
  private ds: DataStore | undefined;

  /**
   * Creates a new Entry instance
   *
   * @param entrydb - The database entry metadata
   * @param data - Either the record data or a DataStore for lazy loading
   */
  constructor(entrydb: EntryDB, data: T | DataStore) {
    // Copy metadata from EntryDB
    this.linkedTo = entrydb.linkedTo;
    this.recordedAt = entrydb.recordedAt;
    this.recorder = entrydb.recorder;
    this.identity = entrydb.identity;
    this.recordType = entrydb.recordType;
    this.recordVersion = entrydb.recordVersion;

    // Handle either direct data or lazy loading via DataStore
    if (data instanceof DataStore) {
      this.ds = data as DataStore;
    } else {
      this.record = data;
    }
  }

  /**
   * Static factory method to create an Entry instance with async initialization
   * This is needed because constructor cannot be async
   *
   * @param entryDB - The database entry metadata
   * @param data - Either the record data or a DataStore for lazy loading
   * @returns A Promise resolving to the initialized Entry instance
   */
  static async create<T>(entryDB: EntryDB, data: T | DataStore) {
    const instance = new Entry<T>(entryDB, data);

    // If DataStore is provided, initialize the record asynchronously
    if (data instanceof DataStore) {
      await instance.initialize();
    }

    return instance;
  }

  /**
   * Private method to initialize the record from DataStore if needed
   * Allows lazy loading of data from the data store
   */
  private async initialize() {
    // Fetch the data from the DataStore if it's available
    let data = await this.ds?.fetch<T>(this.recordType, this.identity, this.recordVersion);
    if (data) {
      this.record = data;
    }
  }
}
