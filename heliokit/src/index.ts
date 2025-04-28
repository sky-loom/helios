import { AtpAgent } from "@atproto/api";
import { Command } from "commander";
import fs from "fs";
import { nanoid } from "nanoid";
import path from "path";
import { BskyClientManager } from "@skyloom/blueskyclientmanager";
import { DataStore } from "@skyloom/helios";
import { RequestParams, ApiWrapper } from "@skyloom/helios";
import { getDIDFromATUri, PDSFromRepoDesc, rkeyFromUri, SafeDid } from "@skyloom/helios";
import { DataBrowser } from "@skyloom/helios";
import readline from "node:readline";
import { ContextService } from "@skyloom/helios";
import { SearchService } from "@skyloom/helios";
import { BaseCommandOptions } from "./BaseCommandOptions.js";
import SuperJSON from "superjson";

// Initialize agents
const publicAgent = new AtpAgent({
  service: "https://public.api.bsky.app",
});

// Main program setup
const program = new Command();
program
  .name("heliokit")
  .description("Command line tools for working with bluesky data")
  .version("1.0.0")
  .option("-i, --interactive", "Enter interactive mode")
  .option("-s, --sqlite [path]", "Use SQLite persistence for context data");

// Get options
const options = program.opts();
const useSQLite = !!options.sqlite;
const dbPath = typeof options.sqlite === "string" ? options.sqlite : undefined;

// Initialize services
// let ds: DataStore = new DataStore({
//   type: "postgresql",
//   postgresConfig: { connectionString: process.env.DATABASE_URL_BSKYTOOLS ?? "" },
// });

let ds: DataStore = new DataStore({
  type: "flatfile",
  flatfileConfig: {
    baseDir: "./data-test",
  },
});

// let ds = new DataStore({
//   type: "sqlite",
//   sqliteConfig: {
//     filename: "./test-output/data/database.sqlite",
//   },
// });

// Declare client variables
let bskyClient: BskyClientManager;
let toolClient: ApiWrapper;
let contextService = new ContextService(useSQLite, dbPath);
let searchService = new SearchService(contextService);
let db: DataBrowser;

// Initialize or reinitialize clients with labelers
function initializeClients(labelers?: string[]) {
  // Default DIDs if no labelers provided
  const defaultDIDs = ["did:plc:e4elbtctnfqocyfcml6h2lf7", "did:plc:4ugewi6aca52a62u62jccbl7", "did:plc:wkoofae5uytcm7bjncmev6n6"];

  // Use provided labelers or default DIDs
  const dids = labelers && labelers.length > 0 ? labelers : defaultDIDs;

  // Create new client instances
  bskyClient = new BskyClientManager(dids);
  toolClient = new ApiWrapper(bskyClient, ds, "Helios");
  db = new DataBrowser(ds, toolClient, contextService);
}

// let bskyClient = new BskyClientManager([
//   "did:plc:e4elbtctnfqocyfcml6h2lf7",
//   "did:plc:4ugewi6aca52a62u62jccbl7",
//   "did:plc:wkoofae5uytcm7bjncmev6n6",
// ]);

// Utility function to handle common command options
function addBaseOptions(command: Command): Command {
  return command
    .option("-o, --output <file>", "Output results to JSON file")
    .option("-d, --dry-run", "Execute command without storing data to database")
    .option("-c, --context <n>", "Specify context to use")
    .option("-s, --snapshot <n>", "Specify snapshot name")
    .option("--debug", "Enable debug output")
    .option("-l, --labelers <items>", "Specify labeler DIDs to use (comma-separated list)", (val) => val.split(","));
}

// Helper function to process base options
async function processBaseOptions(options: BaseCommandOptions): Promise<RequestParams> {
  const params = new RequestParams();

  // Set snapshot
  if (options.snapshot) {
    params.snapshotSet = options.snapshot;
  } else {
    params.snapshotSet = nanoid(); // Generate unique ID if not specified
  }

  // Set debug output
  if (options.debug) {
    params.debugOutput = true;
  }

  // Set dry run mode
  if (options.dryRun) {
    params.dryRun = true;
  }

  // Switch context if specified
  if (options.context) {
    await contextService.SwitchContext(options.context);
  }

  // Set labelers if provided
  if (options.labelers) {
    params.labelers = options.labelers;
    let context = await contextService.GetContext(options.context);
    if (context) {
      context.labelers = options.labelers; // Update context labelers
    }
  }

  // Initialize with default DIDs
  initializeClients(params.labelers || []);
  // Switch context if specified
  if (options.context) {
    await contextService.SwitchContext(options.context);
  }
  return params;
}

// Helper function for resolving identifier (handle or DID)
async function resolveIdentifier(identifier: string): Promise<string> {
  // If it looks like a DID, use it directly
  if (identifier.startsWith("did:")) {
    return identifier;
  }

  // Otherwise, try to resolve handle to DID
  const did = await toolClient.GetDidFromHandle(identifier);
  if (!did) {
    throw new Error(`Could not resolve identifier: ${identifier} - ${did}`);
  }

  return did;
}

// Helper to handle outputting results
function handleOutput(data: any, options: BaseCommandOptions): void {
  if (options.output) {
    const outputPath = path.resolve(options.output);
    fs.writeFileSync(outputPath, SuperJSON.stringify(data));
    console.log(`Data saved to ${outputPath}`);
  } else {
    console.log(SuperJSON.stringify(data));
  }
}

// Initialize context service
(async () => {
  await contextService.initialize();
})();

// Root level commands
// Login command
addBaseOptions(
  program
    .command("login <identifier> <password>")
    .description("Login to bluesky")
    .action(async (identifier: string, password: string, cmdOptions: BaseCommandOptions) => {
      try {
        const response = await publicAgent.login({
          identifier: identifier,
          password: password,
        });

        if (cmdOptions.output) {
          const outputPath = path.resolve(cmdOptions.output);
          fs.writeFileSync(outputPath, JSON.stringify(response.data, null, 2));
          console.log(`Login data saved to ${outputPath}`);
        } else {
          console.log("Login successful!");
          console.log("DID:", response.data.did);
          console.log("Handle:", response.data.handle);
        }
      } catch (error) {
        console.error("Login failed:", error);
      }
    })
);

// Exit command
program
  .command("exit")
  .description("Exit the interactive console")
  .action(() => {
    console.log("Exiting...");
    process.exit(0);
  });

// Account commands
const accountCommand = program.command("account").description("Account operations");

addBaseOptions(
  accountCommand
    .command("repo <identifier>")
    .description("Describe a repository for an identifier (handle or DID)")
    .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        const did = await resolveIdentifier(identifier);

        let repoDescription;
        repoDescription = await toolClient.GetRepoDescription(did, params);

        handleOutput(repoDescription, cmdOptions);
      } catch (error) {
        console.error("Error describing repository:", error);
      }
    })
);

addBaseOptions(
  accountCommand
    .command("download <identifier>")
    .description("Download a repository for an identifier (handle or DID)")
    .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        const did = await resolveIdentifier(identifier);

        await toolClient.DownloadAndStorePDS(did, params);
        console.log(`Repository for ${did} has been downloaded ${params.dryRun ? "(dry run)" : ""}`);
      } catch (error) {
        console.error("Error downloading repository:", error);
      }
    })
);

// Social commands
const socialCommand = program.command("social").description("Social graph operations");

addBaseOptions(
  socialCommand
    .command("profile <identifier>")
    .description("Get a user profile by identifier (handle or DID)")
    .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);

        let data;

        const profile = await toolClient.GetPublicProfile(identifier, params);
        data = profile.record;

        handleOutput(data, cmdOptions);
      } catch (error) {
        console.error("Error getting profile:", error);
      }
    })
);

addBaseOptions(
  socialCommand
    .command("followers <identifier>")
    .description("Get followers for an identifier (handle or DID)")
    .option("-p, --page-count <count>", "Number of pages to fetch", "1")
    .action(async (identifier: string, cmdOptions: any) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        params.pageCount = parseInt(cmdOptions.pageCount);
        const did = await resolveIdentifier(identifier);

        let followers;

        followers = await toolClient.GetFollowers(did, params);

        handleOutput(followers, cmdOptions);
      } catch (error) {
        console.error("Error getting followers:", error);
      }
    })
);

addBaseOptions(
  socialCommand
    .command("follows <identifier>")
    .description("Get accounts an identifier follows (handle or DID)")
    .option("-p, --page-count <count>", "Number of pages to fetch", "1")
    .action(async (identifier: string, cmdOptions: any) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        params.pageCount = parseInt(cmdOptions.pageCount);
        const did = await resolveIdentifier(identifier);

        let follows;

        follows = await toolClient.GetFollows(did, params);

        handleOutput(follows, cmdOptions);
      } catch (error) {
        console.error("Error getting follows:", error);
      }
    })
);

addBaseOptions(
  socialCommand
    .command("blocks <identifier>")
    .description("Get blocks for an identifier (handle or DID)")
    .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        const did = await resolveIdentifier(identifier);

        let blocks;

        blocks = await toolClient.GetUserBlocks(did, params);

        handleOutput(blocks, cmdOptions);
      } catch (error) {
        console.error("Error getting blocks:", error);
      }
    })
);

addBaseOptions(
  socialCommand
    .command("blocklists <identifier>")
    .description("Get blocklists for an identifier (handle or DID)")
    .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        const did = await resolveIdentifier(identifier);

        let blocklists;

        blocklists = await toolClient.GetSubcribedBlockLists(did, params);

        handleOutput(blocklists, cmdOptions);
      } catch (error) {
        console.error("Error getting blocklists:", error);
      }
    })
);

// addBaseOptions(
//   socialCommand
//     .command("lists <identifier>")
//     .description("Get lists for an identifier (handle, DID, or URI)")
//     .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
//       try {
//         const params = await processBaseOptions(cmdOptions);
//         let did;

//         // Check if it's a URI
//         if (identifier.startsWith("at://")) {
//           did = getDIDFromATUri(identifier);
//         } else {
//           did = await resolveIdentifier(identifier);
//         }

//         if (did) {
//           let lists;
//           if (params.dryRun) {
//             const repo = await publicAgent.com.atproto.repo.describeRepo({
//               repo: did,
//             });

//             const pds = PDSFromRepoDesc(repo.data);
//             if (pds) {
//               let agent = new AtpAgent({
//                 service: pds,
//               });

//               const { data } = await agent.com.atproto.repo.listRecords({
//                 repo: did,
//                 collection: "app.bsky.graph.list",
//               });

//               lists = data;
//             } else {
//               throw new Error(`Could not determine PDS for DID: ${did}`);
//             }
//           } else {
//             // ToDo: Implement proper list fetching in ApiWrapper
//             lists = await toolClient.GetUserLists(did, params);
//           }

//           handleOutput(lists, cmdOptions);
//         } else {
//           throw new Error(`Invalid identifier: ${identifier}`);
//         }
//       } catch (error) {
//         console.error("Error getting lists:", error);
//       }
//     })
// );

// Content commands
const contentCommand = program.command("content").description("Content operations");

addBaseOptions(
  contentCommand
    .command("post <uri>")
    .description("Get a post by URI")
    .action(async (uri: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);

        let post;

        post = await toolClient.GetPost(uri, params);

        handleOutput(post, cmdOptions);
      } catch (error) {
        console.error("Error getting post:", error);
      }
    })
);

addBaseOptions(
  contentCommand
    .command("posts <identifier>")
    .description("Get posts for an identifier (handle or DID)")
    .action(async (identifier: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);
        const did = await resolveIdentifier(identifier);

        let posts;

        posts = await db.GetPosts(did);
        console.log(posts);

        handleOutput(posts, cmdOptions);
      } catch (error) {
        console.error("Error getting posts:", error);
      }
    })
);

addBaseOptions(
  contentCommand
    .command("thread <uri>")
    .description("Get a thread by URI")
    .action(async (uri: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);

        let thread;

        thread = await db.GetThread(uri, params);

        handleOutput(thread, cmdOptions);
      } catch (error) {
        console.error("Error getting thread: " + uri + " ", error);
      }
    })
);

// addBaseOptions(
//   contentCommand
//     .command("record <uri> <collection>")
//     .description("Get a record by URI and collection")
//     .action(async (uri: string, collection: string, cmdOptions: BaseCommandOptions) => {
//       try {
//         const params = await processBaseOptions(cmdOptions);
//         let did = getDIDFromATUri(uri);
//         let rkey = rkeyFromUri(uri);

//         if (did && rkey) {
//           let record;
//           if (params.dryRun) {
//             const repo = await publicAgent.com.atproto.repo.describeRepo({
//               repo: did,
//             });

//             const pds = PDSFromRepoDesc(repo.data);
//             if (pds) {
//               let agent = new AtpAgent({
//                 service: pds,
//               });

//               record = await agent.com.atproto.repo.getRecord({
//                 repo: did,
//                 collection: collection,
//                 rkey: rkey,
//               });
//             } else {
//               throw new Error(`Could not determine PDS for DID: ${did}`);
//             }
//           } else {
//             // ToDo: Implement generic record getter in ApiWrapper
//             record = await toolClient.GetRecord(uri, collection, params);
//           }

//           handleOutput(record, cmdOptions);
//         } else {
//           throw new Error(`Invalid URI: ${uri}`);
//         }
//       } catch (error) {
//         console.error("Error getting record:", error);
//       }
//     })
// );

addBaseOptions(
  contentCommand
    .command("image <did> <cid>")
    .description("Get an image by DID and CID")
    .action(async (did: string, cid: string, cmdOptions: BaseCommandOptions) => {
      try {
        const params = await processBaseOptions(cmdOptions);

        const data = await toolClient.GetImageBlob(did, cid, params);

        if (cmdOptions.output) {
          const outputPath = path.resolve(cmdOptions.output);
          fs.writeFileSync(outputPath, Buffer.from(data));
          console.log(`Image saved to ${outputPath}`);
        } else {
          // Default location if no output specified
          const fileName = `./images/${SafeDid(did)}_${cid}.jpg`;
          const dirPath = path.dirname(fileName);

          if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
          }

          fs.writeFileSync(fileName, Buffer.from(data));
          console.log(`Image saved to ${fileName}`);
        }
      } catch (error) {
        console.error("Error getting image:", error);
      }
    })
);

// Search commands
const searchCommand = program.command("search").description("Search operations");

addBaseOptions(
  searchCommand
    .command("posts <query>")
    .description("Search posts")
    .option("--method <method>", "Search method (text or semantic)", "text")
    .option("--did <did>", "DID for semantic search (required for semantic search)")
    .action(async (query: string, cmdOptions: any) => {
      try {
        await processBaseOptions(cmdOptions);

        let results;
        if (cmdOptions.method === "semantic") {
          if (!cmdOptions.did) {
            throw new Error("Semantic search requires a DID parameter (--did)");
          }
          results = await searchService.SemanticSearch(cmdOptions.did, query);
        } else {
          results = await searchService.TextSearch(query);
        }

        handleOutput(results, cmdOptions);
      } catch (error) {
        console.error("Error searching posts:", error);
      }
    })
);

addBaseOptions(
  searchCommand
    .command("index")
    .description("Index posts for search")
    .option("--method <method>", "Indexing method (flex, chroma, or both)", "both")
    .option("--overwrite", "Overwrite existing indices")
    .action(async (cmdOptions: any) => {
      try {
        await processBaseOptions(cmdOptions);

        const options = {
          flexsearch: cmdOptions.method === "flex" || cmdOptions.method === "both",
          chroma: cmdOptions.method === "chroma" || cmdOptions.method === "both",
          overwrite: cmdOptions.overwrite || false,
        };

        await searchService.EmbedAndIndexContext(options);
        console.log("Indexing complete");
      } catch (error) {
        console.error("Error indexing posts:", error);
      }
    })
);

addBaseOptions(
  searchCommand
    .command("load")
    .description("Load search index")
    .action(async (cmdOptions: BaseCommandOptions) => {
      try {
        await processBaseOptions(cmdOptions);

        await searchService.ImportFlexSearchIndex(contextService.selectedContext);
        console.log(`Search index loaded for context: ${contextService.selectedContext}`);
      } catch (error) {
        console.error("Error loading search index:", error);
      }
    })
);

// Context commands
const contextCommand = program.command("context").description("Context operations");

contextCommand
  .command("list")
  .description("List all available contexts")
  .action(async () => {
    try {
      const contexts = await contextService.ListContexts();
      console.log("Available contexts:");
      contexts.forEach((ctx) => console.log(`- ${ctx}`));
    } catch (error) {
      console.error("Error listing contexts:", error);
    }
  });

contextCommand
  .command("save")
  .description("Save the current context")
  .action(async () => {
    try {
      await contextService.SaveContext();
      console.log(`Context '${contextService.selectedContext}' saved`);
    } catch (error) {
      console.error("Error saving context:", error);
    }
  });

contextCommand
  .command("use <name>")
  .description("Switch to a different context")
  .action(async (name: string) => {
    try {
      if (!name || name === "") {
        name = "default";
        console.log("Context name not provided. Set to " + name);
      }

      await contextService.SwitchContext(name);
      console.log(`Switched to context: ${name}`);
    } catch (error) {
      console.error("Error switching context:", error);
    }
  });

contextCommand
  .command("delete <name>")
  .description("Delete a context")
  .action(async (name: string) => {
    try {
      const result = await contextService.DeleteContext(name);

      if (result) {
        console.log(`Context '${name}' deleted`);
      } else {
        console.log(`Context '${name}' not found`);
      }
    } catch (error) {
      console.error("Error deleting context:", error);
    }
  });

// Snapshot commands
const snapshotCommand = program.command("snapshot").description("Snapshot operations");

snapshotCommand
  .command("list")
  .description("List all available snapshots")
  .action(async () => {
    try {
      const snapshots = await ds.listSnapshots();
      console.log("Available snapshots:");
      snapshots.forEach((snapshot) => {
        console.log(`- ${snapshot.id} (created: ${snapshot.createdAt})`);
      });
    } catch (error) {
      console.error("Error listing snapshots:", error);
    }
  });

snapshotCommand
  .command("create <name>")
  .description("Create a new snapshot")
  .action(async (name: string) => {
    try {
      const id = await ds.createSnapshot(name);
      console.log(`Created snapshot: ${id}`);
    } catch (error) {
      console.error("Error creating snapshot:", error);
    }
  });

snapshotCommand
  .command("use <id>")
  .description("Select a snapshot for subsequent commands")
  .action(async (id: string) => {
    try {
      // Validate snapshot exists
      const snapshots = await ds.listSnapshots();
      const exists = snapshots.some((snapshot) => snapshot.id === id);

      if (exists) {
        // Store in global state or environment variable
        process.env.BSKY_TOOLKIT_SNAPSHOT = id;
        console.log(`Selected snapshot: ${id}`);
      } else {
        console.log(`Snapshot '${id}' not found`);
      }
    } catch (error) {
      console.error("Error selecting snapshot:", error);
    }
  });

snapshotCommand
  .command("delete <id>")
  .description("Delete a snapshot")
  .action(async (id: string) => {
    try {
      const success = await ds.deleteSnapshot(id);

      if (success) {
        console.log(`Snapshot '${id}' deleted`);
      } else {
        console.log(`Snapshot '${id}' not found or could not be deleted`);
      }
    } catch (error) {
      console.error("Error deleting snapshot:", error);
    }
  });

snapshotCommand
  .command("export <id>")
  .option("-o, --output <file>", "Output file path", "snapshot-export.json")
  .description("Export a snapshot to a file")
  .action(async (id: string, options) => {
    try {
      const snapshot = await ds.exportSnapshot(id);

      if (snapshot) {
        const outputPath = path.resolve(options.output);
        fs.writeFileSync(outputPath, JSON.stringify(snapshot, null, 2));
        console.log(`Snapshot exported to ${outputPath}`);
      } else {
        console.log(`Snapshot '${id}' not found`);
      }
    } catch (error) {
      console.error("Error exporting snapshot:", error);
    }
  });

snapshotCommand
  .command("import <file>")
  .option("-t, --target <target>", "Specify a custom target ID for the imported snapshot")
  .description("Import a snapshot from a file")
  .action(async (file: string, options) => {
    try {
      const filePath = path.resolve(file);
      const snapshotData = JSON.parse(fs.readFileSync(filePath, "utf8"));
      const id = options.target || nanoid();

      const success = await ds.importSnapshot(id, snapshotData);

      if (success) {
        console.log(`Snapshot imported as '${id}'`);
      } else {
        console.log("Failed to import snapshot");
      }
    } catch (error) {
      console.error("Error importing snapshot:", error);
    }
  });

snapshotCommand
  .command("duplicate <sourceId>")
  .option("-t, --target <target>", "Specify a custom target ID for the duplicate snapshot")
  .description("Duplicate a snapshot")
  .action(async (sourceId: string, options) => {
    try {
      const targetId = options.target || `copy-${sourceId}-${nanoid(6)}`;
      const newId = await ds.duplicateSnapshot(sourceId, targetId);

      if (newId) {
        console.log(`Snapshot '${sourceId}' duplicated as '${newId}'`);
      } else {
        console.log(`Snapshot '${sourceId}' not found or could not be duplicated`);
      }
    } catch (error) {
      console.error("Error duplicating snapshot:", error);
    }
  });

snapshotCommand
  .command("compare <id1> <id2>")
  .option("-o, --output <file>", "Output comparison to file")
  .description("Compare two snapshots")
  .action(async (id1: string, id2: string, options) => {
    try {
      const comparison = await ds.compareSnapshots(id1, id2);

      if (options.output) {
        const outputPath = path.resolve(options.output);
        fs.writeFileSync(outputPath, JSON.stringify(comparison, null, 2));
        console.log(`Comparison saved to ${outputPath}`);
      } else {
        console.log(comparison);
      }
    } catch (error) {
      console.error("Error comparing snapshots:", error);
    }
  });

// Handler for interactive mode
let isInteractive = false;

program
  .command("interactive")
  .description("Enter interactive mode")
  .action(async () => {
    isInteractive = true;
    await promptUser();
  });

// program.command("default", { isDefault: true }).action(async () => {
//   const options = program.opts();
//   if (options.interactive) {
//     if (isInteractive === false) {
//       isInteractive = true;
//       await promptUser();
//     } else {
//       console.log("INVALID COMMAND");
//     }
//   } else {
//     process.argv.push("-h");
//     program.parse(process.argv);
//   }
// });

// Interactive prompt function
async function promptUser() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "bsky> ",
  });

  rl.prompt();

  rl.on("line", async (line) => {
    const command = line.trim().split(" ");
    rl.close();

    try {
      await program.parseAsync(command, { from: "user" });
    } catch (error) {
      console.error("Error executing command:", error);
    }

    if (command[0] !== "exit") {
      await promptUser();
    } else {
      process.exit(0);
    }
  });
}
function forceExit() {
  // Allow any final logs to be printed
  //console.log("Forcing exit after command completion...");
  setTimeout(() => {
    //console.log("Exiting now...");
    process.exit(0);
  }, 100);
}

if (process.argv.length <= 2 && process.stdin.isTTY) {
  isInteractive = true;
  await promptUser(); // Don't await here
} else {
  await program
    .parseAsync(process.argv)
    .then(() => {
      // Force exit after command completes
      forceExit();
    })
    .catch((err) => {
      console.error("Error:", err);
      process.exit(1);
    });
}
// Parse arguments
//program.parse(process.argv);
