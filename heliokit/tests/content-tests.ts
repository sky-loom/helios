#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import path from "path";

const execAsync = promisify(exec);

// Test configuration - edit these values as needed
const TEST_CONFIG = {
  // Account identifiers for testing
  testHandle: "rhalin.bsky.social",
  testDID: "did:plc:moykpa7c7np5xyxt4xhdseji",
  // Post/thread URIs for testing
  testPostURI: "at://did:plc:moykpa7c7np5xyxt4xhdseji/app.bsky.feed.post/3lnkrqaj73k2d",
  testThreadURI: "at://did:plc:moykpa7c7np5xyxt4xhdseji/app.bsky.feed.post/3lnj4lpdrh42i",
  // Image CID for testing
  testImageCID: "bafkreiexamplecidstring",
  // Sample test directory for output
  outputDir: "./test-output/content",
};

// Ensure the output directory exists
async function ensureOutputDir() {
  try {
    await fs.mkdir(TEST_CONFIG.outputDir, { recursive: true });
    console.log(`Output directory created: ${TEST_CONFIG.outputDir}`);
  } catch (err) {
    console.error(`Error creating output directory: ${err}`);
  }
}

// Helper function to run a command and log the result
async function runCommand(command: string, description: string): Promise<void> {
  console.log(`\n=== Testing: ${description} ===`);
  console.log(`Command: ${command}`);

  try {
    const { stdout, stderr } = await execAsync(command);

    if (stderr) {
      console.error(`Error: ${stderr}`);
    }

    // Truncate very long output
    const truncated = stdout.length > 500 ? stdout.substring(0, 500) + "... [output truncated]" : stdout;

    console.log(`Result: ${truncated}`);
    console.log(`Test completed: ${description}`);
    return Promise.resolve();
  } catch (error) {
    console.error(`Test failed for: ${description}`);
    console.error(error);
    return Promise.resolve(); // Continue with other tests even after failure
  }
}

// Main test function
async function runContentTests() {
  console.log("=== Starting Bluesky Toolkit Content Tests ===");
  await ensureOutputDir();

  // Create a snapshot for our tests
  const testSnapshot = `test-snapshot-${Date.now()}`;

  // Test content post command
  await runCommand(
    `node ./dist/index.js content post ${TEST_CONFIG.testPostURI} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "post.json"
    )}`,
    "Get Post by URI"
  );

  // Test content post command with dry run
  await runCommand(
    `node ./dist/index.js content post ${TEST_CONFIG.testPostURI} --snapshot ${testSnapshot} --dry-run -o ${path.join(
      TEST_CONFIG.outputDir,
      "post-dry-run.json"
    )}`,
    "Get Post by URI (Dry Run)"
  );

  // Test content posts command
  await runCommand(
    `node ./dist/index.js content posts ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "posts.json"
    )}`,
    "Get Posts for User"
  );

  // Test content posts command with DID
  await runCommand(
    `node ./dist/index.js content posts ${TEST_CONFIG.testDID} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "posts-did.json"
    )}`,
    "Get Posts for User Using DID"
  );

  // Test content posts command with dry run
  await runCommand(
    `node ./dist/index.js content posts ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} --dry-run -o ${path.join(
      TEST_CONFIG.outputDir,
      "posts-dry-run.json"
    )}`,
    "Get Posts for User (Dry Run)"
  );

  // Test content thread command
  await runCommand(
    `node ./dist/index.js content thread ${TEST_CONFIG.testThreadURI} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "thread.json"
    )}`,
    "Get Thread by URI"
  );

  // Test content thread command with dry run
  await runCommand(
    `node ./dist/index.js content thread ${TEST_CONFIG.testThreadURI} --snapshot ${testSnapshot} --dry-run -o ${path.join(
      TEST_CONFIG.outputDir,
      "thread-dry-run.json"
    )}`,
    "Get Thread by URI (Dry Run)"
  );

  // Test content image command
  await runCommand(
    `node ./dist/index.js content image ${TEST_CONFIG.testDID} ${TEST_CONFIG.testImageCID} -o ${path.join(
      TEST_CONFIG.outputDir,
      "image.jpg"
    )}`,
    "Get Image by DID and CID"
  );

  // Test content with context
  await runCommand(
    `node ./dist/index.js content post ${TEST_CONFIG.testPostURI} --context test-context --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "post-context.json"
    )}`,
    "Get Post by URI with Context"
  );

  // Test content with custom labelers
  await runCommand(
    `node ./dist/index.js content post ${
      TEST_CONFIG.testPostURI
    } --labelers did:plc:wkoofae5uytcm7bjncmev6n6,did:plc:4ugewi6aca52a62u62jccbl7 --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "post-labelers.json"
    )}`,
    "Get Post by URI with Custom Labelers"
  );

  console.log("\n=== Content Tests Completed ===");
}

// Run the tests
runContentTests().catch((err) => {
  console.error("Error running content tests:", err);
  process.exit(1);
});
