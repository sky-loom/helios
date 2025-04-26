#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import path from "path";

const execAsync = promisify(exec);

// Test configuration - edit these values as needed
const TEST_CONFIG = {
  // Account identifiers for testing
  testDID: "did:plc:moykpa7c7np5xyxt4xhdseji",
  // Search queries
  testSearchQuery: "ATProtato",
  testSemanticQuery: "potato",
  // Sample test directory for output
  outputDir: "./test-output/search",
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
async function runSearchTests() {
  console.log("=== Starting Bluesky Toolkit Search Tests ===");
  await ensureOutputDir();

  // Test search posts command with text search
  await runCommand(
    `node ./dist/index.js search posts "${TEST_CONFIG.testSearchQuery}" --method text -o ${path.join(
      TEST_CONFIG.outputDir,
      "search-text.json"
    )}`,
    "Search Posts with Text Search"
  );

  // Test search posts command with semantic search
  await runCommand(
    `node ./dist/index.js search posts "${TEST_CONFIG.testSemanticQuery}" --method semantic --did ${TEST_CONFIG.testDID} -o ${path.join(
      TEST_CONFIG.outputDir,
      "search-semantic.json"
    )}`,
    "Search Posts with Semantic Search"
  );

  // Test search with context
  await runCommand(
    `node ./dist/index.js search posts "${TEST_CONFIG.testSearchQuery}" --method text --context test-context -o ${path.join(
      TEST_CONFIG.outputDir,
      "search-text-context.json"
    )}`,
    "Search Posts with Text Search and Context"
  );

  // Test search index command with flex
  await runCommand(`node ./dist/index.js search index --method flex`, "Index Posts for Search with FlexSearch");

  // Test search index command with chroma
  await runCommand(`node ./dist/index.js search index --method chroma`, "Index Posts for Search with Chroma");

  // Test search index command with both
  await runCommand(`node ./dist/index.js search index --method both --overwrite`, "Index Posts for Search with Both Methods and Overwrite");

  // Test search load command
  await runCommand(`node ./dist/index.js search load`, "Load Search Index");

  // Test search load command with context
  await runCommand(`node ./dist/index.js search load --context test-context`, "Load Search Index with Context");

  console.log("\n=== Search Tests Completed ===");
}

// Run the tests
runSearchTests().catch((err) => {
  console.error("Error running search tests:", err);
  process.exit(1);
});
