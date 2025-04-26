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
  // Sample test directory for output
  outputDir: "./test-output/account",
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
async function runAccountTests() {
  console.log("=== Starting Bluesky Toolkit Account Tests ===");
  await ensureOutputDir();

  // Create a snapshot for our tests
  const testSnapshot = `test-snapshot-${Date.now()}`;

  // Test account repo command - getting repository description
  await runCommand(
    `node ./dist/index.js account repo ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "repo.json"
    )}`,
    "Get Account Repository Description"
  );

  // Test account repo command with DID
  await runCommand(
    `node ./dist/index.js account repo ${TEST_CONFIG.testDID} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "repo-did.json"
    )}`,
    "Get Account Repository Description Using DID"
  );

  // Test account repo command with dry run
  await runCommand(
    `node ./dist/index.js account repo ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} --dry-run -o ${path.join(
      TEST_CONFIG.outputDir,
      "repo-dry-run.json"
    )}`,
    "Get Account Repository Description (Dry Run)"
  );

  // Test account download command
  // await runCommand(
  //   `node ./dist/index.js account download ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} --debug`,
  //   "Download Account Repository"
  // );

  // // Test account download command with DID
  // await runCommand(
  //   `node ./dist/index.js account download ${TEST_CONFIG.testDID} --snapshot ${testSnapshot} --debug`,
  //   "Download Account Repository Using DID"
  // );

  // // Test account download command with dry run
  // await runCommand(
  //   `node ./dist/index.js account download ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} --dry-run --debug`,
  //   "Download Account Repository (Dry Run)"
  // );

  console.log("\n=== Account Tests Completed ===");
}

// Run the tests
runAccountTests().catch((err) => {
  console.error("Error running account tests:", err);
  process.exit(1);
});
