#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import path from "path";
import chalk from "chalk";

const execAsync = promisify(exec);

// Test configuration - edit these values as needed
const TEST_CONFIG = {
  // Account identifiers for testing
  testHandle: "rhalin.bsky.social",
  testDID: "did:plc:moykpa7c7np5xyxt4xhdseji",
  // Sample test directory for output
  outputDir: "./test-output/social",
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
  console.log(chalk.green(`\n=== Testing: ${description} ===`));
  console.log(chalk.cyanBright(`Command: ${command}`));

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
async function runSocialTests() {
  console.log("=== Starting Bluesky Toolkit Social Tests ===");
  await ensureOutputDir();

  // Create a snapshot for our tests
  const testSnapshot = `test-snapshot-${Date.now()}`;

  // Test social profile command
  await runCommand(
    `node ./dist/index.js social profile ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "profile.json"
    )}`,
    "Get User Profile"
  );

  // Test social profile command with DID
  await runCommand(
    `node ./dist/index.js social profile ${TEST_CONFIG.testDID} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "profile-did.json"
    )}`,
    "Get User Profile Using DID"
  );

  // Test social profile command with dry run
  await runCommand(
    `node ./dist/index.js social profile ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} --dry-run -o ${path.join(
      TEST_CONFIG.outputDir,
      "profile-dry-run.json"
    )}`,
    "Get User Profile (Dry Run)"
  );

  // Test social followers command
  await runCommand(
    `node ./dist/index.js social followers ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "followers.json"
    )}`,
    "Get User Followers"
  );

  // Test social followers command with page count
  await runCommand(
    `node ./dist/index.js social followers ${TEST_CONFIG.testHandle} --page-count 2 --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "followers-page2.json"
    )}`,
    "Get User Followers (2 Pages)"
  );

  // Test social follows command
  await runCommand(
    `node ./dist/index.js social follows ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "follows.json"
    )}`,
    "Get User Follows"
  );

  // Test social follows command with page count
  await runCommand(
    `node ./dist/index.js social follows ${TEST_CONFIG.testHandle} --page-count 2 --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "follows-page2.json"
    )}`,
    "Get User Follows (2 Pages)"
  );

  // Test social blocks command
  await runCommand(
    `node ./dist/index.js social blocks ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "blocks.json"
    )}`,
    "Get User Blocks"
  );

  // Test social blocklists command
  await runCommand(
    `node ./dist/index.js social blocklists ${TEST_CONFIG.testHandle} --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "blocklists.json"
    )}`,
    "Get User Blocklists"
  );

  // Test social with context
  await runCommand(
    `node ./dist/index.js social profile ${TEST_CONFIG.testHandle} --context test-context --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "profile-context.json"
    )}`,
    "Get User Profile with Context"
  );

  // Test social with custom labelers
  await runCommand(
    `node ./dist/index.js social profile ${
      TEST_CONFIG.testHandle
    } --labelers did:plc:wkoofae5uytcm7bjncmev6n6,did:plc:4ugewi6aca52a62u62jccbl7 --snapshot ${testSnapshot} -o ${path.join(
      TEST_CONFIG.outputDir,
      "profile-labelers.json"
    )}`,
    "Get User Profile with Custom Labelers"
  );

  console.log("\n=== Social Tests Completed ===");
}

// Run the tests
runSocialTests().catch((err) => {
  console.error("Error running social tests:", err);
  process.exit(1);
});
