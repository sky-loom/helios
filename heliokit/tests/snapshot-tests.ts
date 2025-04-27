#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import path from "path";
import chalk from "chalk";

const execAsync = promisify(exec);

// Test configuration - edit these values as needed
const TEST_CONFIG = {
  // Snapshot names/IDs for testing
  testSnapshot: "test-snapshot",
  // Sample test directory for output
  outputDir: "./test-output/snapshot",
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
  console.log(chalk.yellow(`Command: ${command}`));

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
    console.error(chalk.red(`Test failed for: ${description}`));
    console.error(chalk.red(error));
    return Promise.resolve(); // Continue with other tests even after failure
  }
}

// Main test function
async function runSnapshotTests() {
  console.log("=== Starting Bluesky Toolkit Snapshot Tests ===");
  await ensureOutputDir();

  // Generate unique timestamp for this test run
  const timestamp = Date.now();
  const testSnapshotId = `${TEST_CONFIG.testSnapshot}-${timestamp}`;
  const duplicateSnapshotId = `duplicate-${testSnapshotId}`;

  // Test snapshot list command
  await runCommand(`node ./dist/index.js snapshot list`, "List All Snapshots");

  // Test snapshot create command
  await runCommand(`node ./dist/index.js snapshot create ${testSnapshotId}`, `Create Snapshot: ${testSnapshotId}`);

  // Test snapshot list command after creation
  await runCommand(`node ./dist/index.js snapshot list`, "List Snapshots After Creation");

  // Test snapshot use command
  await runCommand(`node ./dist/index.js snapshot use ${testSnapshotId}`, `Use Snapshot: ${testSnapshotId}`);

  // Test snapshot export command
  await runCommand(
    `node ./dist/index.js snapshot export ${testSnapshotId} -o ${path.join(TEST_CONFIG.outputDir, "snapshot-export.json")}`,
    `Export Snapshot: ${testSnapshotId}`
  );

  // Test snapshot duplicate command
  await runCommand(
    `node ./dist/index.js snapshot duplicate ${testSnapshotId} -t ${duplicateSnapshotId}`,
    `Duplicate Snapshot: ${testSnapshotId} to ${duplicateSnapshotId}`
  );

  // Test snapshot list command after duplication
  await runCommand(`node ./dist/index.js snapshot list`, "List Snapshots After Duplication");

  // Test snapshot compare command
  await runCommand(
    `node ./dist/index.js snapshot compare ${testSnapshotId} ${duplicateSnapshotId} -o ${path.join(
      TEST_CONFIG.outputDir,
      "snapshot-comparison.json"
    )}`,
    `Compare Snapshots: ${testSnapshotId} and ${duplicateSnapshotId}`
  );

  // Test snapshot import command (using the previously exported file)
  await runCommand(
    `node ./dist/index.js snapshot import ${path.join(TEST_CONFIG.outputDir, "snapshot-export.json")} -t imported-${testSnapshotId}`,
    "Import Snapshot from File"
  );

  // Test snapshot list command after import
  await runCommand(`node ./dist/index.js snapshot list`, "List Snapshots After Import");

  // Test snapshot delete command for duplicate
  await runCommand(`node ./dist/index.js snapshot delete ${duplicateSnapshotId}`, `Delete Snapshot: ${duplicateSnapshotId}`);

  // Test snapshot delete command for imported
  await runCommand(`node ./dist/index.js snapshot delete imported-${testSnapshotId}`, `Delete Snapshot: imported-${testSnapshotId}`);

  // Test snapshot delete command for original
  await runCommand(`node ./dist/index.js snapshot delete ${testSnapshotId}`, `Delete Snapshot: ${testSnapshotId}`);

  // Test snapshot list command after deletion
  await runCommand(`node ./dist/index.js snapshot list`, "List Snapshots After Deletion");

  console.log("\n=== Snapshot Tests Completed ===");
}

// Run the tests
runSnapshotTests().catch((err) => {
  console.error("Error running snapshot tests:", err);
  process.exit(1);
});
