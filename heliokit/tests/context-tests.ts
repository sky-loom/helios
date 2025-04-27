#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import path from "path";
import chalk from "chalk";

const execAsync = promisify(exec);

// Test configuration - edit these values as needed
const TEST_CONFIG = {
  // Context names for testing
  testContext: "test-context",
  secondContext: "second-test-context",
  // Sample test directory for output
  outputDir: "./test-output/context",
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
async function runContextTests() {
  console.log("=== Starting Bluesky Toolkit Context Tests ===");
  await ensureOutputDir();

  // Test context list command
  await runCommand(`node ./dist/index.js context list`, "List All Contexts");

  // Test context use command
  await runCommand(`node ./dist/index.js context use ${TEST_CONFIG.testContext}`, `Switch to Context: ${TEST_CONFIG.testContext}`);

  // Test context save command
  await runCommand(`node ./dist/index.js context save`, "Save Current Context");

  // Test context use command with another context
  await runCommand(`node ./dist/index.js context use ${TEST_CONFIG.secondContext}`, `Switch to Context: ${TEST_CONFIG.secondContext}`);

  // Test context save command with second context
  await runCommand(`node ./dist/index.js context save`, "Save Second Context");

  // Test context use command with default context
  await runCommand(`node ./dist/index.js context use default`, "Switch to Default Context");

  // Test context list command after creating contexts
  await runCommand(`node ./dist/index.js context list`, "List Contexts After Creation");

  // Test context delete command
  await runCommand(`node ./dist/index.js context delete ${TEST_CONFIG.secondContext}`, `Delete Context: ${TEST_CONFIG.secondContext}`);

  // Test context list command after deletion
  await runCommand(`node ./dist/index.js context list`, "List Contexts After Deletion");

  console.log("\n=== Context Tests Completed ===");
}

// Run the tests
runContextTests().catch((err) => {
  console.error("Error running context tests:", err);
  process.exit(1);
});
