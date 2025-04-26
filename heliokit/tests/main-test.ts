#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs/promises";
import path from "path";

const execAsync = promisify(exec);

// Main configuration
const TEST_CONFIG = {
  // Base output directory
  outputDir: "./test-output",
  // Test modules to run
  testModules: [
    "account-tests.js",
    "social-tests.js",
    "content-tests.js",
    //'search-tests.js',
    "context-tests.js",
    "snapshot-tests.js",
  ],
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

// Helper function to run a test module
async function runTestModule(modulePath: string): Promise<void> {
  console.log(`\n=== Running test module: ${modulePath} ===\n`);

  try {
    const { stdout, stderr } = await execAsync(`node ${modulePath}`);

    if (stderr) {
      console.error(`Error in test module ${modulePath}:`, stderr);
    }

    console.log(stdout);
    console.log(`\n=== Completed test module: ${modulePath} ===\n`);
    return Promise.resolve();
  } catch (error) {
    console.error(`Test module ${modulePath} failed:`, error);
    return Promise.resolve(); // Continue with other modules even if one fails
  }
}

// Main test runner
async function runAllTests() {
  console.log("=== Starting Bluesky Toolkit Test Suite ===\n");

  await ensureOutputDir();

  // Record start time
  const startTime = Date.now();

  // Run each test module sequentially
  for (const module of TEST_CONFIG.testModules) {
    await runTestModule(path.join("dist", "tests", module));
  }

  // Calculate execution time
  const endTime = Date.now();
  const executionTimeSeconds = ((endTime - startTime) / 1000).toFixed(2);

  console.log(`\n=== All Tests Completed ===`);
  console.log(`Total execution time: ${executionTimeSeconds} seconds`);
}

// Run all tests
runAllTests().catch((err) => {
  console.error("Error running test suite:", err);
  process.exit(1);
});
