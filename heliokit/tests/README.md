# Bluesky Toolkit Test Suite

This test suite verifies the functionality of the Bluesky Toolkit CLI application. It tests each command and subcommand to ensure they work as expected.

## Structure

The test suite is organized into modules by command namespace:

- **Account Tests**: Tests for repository operations
- **Social Tests**: Tests for social graph operations
- **Content Tests**: Tests for content retrieval operations
- **Search Tests**: Tests for search functionality
- **Context Tests**: Tests for context management
- **Snapshot Tests**: Tests for snapshot management

## Setup

1. Place all test files in a `tests` directory at the root of your project.
2. Copy the provided `tsconfig.json` file or add the test directory to your existing configuration.
3. Update the test configuration variables in each test file to match your testing environment.

## Running Tests

You can run the entire test suite with:

```bash
npm run test-suite
```

Or run individual test modules with:

```bash
node dist/tests/account-tests.js
node dist/tests/social-tests.js
# etc.
```

## Configuration

Each test file contains a `TEST_CONFIG` object at the top that should be configured before running:

```typescript
const TEST_CONFIG = {
  // Account identifiers for testing
  testHandle: "your-handle.bsky.social",
  testDID: "did:plc:exampledidforyourtests",
  // Other configuration values...
};
```

Important configuration values:

- `testHandle`: A valid Bluesky handle for testing
- `testDID`: A valid Bluesky DID for testing
- `testPostURI`: A valid Bluesky post URI for testing
- `testThreadURI`: A valid Bluesky thread URI for testing
- `testImageCID`: A valid Bluesky image CID for testing
- `outputDir`: Directory for test output files

## Adding to package.json

Add the following to your `package.json` scripts:

```json
"scripts": {
  "test-suite": "tsc && node dist/tests/main-test.js",
  "test-account": "tsc && node dist/tests/account-tests.js",
  "test-social": "tsc && node dist/tests/social-tests.js",
  "test-content": "tsc && node dist/tests/content-tests.js",
  "test-search": "tsc && node dist/tests/search-tests.js",
  "test-context": "tsc && node dist/tests/context-tests.js",
  "test-snapshot": "tsc && node dist/tests/snapshot-tests.js"
}
```

## Test Output

Each test module creates output files in the specified output directory. These files contain the results of commands and can be used for verification or debugging.

The test runner also logs all commands and their results to the console.
