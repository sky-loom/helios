# HelioKit CLI

A comprehensive command-line toolkit for working with Bluesky data. This toolkit provides tools for researchers, developers, and data scientists to download, analyze, and manipulate data from the Bluesky social network (AT Protocol).

## Features

- **Account Management**: View repository information and download user repositories
- **Social Graph Analysis**: Fetch profiles, followers, follows, blocks, and blocklists
- **Content Retrieval**: Get posts, threads, and embedded media
- **Search Capabilities**: Text and semantic search across downloaded content
- **Context Management**: Work with different data contexts for organizing research
- **Snapshot Management**: Create, manage, and compare data snapshots

## Installation

```bash
# Install globally from npm
npm install -g @skyloom/heliokit

# Or run directly using npx without installing
npx @skyloom/heliokit [command]

# Or install from source
git clone https://github.com/sky-loom/helios.git
cd helios
pnpm install
pnpm run build
npm link
```

## Usage

```bash
# If installed globally
heliokit [options] [command]

# If using npx
npx @skyloom/heliokit [options] [command]
```

### Global Options

- `-i, --interactive`: Enter interactive mode
- `-s, --sqlite [path]`: Use SQLite persistence for context data
- `-h, --help`: Display help information
- `-v, --version`: Output the version number

### Base Command Options

Most commands support these options:

- `-o, --output <file>`: Output results to JSON file
- `-d, --dry-run`: Execute command without storing data to database
- `-c, --context <n>`: Specify context to use
- `-s, --snapshot <n>`: Specify snapshot name
- `--debug`: Enable debug output
- `-l, --labelers <items>`: Specify labeler DIDs to use (comma-separated list)

## Commands

### Account Commands

```bash
# Get repository information
heliokit account repo <identifier>

# Download repository
heliokit account download <identifier>
```

### Social Commands

```bash
# Get user profile
heliokit social profile <identifier>

# Get followers
heliokit social followers <identifier> [options]

# Get follows
heliokit social follows <identifier> [options]

# Get blocks
heliokit social blocks <identifier>

# Get blocklists
heliokit social blocklists <identifier>
```

### Content Commands

```bash
# Get a specific post
heliokit content post <uri>

# Get all posts for a user
heliokit content posts <identifier>

# Get a thread
heliokit content thread <uri>

# Get an image
heliokit content image <did> <cid> [options]
```

### Search Commands

```bash
# Search posts
heliokit search posts <query> [options]

# Index posts for search
heliokit search index [options]

# Load search index
heliokit search load [options]
```

### Context Commands

```bash
# List contexts
heliokit context list

# Save current context
heliokit context save

# Switch context
heliokit context use <name>

# Delete context
heliokit context delete <name>
```

### Snapshot Commands

```bash
# List snapshots
heliokit snapshot list

# Create snapshot
heliokit snapshot create <name>

# Select snapshot
heliokit snapshot use <id>

# Delete snapshot
heliokit snapshot delete <id>

# Export snapshot
heliokit snapshot export <id> [options]

# Import snapshot
heliokit snapshot import <file> [options]

# Duplicate snapshot
heliokit snapshot duplicate <sourceId> [options]

# Compare snapshots
heliokit snapshot compare <id1> <id2> [options]
```

## Examples

### Getting a User Profile

```bash
# By handle
heliokit social profile alice.bsky.social

# By DID
heliokit social profile did:plc:abcdefghijklmnop

# Saving the output to a file
heliokit social profile alice.bsky.social -o alice-profile.json

# Using a specific context
heliokit social profile alice.bsky.social -c research-project-1

# Using npx
npx @skyloom/heliokit social profile alice.bsky.social
```

### Downloading a Repository

```bash
heliokit account download alice.bsky.social --snapshot repo-snapshot-1
```

### Getting Followers

```bash
# Get first page of followers
heliokit social followers alice.bsky.social

# Get multiple pages of followers
heliokit social followers alice.bsky.social --page-count 5
```

### Searching Posts

```bash
# Text-based search
heliokit search posts "climate change"

# Semantic search
heliokit search posts "impact of misinformation" --method semantic --did did:plc:yourauthoringdid
```

### Working with Snapshots

```bash
# Create a snapshot
heliokit snapshot create research-phase-1

# Export a snapshot for backup
heliokit snapshot export research-phase-1 -o backup.json

# Compare two snapshots
heliokit snapshot compare research-phase-1 research-phase-2 -o comparison.json
```

## Interactive Mode

You can use the CLI in interactive mode by running:

```bash
heliokit -i

# Or with npx
npx @skyloom/heliokit -i
```

This will give you a prompt where you can enter commands without prefixing them with `heliokit`:

```
bsky> social profile alice.bsky.social
```

## Environment Variables

- `DATABASE_URL_BSKYTOOLS`: PostgreSQL connection string for data persistence

## Data Storage

By default, the toolkit uses PostgreSQL for data storage. You can switch to SQLite with the `-s, --sqlite` option:

```bash
heliokit -s ./data.sqlite

# Or with npx
npx @skyloom/heliokit -s ./data.sqlite
```

## Development

```bash
# Clone the repository
git clone https://github.com/yourusername/heliokit.git
cd heliokit

# Install dependencies
pnpm install

# Build
pnpm run build

# Run tests
pnpm run test-suite

# Local development execution
node ./heliokit/dist/index.js [command]
```

## License

MIT

## Credits

Developed by Skyloom. Powered by the AT Protocol.
