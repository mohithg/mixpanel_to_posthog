# Mixpanel to PostHog Migration Tool

A high-performance Rust CLI tool to migrate historical event data and user profiles from Mixpanel to PostHog.

## Features

- **Event Migration**: Exports raw events from Mixpanel and imports them to PostHog
- **User Profile Migration**: Migrates user properties from Mixpanel's Engage API to PostHog `$set` events
- **Deduplication**: Generates deterministic UUIDs so re-running the migration won't create duplicates
- **Rate Limit Handling**: Respects Mixpanel's 60 queries/hour limit with automatic retries and exponential backoff
- **Memory Efficient**: Processes data in waves, freeing memory after each batch is sent
- **Concurrent Uploads**: Sends multiple batches to PostHog simultaneously for faster imports
- **Historical Migration Flag**: Uses PostHog's `historical_migration: true` for proper timestamp handling

## Prerequisites

- Rust 1.70+ (install via [rustup](https://rustup.rs/))
- Mixpanel Service Account credentials
- PostHog Project API Key

## Configuration

Edit the constants at the top of `src/main.rs`:

```rust
// Mixpanel credentials (from Project Settings > Service Accounts)
const MIXPANEL_USERNAME: &str = "your-service-account-username";
const MIXPANEL_PASSWORD: &str = "your-service-account-secret";
const MIXPANEL_PROJECT_ID: &str = "your-project-id";

// PostHog credentials (from Project Settings > Project API Key)
const POSTHOG_PROJECT_KEY: &str = "phc_your_project_key";
const POSTHOG_ENDPOINT: &str = "https://us.i.posthog.com"; // or https://eu.i.posthog.com
```

## Build

```bash
cd mixpanel_to_posthog
cargo build --release
```

The binary will be at `./target/release/mixpanel_to_posthog`.

## Usage

### Migrate Events

```bash
# Migrate events from a date range (inclusive)
./target/release/mixpanel_to_posthog --start-date 2021-01-01 --end-date 2024-12-31

# Dry run - export and count events without sending to PostHog
./target/release/mixpanel_to_posthog --start-date 2021-01-01 --end-date 2024-12-31 --dry-run
```

### Migrate User Profiles

```bash
# Migrate all user profiles from Mixpanel
./target/release/mixpanel_to_posthog --users

# Dry run
./target/release/mixpanel_to_posthog --users --dry-run
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--start-date YYYY-MM-DD` | Start date for event migration (required for events) |
| `--end-date YYYY-MM-DD` | End date for event migration (required for events) |
| `--users` | Migrate user profiles instead of events |
| `--dry-run` | Export and count data without sending to PostHog |
| `--help` | Show help message |

## How It Works

### Event Migration

1. **Export**: Fetches events from Mixpanel's Raw Data Export API in 3-day chunks
2. **Transform**: Converts Mixpanel event format to PostHog format
   - Maps event names (e.g., `$mp_web_page_view` → `$pageview`)
   - Maps property names (e.g., `$browser` → `$browser`, `$city` → `$geoip_city_name`)
   - Generates deterministic UUID from Mixpanel's `$insert_id` for deduplication
3. **Upload**: Sends events to PostHog's `/batch` endpoint in batches of 500

### User Profile Migration

1. **Export**: Fetches all user profiles from Mixpanel's Engage API with pagination
2. **Transform**: Converts profiles to PostHog `$set` events
3. **Upload**: Sends to PostHog's `/batch` endpoint

### Property Mapping

| Mixpanel Property | PostHog Property |
|-------------------|------------------|
| `$browser` | `$browser` |
| `$os` | `$os` |
| `$device` | `$device_type` |
| `$current_url` | `$current_url` |
| `$referrer` | `$referrer` |
| `$city` | `$geoip_city_name` |
| `$region` | `$geoip_subdivision_1_name` |
| `$country_code` | `$geoip_country_code` |
| `mp_country_code` | `$geoip_country_code` |
| `$screen_width` | `$screen_width` |
| `$screen_height` | `$screen_height` |

### Event Name Mapping

| Mixpanel Event | PostHog Event |
|----------------|---------------|
| `$mp_web_page_view` | `$pageview` |
| `mp_page_view` | `$pageview` |
| `$mp_page_view` | `$pageview` |
| Other events | Kept as-is |

## Tuning Parameters

These can be adjusted in `src/main.rs`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BATCH_SIZE` | 500 | Events per PostHog batch request |
| `MAX_CONCURRENT_UPLOADS` | 10 | Parallel batch uploads to PostHog |
| `DAYS_PER_CHUNK` | 3 | Days per Mixpanel API call |
| `MAX_RETRIES` | 10 | Max retries for failed requests |
| `RETRY_INITIAL_DELAY_MS` | 5000 | Initial retry delay (doubles each retry, max 60s) |

## Rate Limits

### Mixpanel
- Raw Data Export API: 60 queries/hour, 3 queries/second
- Engage API: 60 queries/hour

### PostHog
- Batch API: No hard limit, but uses `historical_migration` flag for proper queuing

## Deduplication

The tool generates deterministic UUIDs for each event:

1. If Mixpanel's `$insert_id` exists: Creates UUID v5 from it
2. Otherwise: Hashes `event_name + distinct_id + timestamp` to create a UUID

PostHog deduplicates events with the same `(uuid, event, timestamp, distinct_id)`, so re-running the migration is safe.

## Memory Usage

Memory usage depends on the data volume per chunk:
- ~1GB per 500K events in a chunk
- The tool processes in waves of 10 batches (5000 events), freeing memory after each wave

To reduce memory usage, decrease `DAYS_PER_CHUNK`.

## Error Handling

- **429 Rate Limit**: Retries with exponential backoff (up to 60 seconds)
- **5xx Server Errors**: Retries with exponential backoff
- **Timeouts**: 10-minute timeout per request, retries on timeout
- **Network Errors**: Retries with exponential backoff

Errors are logged but don't stop the migration. A summary is printed at the end.

## Example Output

```
Mixpanel to PostHog Migration
=============================
Mixpanel Project: 2322851
PostHog Endpoint: https://us.i.posthog.com

Processing 275 days in 92 chunks of up to 3 days each

Processing: 2024-01-01 to 2024-01-03 (3 days)
  Exported 156234 events from Mixpanel
  Sending ~313 batches to PostHog...
  Transformed 156234 events, sent 313 batches
  Completed 2024-01-01 to 2024-01-03
Processing: 2024-01-04 to 2024-01-06 (3 days)
...

=== Migration Summary ===
Events exported: 12,456,789
Events imported: 12,456,789
Batches sent: 24,914
Days processed: 275
Errors: 0
```

## Troubleshooting

### "operation timed out"
Increase the timeout in the HTTP client configuration or reduce `DAYS_PER_CHUNK`.

### "too_many_internal_resets"
PostHog is rate limiting. The tool will automatically retry with backoff.

### High memory usage
Reduce `DAYS_PER_CHUNK` to process smaller chunks.

### Missing events
Check the error count in the summary. Re-run the migration for the affected date range (deduplication prevents duplicates).
