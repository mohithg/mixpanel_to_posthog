//! Mixpanel to PostHog Migration CLI
//!
//! A high-performance Rust CLI tool to migrate historical event data from Mixpanel to PostHog.
//! Handles 4+ years of data with paginated retrieval (by day) and batch uploads.
//!
//! Usage:
//!   cargo run --release -- --start-date 2021-01-01 --end-date 2024-12-31
//!
//! Or build and run:
//!   cargo build --release
//!   ./target/release/mixpanel_to_posthog --start-date 2021-01-01 --end-date 2024-12-31

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{Duration, NaiveDate};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration as TokioDuration};
use uuid::Uuid;

// ============================================================================
// CONFIGURATION - HARDCODE YOUR SECRETS HERE
// ============================================================================

/// Mixpanel Service Account Username
const MIXPANEL_USERNAME: &str = "MIXPANEL_USERNAME";

/// Mixpanel Service Account Secret/Password
const MIXPANEL_PASSWORD: &str = "MIXPANEL_PASSWORD";

/// Mixpanel Project ID (found in Project Settings)
const MIXPANEL_PROJECT_ID: &str = "MIXPANEL_PROJECT_ID";

/// Mixpanel Data Export API URL (for events)
const MIXPANEL_DATA_API_URL: &str = "https://data.mixpanel.com/api/2.0";

/// Mixpanel Query API URL (for user profiles / Engage API)
const MIXPANEL_QUERY_API_URL: &str = "https://mixpanel.com/api/2.0";

/// PostHog Project API Key
const POSTHOG_PROJECT_KEY: &str = "POSTHOG_PROJECT_KEY";

/// PostHog API Endpoint (us.i.posthog.com or eu.i.posthog.com)
const POSTHOG_ENDPOINT: &str = "https://us.i.posthog.com";

// ============================================================================
// TUNING PARAMETERS
// ============================================================================

/// Maximum concurrent PostHog batch uploads
const MAX_CONCURRENT_UPLOADS: usize = 10;

/// Events per batch to send to PostHog (max request size is 20MB)
const BATCH_SIZE: usize = 500;

/// Maximum retries for failed requests
const MAX_RETRIES: u32 = 10;

/// Initial retry delay in milliseconds (starts at 5 seconds for rate limits)
const RETRY_INITIAL_DELAY_MS: u64 = 5000;

/// Delay between Mixpanel API calls to avoid rate limiting (ms)
/// Mixpanel allows 60 queries/hour for Raw Data Export API = 1 query per minute
/// We use 2 seconds between calls to be safe (allows ~1800 calls/hour theoretical max)
const MIXPANEL_RATE_LIMIT_DELAY_MS: u64 = 2000;

/// Page size for Mixpanel Engage API (user profiles)
const ENGAGE_PAGE_SIZE: usize = 1000;

/// Number of days to fetch per Mixpanel API call (reduces rate limiting)
/// Mixpanel allows 60 queries/hour, so larger chunks = fewer API calls
/// 3-day chunks to limit memory usage (~1-2M events per chunk)
const DAYS_PER_CHUNK: i64 = 3;

// ============================================================================
// DATA STRUCTURES
// ============================================================================

/// Raw Mixpanel event from export API (JSONL format)
#[derive(Debug, Deserialize)]
struct MixpanelEvent {
    event: String,
    properties: HashMap<String, Value>,
}

/// PostHog event format for batch API
#[derive(Debug, Clone, Serialize)]
struct PostHogEvent {
    event: String,
    properties: Map<String, Value>,
    timestamp: String,
    /// UUID for deduplication - PostHog dedupes on (uuid, event, timestamp, distinct_id)
    #[serde(skip_serializing_if = "Option::is_none")]
    uuid: Option<String>,
}

/// PostHog batch request body
#[derive(Debug, Serialize)]
struct PostHogBatchRequest {
    api_key: String,
    historical_migration: bool,
    batch: Vec<PostHogEvent>,
}

/// Mixpanel user profile from Engage API
#[derive(Debug, Deserialize)]
struct MixpanelProfile {
    #[serde(rename = "$distinct_id")]
    distinct_id: String,
    #[serde(rename = "$properties")]
    properties: HashMap<String, Value>,
}

/// Mixpanel Engage API response
#[derive(Debug, Deserialize)]
struct EngageResponse {
    results: Vec<MixpanelProfile>,
    #[serde(default)]
    page: u32,
    #[serde(default)]
    page_size: Option<u32>,
    session_id: Option<String>,
    total: Option<u64>,
}

/// Statistics tracker
struct Stats {
    events_exported: AtomicU64,
    events_imported: AtomicU64,
    batches_sent: AtomicU64,
    errors: AtomicU64,
    days_processed: AtomicU64,
    users_exported: AtomicU64,
    users_imported: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            events_exported: AtomicU64::new(0),
            events_imported: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            days_processed: AtomicU64::new(0),
            users_exported: AtomicU64::new(0),
            users_imported: AtomicU64::new(0),
        }
    }

    fn print_summary(&self, include_users: bool) {
        println!("\n========================================");
        println!("           MIGRATION SUMMARY");
        println!("========================================");
        if include_users {
            println!(
                "Users exported:    {}",
                self.users_exported.load(Ordering::Relaxed)
            );
            println!(
                "Users imported:    {}",
                self.users_imported.load(Ordering::Relaxed)
            );
        } else {
            println!(
                "Days processed:    {}",
                self.days_processed.load(Ordering::Relaxed)
            );
            println!(
                "Events exported:   {}",
                self.events_exported.load(Ordering::Relaxed)
            );
            println!(
                "Events imported:   {}",
                self.events_imported.load(Ordering::Relaxed)
            );
        }
        println!(
            "Batches sent:      {}",
            self.batches_sent.load(Ordering::Relaxed)
        );
        println!(
            "Errors:            {}",
            self.errors.load(Ordering::Relaxed)
        );
        println!("========================================\n");
    }
}

// ============================================================================
// MIXPANEL EVENT NAME MAPPINGS
// ============================================================================

/// Map Mixpanel event names to PostHog equivalents
fn map_event_name(mixpanel_event: &str) -> String {
    match mixpanel_event {
        "$mp_web_page_view" => "$pageview".to_string(),
        "$mp_page_view" => "$pageview".to_string(),
        "mp_page_view" => "$pageview".to_string(),
        "$mp_session_start" => "$session_start".to_string(),
        "$mp_session_end" => "$session_end".to_string(),
        "$mp_identify" => "$identify".to_string(),
        "$mp_alias" => "$create_alias".to_string(),
        "$mp_people_set" => "$set".to_string(),
        "$mp_people_set_once" => "$set_once".to_string(),
        "$mp_people_unset" => "$unset".to_string(),
        "$mp_people_increment" => "$increment".to_string(),
        "$mp_people_append" => "$append".to_string(),
        "$mp_people_union" => "$union".to_string(),
        "$mp_people_remove" => "$remove".to_string(),
        "$mp_people_delete" => "$delete".to_string(),
        // Keep other events as-is
        other => other.to_string(),
    }
}

// ============================================================================
// MIXPANEL PROPERTY MAPPINGS
// ============================================================================

/// Map Mixpanel property names to PostHog equivalents
fn map_property_name(mixpanel_prop: &str) -> Option<String> {
    match mixpanel_prop {
        // URL/Path properties
        "current_url_path" | "$current_url_path" => Some("$pathname".to_string()),
        "current_url" | "$current_url" => Some("$current_url".to_string()),
        "$current_url_host" | "current_url_host" => Some("$host".to_string()),
        "$current_url_protocol" | "current_url_protocol" => Some("$protocol".to_string()),
        "$current_url_search" | "current_url_search" => Some("$search".to_string()),

        // Referrer properties
        "$referrer" | "referrer" => Some("$referrer".to_string()),
        "$referring_domain" | "referring_domain" => Some("$referring_domain".to_string()),
        "$initial_referrer" | "initial_referrer" => Some("$initial_referrer".to_string()),
        "$initial_referring_domain" | "initial_referring_domain" => {
            Some("$initial_referring_domain".to_string())
        }

        // Device/Browser properties
        "$browser" | "browser" => Some("$browser".to_string()),
        "$browser_version" | "browser_version" => Some("$browser_version".to_string()),
        "$os" | "os" => Some("$os".to_string()),
        "$os_version" | "os_version" => Some("$os_version".to_string()),
        "$device" | "device" => Some("$device".to_string()),
        "$device_id" | "device_id" => Some("$device_id".to_string()),
        "$screen_height" | "screen_height" => Some("$screen_height".to_string()),
        "$screen_width" | "screen_width" => Some("$screen_width".to_string()),

        // Geo properties
        "$city" | "city" | "mp_city" => Some("$geoip_city_name".to_string()),
        "$region" | "region" | "mp_region" => Some("$geoip_subdivision_1_name".to_string()),
        "$country_code" | "country_code" | "mp_country_code" => {
            Some("$geoip_country_code".to_string())
        }
        "$timezone" | "timezone" => Some("$geoip_time_zone".to_string()),

        // User properties
        "$user_id" | "user_id" => Some("$user_id".to_string()),
        "$email" | "email" => Some("$email".to_string()),
        "$name" | "name" => Some("$name".to_string()),
        "$first_name" | "first_name" => Some("$first_name".to_string()),
        "$last_name" | "last_name" => Some("$last_name".to_string()),
        "$phone" | "phone" => Some("$phone".to_string()),
        "$avatar" | "avatar" => Some("$avatar".to_string()),

        // Session properties
        "$session_id" | "session_id" => Some("$session_id".to_string()),

        // UTM properties
        "utm_source" | "$utm_source" => Some("$utm_source".to_string()),
        "utm_medium" | "$utm_medium" => Some("$utm_medium".to_string()),
        "utm_campaign" | "$utm_campaign" => Some("$utm_campaign".to_string()),
        "utm_content" | "$utm_content" => Some("$utm_content".to_string()),
        "utm_term" | "$utm_term" => Some("$utm_term".to_string()),

        // Library properties
        "$lib" | "mp_lib" => Some("$lib".to_string()),
        "$lib_version" | "mp_lib_version" => Some("$lib_version".to_string()),

        // IP address
        "$ip" | "ip" => Some("$ip".to_string()),

        // Mixpanel internal properties to drop
        "mp_processing_time_ms" => None,
        "$mp_api_timestamp_ms" => None,
        "$mp_api_endpoint" => None,
        "$import" => None,
        "mp_sent_by_lib_version" => None,
        "$insert_id" => None,

        // Keep all other properties as-is
        other => Some(other.to_string()),
    }
}

/// Properties to completely drop from migration
fn should_drop_property(prop: &str) -> bool {
    matches!(
        prop,
        "mp_processing_time_ms"
            | "$mp_api_timestamp_ms"
            | "$mp_api_endpoint"
            | "$insert_id"
            | "$import"
            | "mp_sent_by_lib_version"
            | "$mp_session_id"
            | "$mp_session_seq_id"
            | "$mp_session_start_sec"
    )
}

// ============================================================================
// TRANSFORMATION LOGIC
// ============================================================================

/// Transform a Mixpanel event to PostHog format
fn transform_event(mixpanel_event: MixpanelEvent) -> Option<PostHogEvent> {
    let mut properties = Map::new();

    // Extract distinct_id for PostHog
    let distinct_id = mixpanel_event
        .properties
        .get("distinct_id")
        .or_else(|| mixpanel_event.properties.get("$distinct_id"))
        .or_else(|| mixpanel_event.properties.get("$device_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("anonymous");

    properties.insert("distinct_id".to_string(), json!(distinct_id));

    // Extract timestamp
    let timestamp = mixpanel_event
        .properties
        .get("time")
        .and_then(|v| v.as_i64())
        .map(|ts| {
            chrono::DateTime::from_timestamp(ts, 0)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string())
        })
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());

    // Generate deterministic UUID for deduplication
    // Use Mixpanel's $insert_id if available, otherwise create from event data
    let event_uuid = generate_event_uuid(&mixpanel_event, distinct_id, &timestamp);

    // Transform properties
    for (key, value) in mixpanel_event.properties {
        // Skip internal/dropped properties
        if should_drop_property(&key) {
            continue;
        }

        // Skip time as we handle it separately
        if key == "time" {
            continue;
        }

        // Map property name
        if let Some(mapped_key) = map_property_name(&key) {
            // Don't overwrite distinct_id if already set
            if mapped_key == "distinct_id" {
                continue;
            }
            properties.insert(mapped_key, value);
        }
    }

    // Map event name
    let event_name = map_event_name(&mixpanel_event.event);

    Some(PostHogEvent {
        event: event_name,
        properties,
        timestamp,
        uuid: Some(event_uuid),
    })
}

/// Generate a deterministic UUID for an event (for PostHog deduplication)
/// Uses Mixpanel's $insert_id if available, otherwise hashes event data
fn generate_event_uuid(event: &MixpanelEvent, distinct_id: &str, timestamp: &str) -> String {
    // Try to use Mixpanel's $insert_id first (most reliable)
    if let Some(insert_id) = event.properties.get("$insert_id").and_then(|v| v.as_str()) {
        // Create a UUID v5 from the insert_id using a namespace
        let namespace = Uuid::NAMESPACE_OID;
        return Uuid::new_v5(&namespace, insert_id.as_bytes()).to_string();
    }

    // Fallback: create deterministic UUID from event data
    // Hash: event_name + distinct_id + timestamp + sorted properties
    let mut hasher = Sha256::new();
    hasher.update(event.event.as_bytes());
    hasher.update(b"|");
    hasher.update(distinct_id.as_bytes());
    hasher.update(b"|");
    hasher.update(timestamp.as_bytes());

    // Add some properties to make it more unique
    if let Some(time) = event.properties.get("time") {
        hasher.update(b"|time:");
        hasher.update(time.to_string().as_bytes());
    }

    let hash = hasher.finalize();
    // Use first 16 bytes of SHA256 to create a UUID v4-like string
    let uuid_bytes: [u8; 16] = hash[..16].try_into().unwrap();
    Uuid::from_bytes(uuid_bytes).to_string()
}

// ============================================================================
// MIXPANEL API CLIENT
// ============================================================================

/// Export events from Mixpanel for a date range (reduces API calls vs day-by-day)
async fn export_mixpanel_range(
    client: &Client,
    from_date: NaiveDate,
    to_date: NaiveDate,
    stats: &Arc<Stats>,
) -> Result<Vec<MixpanelEvent>, String> {
    let from_str = from_date.format("%Y-%m-%d").to_string();
    let to_str = to_date.format("%Y-%m-%d").to_string();
    let range_str = if from_date == to_date {
        from_str.clone()
    } else {
        format!("{} to {}", from_str, to_str)
    };

    // Build URL with query parameters
    let url = format!(
        "{}/export?project_id={}&from_date={}&to_date={}",
        MIXPANEL_DATA_API_URL, MIXPANEL_PROJECT_ID, from_str, to_str
    );

    // Create Basic Auth header
    let credentials = format!("{}:{}", MIXPANEL_USERNAME, MIXPANEL_PASSWORD);
    let auth_header = format!("Basic {}", BASE64.encode(credentials.as_bytes()));

    let mut retries = 0;
    let mut delay = RETRY_INITIAL_DELAY_MS;

    loop {
        let response = client
            .get(&url)
            .header("Authorization", &auth_header)
            .header("Accept", "application/json")
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                if resp.status().is_success() {
                    let body = resp.text().await.map_err(|e| e.to_string())?;

                    // Parse JSONL (each line is a JSON object)
                    let events: Vec<MixpanelEvent> = body
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .filter_map(|line| {
                            serde_json::from_str(line)
                                .map_err(|e| {
                                    eprintln!("Failed to parse event: {} - Line: {}", e, line);
                                    e
                                })
                                .ok()
                        })
                        .collect();

                    stats
                        .events_exported
                        .fetch_add(events.len() as u64, Ordering::Relaxed);

                    return Ok(events);
                } else if status_code == 429 || (status_code >= 500 && status_code < 600) {
                    // Rate limited or server error - retry with backoff
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(format!("Mixpanel error {} after {} retries for {}", status_code, MAX_RETRIES, range_str));
                    }
                    let reason = if status_code == 429 { "Rate limited" } else { "Server error" };
                    eprintln!(
                        "{} ({}) for {}, waiting {}ms (retry {}/{})",
                        reason, status_code, range_str, delay, retries, MAX_RETRIES
                    );
                    sleep(TokioDuration::from_millis(delay)).await;
                    delay = std::cmp::min(delay * 2, 60000); // Cap at 60 seconds
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(format!(
                        "Mixpanel API error for {}: {} - {}",
                        range_str, status, body
                    ));
                }
            }
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    return Err(format!(
                        "Network error after {} retries for {}: {}",
                        MAX_RETRIES, range_str, e
                    ));
                }
                eprintln!(
                    "Network error for {}, retrying in {}ms (retry {}/{}): {}",
                    range_str, delay, retries, MAX_RETRIES, e
                );
                sleep(TokioDuration::from_millis(delay)).await;
                delay = std::cmp::min(delay * 2, 60000);
            }
        }
    }
}

// ============================================================================
// USER PROFILE EXPORT (ENGAGE API)
// ============================================================================

/// Export all user profiles from Mixpanel using the Engage API with pagination
async fn export_mixpanel_users(
    client: &Client,
    stats: &Arc<Stats>,
) -> Result<Vec<MixpanelProfile>, String> {
    let mut all_profiles: Vec<MixpanelProfile> = Vec::new();
    let mut session_id: Option<String> = None;
    let mut page: u32 = 0;

    // Create Basic Auth header
    let credentials = format!("{}:{}", MIXPANEL_USERNAME, MIXPANEL_PASSWORD);
    let auth_header = format!("Basic {}", BASE64.encode(credentials.as_bytes()));

    println!("Fetching user profiles from Mixpanel...");

    loop {
        // Build URL with pagination parameters
        let mut url = format!(
            "{}/engage?project_id={}&page_size={}",
            MIXPANEL_QUERY_API_URL, MIXPANEL_PROJECT_ID, ENGAGE_PAGE_SIZE
        );

        if let Some(ref sid) = session_id {
            url.push_str(&format!("&session_id={}&page={}", sid, page));
        }

        let mut retries = 0;
        let mut delay = RETRY_INITIAL_DELAY_MS;

        let response: EngageResponse = loop {
            let resp = client
                .get(&url)
                .header("Authorization", &auth_header)
                .header("Accept", "application/json")
                .send()
                .await;

            match resp {
                Ok(r) => {
                    if r.status().is_success() {
                        let body = r.text().await.map_err(|e| e.to_string())?;
                        match serde_json::from_str::<EngageResponse>(&body) {
                            Ok(engage_resp) => break engage_resp,
                            Err(e) => {
                                return Err(format!("Failed to parse Engage response: {} - Body: {}", e, body));
                            }
                        }
                    } else if r.status().as_u16() == 429 {
                        retries += 1;
                        if retries > MAX_RETRIES {
                            return Err(format!("Rate limited after {} retries", MAX_RETRIES));
                        }
                        eprintln!(
                            "Rate limited, waiting {}ms (retry {}/{})",
                            delay, retries, MAX_RETRIES
                        );
                        sleep(TokioDuration::from_millis(delay)).await;
                        delay *= 2;
                    } else {
                        let status = r.status();
                        let body = r.text().await.unwrap_or_default();
                        return Err(format!("Mixpanel Engage API error: {} - {}", status, body));
                    }
                }
                Err(e) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(format!("Network error after {} retries: {}", MAX_RETRIES, e));
                    }
                    eprintln!(
                        "Network error, retrying in {}ms (retry {}/{}): {}",
                        delay, retries, MAX_RETRIES, e
                    );
                    sleep(TokioDuration::from_millis(delay)).await;
                    delay *= 2;
                }
            }
        };

        let results_count = response.results.len();
        println!("  Page {}: {} profiles", page, results_count);

        all_profiles.extend(response.results);
        stats.users_exported.fetch_add(results_count as u64, Ordering::Relaxed);

        // Check if we need to fetch more pages
        // Use the page_size from response, or default to ENGAGE_PAGE_SIZE
        let effective_page_size = response.page_size.unwrap_or(ENGAGE_PAGE_SIZE as u32) as usize;
        if results_count < effective_page_size {
            // No more pages
            break;
        }

        // Set up for next page
        if session_id.is_none() {
            session_id = response.session_id;
        }
        page += 1;

        // Small delay to avoid rate limiting
        sleep(TokioDuration::from_millis(MIXPANEL_RATE_LIMIT_DELAY_MS)).await;
    }

    println!("Total profiles exported: {}", all_profiles.len());
    Ok(all_profiles)
}

/// Transform a Mixpanel user profile to a PostHog $set event
fn transform_user_profile(profile: MixpanelProfile) -> PostHogEvent {
    let mut properties = Map::new();

    // Set distinct_id
    properties.insert("distinct_id".to_string(), json!(profile.distinct_id));

    // Create $set payload with all user properties
    let mut set_properties = Map::new();

    for (key, value) in profile.properties {
        // Skip internal Mixpanel properties
        if key.starts_with("$mp_") || should_drop_user_property(&key) {
            continue;
        }

        // Map property names
        let mapped_key = map_user_property_name(&key);
        set_properties.insert(mapped_key, value);
    }

    // Add $set to properties
    properties.insert("$set".to_string(), json!(set_properties));

    // Generate deterministic UUID for user profile deduplication
    let namespace = Uuid::NAMESPACE_OID;
    let uuid = Uuid::new_v5(&namespace, format!("user_profile:{}", profile.distinct_id).as_bytes()).to_string();

    PostHogEvent {
        event: "$set".to_string(),
        properties,
        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        uuid: Some(uuid),
    }
}

/// Map Mixpanel user property names to PostHog equivalents
fn map_user_property_name(prop: &str) -> String {
    match prop {
        "$email" => "email".to_string(),
        "$first_name" => "first_name".to_string(),
        "$last_name" => "last_name".to_string(),
        "$name" => "name".to_string(),
        "$phone" => "phone".to_string(),
        "$avatar" => "avatar".to_string(),
        "$city" => "city".to_string(),
        "$region" => "region".to_string(),
        "$country_code" => "country_code".to_string(),
        "$timezone" => "timezone".to_string(),
        "$created" => "created_at".to_string(),
        "$last_seen" => "last_seen".to_string(),
        // Keep other properties as-is (remove $ prefix if present)
        other => {
            if other.starts_with('$') {
                other[1..].to_string()
            } else {
                other.to_string()
            }
        }
    }
}

/// Check if a user property should be dropped
fn should_drop_user_property(prop: &str) -> bool {
    matches!(
        prop,
        "$distinct_id"
            | "$mp_first_event_time"
            | "$mp_last_event_time"
            | "$mp_first_event_name"
            | "$mp_last_event_name"
            | "$mp_session_count"
            | "$mp_event_count"
            | "$mp_device_count"
            | "$mp_browser_count"
            | "$mp_os_count"
            | "$mp_country_count"
            | "$mp_city_count"
    )
}

/// Process user profiles: export from Mixpanel, transform, and import to PostHog
async fn process_users(
    client: &Client,
    stats: &Arc<Stats>,
    semaphore: &Arc<Semaphore>,
    dry_run: bool,
) -> Result<(), String> {
    // Export all user profiles
    let profiles = export_mixpanel_users(client, stats).await?;

    if profiles.is_empty() {
        println!("No user profiles found.");
        return Ok(());
    }

    if dry_run {
        println!("\nDRY RUN: Would migrate {} user profiles to PostHog", profiles.len());
        return Ok(());
    }

    println!("\nTransforming {} user profiles...", profiles.len());

    // Transform profiles to PostHog $set events
    let posthog_events: Vec<PostHogEvent> = profiles
        .into_iter()
        .map(transform_user_profile)
        .collect();

    // Send in batches
    let batches: Vec<Vec<PostHogEvent>> = posthog_events
        .into_iter()
        .collect::<Vec<_>>()
        .chunks(BATCH_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect();

    let batch_count = batches.len();
    println!("Sending {} batches to PostHog...", batch_count);

    // Send batches concurrently with semaphore control
    let results: Vec<Result<(), String>> = stream::iter(batches)
        .map(|batch| {
            let client = client.clone();
            let stats = Arc::clone(stats);
            let semaphore = Arc::clone(semaphore);
            async move { send_posthog_user_batch(&client, batch, &stats, &semaphore).await }
        })
        .buffer_unordered(MAX_CONCURRENT_UPLOADS)
        .collect()
        .await;

    // Check for errors
    let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
    if !errors.is_empty() {
        eprintln!("{} batch errors: {:?}", errors.len(), errors);
    }

    println!("User profile migration complete!");
    Ok(())
}

/// Send a batch of user profile events to PostHog
async fn send_posthog_user_batch(
    client: &Client,
    events: Vec<PostHogEvent>,
    stats: &Arc<Stats>,
    semaphore: &Arc<Semaphore>,
) -> Result<(), String> {
    let _permit = semaphore.acquire().await.map_err(|e| e.to_string())?;

    let batch_request = PostHogBatchRequest {
        api_key: POSTHOG_PROJECT_KEY.to_string(),
        historical_migration: true,
        batch: events,
    };

    let event_count = batch_request.batch.len();
    let url = format!("{}/batch/", POSTHOG_ENDPOINT);

    let mut retries = 0;
    let mut delay = RETRY_INITIAL_DELAY_MS;

    loop {
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&batch_request)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                if resp.status().is_success() {
                    stats.users_imported.fetch_add(event_count as u64, Ordering::Relaxed);
                    stats.batches_sent.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                } else if status_code == 429 || (status_code >= 500 && status_code < 600) {
                    // Retry on rate limit (429) or server errors (5xx)
                    retries += 1;
                    if retries > MAX_RETRIES {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                        return Err(format!("PostHog error {} after {} retries", status_code, MAX_RETRIES));
                    }
                    let reason = if status_code == 429 { "rate limited" } else { "server error" };
                    eprintln!(
                        "PostHog {} ({}), waiting {}ms (retry {}/{})",
                        reason, status_code, delay, retries, MAX_RETRIES
                    );
                    sleep(TokioDuration::from_millis(delay)).await;
                    delay = std::cmp::min(delay * 2, 60000); // Cap at 60 seconds
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(format!("PostHog API error: {} - {}", status, body));
                }
            }
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(format!("PostHog network error after {} retries: {}", MAX_RETRIES, e));
                }
                eprintln!(
                    "PostHog network error, retrying in {}ms (retry {}/{}): {}",
                    delay, retries, MAX_RETRIES, e
                );
                sleep(TokioDuration::from_millis(delay)).await;
                delay = std::cmp::min(delay * 2, 60000); // Cap at 60 seconds
            }
        }
    }
}

// ============================================================================
// POSTHOG API CLIENT
// ============================================================================

/// Send a batch of events to PostHog
async fn send_posthog_batch(
    client: &Client,
    events: Vec<PostHogEvent>,
    stats: &Arc<Stats>,
    semaphore: &Arc<Semaphore>,
) -> Result<(), String> {
    let _permit = semaphore.acquire().await.map_err(|e| e.to_string())?;

    let batch_request = PostHogBatchRequest {
        api_key: POSTHOG_PROJECT_KEY.to_string(),
        historical_migration: true,
        batch: events,
    };

    let event_count = batch_request.batch.len();
    let url = format!("{}/batch/", POSTHOG_ENDPOINT);

    let mut retries = 0;
    let mut delay = RETRY_INITIAL_DELAY_MS;

    loop {
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&batch_request)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                if resp.status().is_success() {
                    stats
                        .events_imported
                        .fetch_add(event_count as u64, Ordering::Relaxed);
                    stats.batches_sent.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                } else if status_code == 429 || (status_code >= 500 && status_code < 600) {
                    // Retry on rate limit (429) or server errors (5xx)
                    retries += 1;
                    if retries > MAX_RETRIES {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                        return Err(format!(
                            "PostHog error {} after {} retries",
                            status_code, MAX_RETRIES
                        ));
                    }
                    let reason = if status_code == 429 { "rate limited" } else { "server error" };
                    eprintln!(
                        "PostHog {} ({}), waiting {}ms (retry {}/{})",
                        reason, status_code, delay, retries, MAX_RETRIES
                    );
                    sleep(TokioDuration::from_millis(delay)).await;
                    delay = std::cmp::min(delay * 2, 60000); // Cap at 60 seconds
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(format!("PostHog API error: {} - {}", status, body));
                }
            }
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(format!(
                        "PostHog network error after {} retries: {}",
                        MAX_RETRIES, e
                    ));
                }
                eprintln!(
                    "PostHog network error, retrying in {}ms (retry {}/{}): {}",
                    delay, retries, MAX_RETRIES, e
                );
                sleep(TokioDuration::from_millis(delay)).await;
                delay = std::cmp::min(delay * 2, 60000); // Cap at 60 seconds
            }
        }
    }
}

// ============================================================================
// MAIN MIGRATION LOGIC
// ============================================================================

/// Send a wave of batches concurrently and return error count + messages
async fn send_batch_wave(
    client: &Client,
    batches: Vec<Vec<PostHogEvent>>,
    stats: &Arc<Stats>,
    semaphore: &Arc<Semaphore>,
) -> (usize, Vec<String>) {
    let results: Vec<Result<(), String>> = stream::iter(batches)
        .map(|batch| {
            let client = client.clone();
            let stats = Arc::clone(stats);
            let semaphore = Arc::clone(semaphore);
            async move {
                let result = send_posthog_batch(&client, batch, &stats, &semaphore).await;
                // batch is dropped here
                result
            }
        })
        .buffer_unordered(MAX_CONCURRENT_UPLOADS)
        .collect()
        .await;

    let mut error_count = 0;
    let mut error_messages = Vec::new();
    for result in results {
        if let Err(e) = result {
            error_count += 1;
            if error_messages.len() < 5 {
                error_messages.push(e);
            }
        }
    }
    (error_count, error_messages)
}

/// Process a date range: export from Mixpanel, transform, and import to PostHog
async fn process_range(
    client: &Client,
    from_date: NaiveDate,
    to_date: NaiveDate,
    stats: &Arc<Stats>,
    semaphore: &Arc<Semaphore>,
) -> Result<(), String> {
    let range_str = if from_date == to_date {
        from_date.format("%Y-%m-%d").to_string()
    } else {
        format!("{} to {}", from_date.format("%Y-%m-%d"), to_date.format("%Y-%m-%d"))
    };
    let days_in_range = (to_date - from_date).num_days() + 1;

    println!("Processing: {} ({} days)", range_str, days_in_range);

    // Export from Mixpanel - this is the main memory consumer
    let mixpanel_events = export_mixpanel_range(client, from_date, to_date, stats).await?;
    let event_count = mixpanel_events.len();

    if event_count == 0 {
        println!("  No events for {}", range_str);
        stats.days_processed.fetch_add(days_in_range as u64, Ordering::Relaxed);
        return Ok(());
    }

    println!("  Exported {} events from Mixpanel", event_count);
    let batch_count = (event_count + BATCH_SIZE - 1) / BATCH_SIZE;
    println!("  Sending ~{} batches to PostHog...", batch_count);

    // Process in smaller sub-chunks to limit memory
    // Send batches sequentially in groups to free memory as we go
    let mut current_batch: Vec<PostHogEvent> = Vec::with_capacity(BATCH_SIZE);
    let mut pending_batches: Vec<Vec<PostHogEvent>> = Vec::new();
    let mut transformed_count = 0;
    let mut error_count = 0;
    let mut error_messages: Vec<String> = Vec::new();
    let mut batches_sent = 0;

    // Process events and send in waves of MAX_CONCURRENT_UPLOADS batches
    for event in mixpanel_events {
        if let Some(posthog_event) = transform_event(event) {
            transformed_count += 1;
            current_batch.push(posthog_event);

            if current_batch.len() >= BATCH_SIZE {
                pending_batches.push(std::mem::take(&mut current_batch));
                current_batch = Vec::with_capacity(BATCH_SIZE);

                // When we have enough batches, send them and free memory
                if pending_batches.len() >= MAX_CONCURRENT_UPLOADS {
                    let (errors, msgs) = send_batch_wave(
                        client, std::mem::take(&mut pending_batches), stats, semaphore
                    ).await;
                    error_count += errors;
                    for msg in msgs {
                        if error_messages.len() < 5 { error_messages.push(msg); }
                    }
                    batches_sent += MAX_CONCURRENT_UPLOADS;
                    pending_batches = Vec::new(); // Ensure fresh allocation
                }
            }
        }
    }
    // Don't forget the last partial batch
    if !current_batch.is_empty() {
        pending_batches.push(current_batch);
    }
    // Send remaining batches
    if !pending_batches.is_empty() {
        let remaining = pending_batches.len();
        let (errors, msgs) = send_batch_wave(
            client, pending_batches, stats, semaphore
        ).await;
        error_count += errors;
        for msg in msgs {
            if error_messages.len() < 5 { error_messages.push(msg); }
        }
        batches_sent += remaining;
    }

    println!("  Transformed {} events, sent {} batches", transformed_count, batches_sent);

    if error_count > 0 {
        eprintln!("  {} batch errors for {}: {:?}", error_count, range_str, error_messages);
    }

    stats.days_processed.fetch_add(days_in_range as u64, Ordering::Relaxed);
    println!("  Completed {}", range_str);

    // Delay between chunks to avoid rate limiting
    sleep(TokioDuration::from_millis(MIXPANEL_RATE_LIMIT_DELAY_MS)).await;

    Ok(())
}

/// Generate date range chunks iterator (groups dates into chunks of DAYS_PER_CHUNK)
fn date_range_chunks(start: NaiveDate, end: NaiveDate) -> impl Iterator<Item = (NaiveDate, NaiveDate)> {
    let mut current = start;
    std::iter::from_fn(move || {
        if current > end {
            return None;
        }
        let chunk_start = current;
        let chunk_end = std::cmp::min(current + Duration::days(DAYS_PER_CHUNK - 1), end);
        current = chunk_end + Duration::days(1);
        Some((chunk_start, chunk_end))
    })
}

/// CLI arguments structure
struct CliArgs {
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    dry_run: bool,
    users_only: bool,
}

/// Parse command line arguments
fn parse_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();

    let mut start_date: Option<NaiveDate> = None;
    let mut end_date: Option<NaiveDate> = None;
    let mut dry_run = false;
    let mut users_only = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--start-date" | "-s" => {
                if i + 1 < args.len() {
                    start_date = NaiveDate::parse_from_str(&args[i + 1], "%Y-%m-%d").ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--end-date" | "-e" => {
                if i + 1 < args.len() {
                    end_date = NaiveDate::parse_from_str(&args[i + 1], "%Y-%m-%d").ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--dry-run" | "-d" => {
                dry_run = true;
                i += 1;
            }
            "--users" | "-u" => {
                users_only = true;
                i += 1;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                i += 1;
            }
        }
    }

    // Validate: if not users_only, dates are required
    if !users_only {
        if start_date.is_none() {
            eprintln!("Error: --start-date is required for event migration (format: YYYY-MM-DD)");
            eprintln!("       Use --users to migrate user profiles instead.");
            print_help();
            std::process::exit(1);
        }
        if end_date.is_none() {
            eprintln!("Error: --end-date is required for event migration (format: YYYY-MM-DD)");
            print_help();
            std::process::exit(1);
        }
        if let (Some(s), Some(e)) = (start_date, end_date) {
            if s > e {
                eprintln!("Error: start-date must be before or equal to end-date");
                std::process::exit(1);
            }
        }
    }

    CliArgs {
        start_date,
        end_date,
        dry_run,
        users_only,
    }
}

fn print_help() {
    println!(
        r#"
Mixpanel to PostHog Migration Tool

USAGE:
    mixpanel_to_posthog [OPTIONS]

MODES:
    Events Mode (default):
        Migrate historical events from Mixpanel to PostHog.
        Requires --start-date and --end-date.

    Users Mode (--users):
        Migrate user profiles from Mixpanel to PostHog.
        User properties are sent as $set events.

OPTIONS:
    -s, --start-date <DATE>    Start date for event migration (YYYY-MM-DD)
    -e, --end-date <DATE>      End date for event migration (YYYY-MM-DD)
    -u, --users                Migrate user profiles instead of events
    -d, --dry-run              Show what would be migrated without sending to PostHog
    -h, --help                 Print this help message

EXAMPLES:
    # Migrate events for all of 2023
    mixpanel_to_posthog --start-date 2023-01-01 --end-date 2023-12-31

    # Migrate a single day of events
    mixpanel_to_posthog -s 2024-01-15 -e 2024-01-15

    # Migrate all user profiles
    mixpanel_to_posthog --users

    # Dry run user migration
    mixpanel_to_posthog --users --dry-run

    # Dry run to see event counts
    mixpanel_to_posthog -s 2024-01-01 -e 2024-01-31 --dry-run

CONFIGURATION:
    Edit the constants at the top of main.rs to set your credentials:
    - MIXPANEL_USERNAME: Service account username
    - MIXPANEL_PASSWORD: Service account secret
    - MIXPANEL_PROJECT_ID: Your Mixpanel project ID
    - POSTHOG_PROJECT_KEY: Your PostHog project API key
    - POSTHOG_ENDPOINT: PostHog API endpoint (us or eu)

NOTES:
    - Mixpanel API has rate limits (60 queries/hour for events, 60/hour for users)
    - For large datasets, run in smaller date ranges
    - Events are sent with historical_migration=true to avoid spike detection
    - User profiles are paginated automatically (1000 users per page)
"#
    );
}

fn validate_config() {
    let mut errors = Vec::new();

    if MIXPANEL_USERNAME.starts_with("YOUR_") {
        errors.push("MIXPANEL_USERNAME is not configured");
    }
    if MIXPANEL_PASSWORD.starts_with("YOUR_") {
        errors.push("MIXPANEL_PASSWORD is not configured");
    }
    if MIXPANEL_PROJECT_ID.starts_with("YOUR_") {
        errors.push("MIXPANEL_PROJECT_ID is not configured");
    }
    if POSTHOG_PROJECT_KEY.starts_with("YOUR_") {
        errors.push("POSTHOG_PROJECT_KEY is not configured");
    }

    if !errors.is_empty() {
        eprintln!("\nConfiguration Error!");
        eprintln!("Please edit the constants at the top of main.rs:\n");
        for error in errors {
            eprintln!("  - {}", error);
        }
        eprintln!("\nSee --help for more information.");
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    // Validate configuration
    validate_config();

    println!("\n========================================");
    println!("  Mixpanel to PostHog Migration Tool");
    println!("========================================\n");

    if args.users_only {
        println!("Mode:       User Profiles");
    } else {
        println!("Mode:       Events");
        println!("Start Date: {}", args.start_date.unwrap());
        println!("End Date:   {}", args.end_date.unwrap());
        println!(
            "Days:       {}",
            (args.end_date.unwrap() - args.start_date.unwrap()).num_days() + 1
        );
    }
    println!("Dry Run:    {}", args.dry_run);
    println!("\nMixpanel Project: {}", MIXPANEL_PROJECT_ID);
    println!("PostHog Endpoint: {}", POSTHOG_ENDPOINT);
    println!();

    if args.dry_run {
        println!("DRY RUN MODE - No data will be sent to PostHog\n");
    }

    // Create HTTP client with connection pooling
    let client = Client::builder()
        .pool_max_idle_per_host(20)
        .timeout(std::time::Duration::from_secs(600)) // 10 minutes for large exports
        .build()
        .expect("Failed to create HTTP client");

    // Create stats tracker
    let stats = Arc::new(Stats::new());

    // Create semaphore for concurrent uploads
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS));

    if args.users_only {
        // User profile migration mode
        if let Err(e) = process_users(&client, &stats, &semaphore, args.dry_run).await {
            eprintln!("Error migrating user profiles: {}", e);
            stats.errors.fetch_add(1, Ordering::Relaxed);
        }

        // Print summary
        stats.print_summary(true);
    } else {
        // Event migration mode
        let start_date = args.start_date.unwrap();
        let end_date = args.end_date.unwrap();
        let total_days = (end_date - start_date).num_days() + 1;
        let total_chunks = (total_days + DAYS_PER_CHUNK - 1) / DAYS_PER_CHUNK;

        println!("Processing {} days in {} chunks of up to {} days each\n",
            total_days, total_chunks, DAYS_PER_CHUNK);

        // Process in chunks to reduce API calls and avoid rate limiting
        for (chunk_start, chunk_end) in date_range_chunks(start_date, end_date) {
            if args.dry_run {
                // In dry run mode, just export and count
                let range_str = if chunk_start == chunk_end {
                    chunk_start.format("%Y-%m-%d").to_string()
                } else {
                    format!("{} to {}", chunk_start.format("%Y-%m-%d"), chunk_end.format("%Y-%m-%d"))
                };
                match export_mixpanel_range(&client, chunk_start, chunk_end, &stats).await {
                    Ok(events) => {
                        println!("{}: {} events (would transform and send)", range_str, events.len());
                    }
                    Err(e) => {
                        eprintln!("{}: Error - {}", range_str, e);
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let days_in_chunk = (chunk_end - chunk_start).num_days() + 1;
                stats.days_processed.fetch_add(days_in_chunk as u64, Ordering::Relaxed);
            } else {
                // Full migration
                if let Err(e) = process_range(&client, chunk_start, chunk_end, &stats, &semaphore).await {
                    let range_str = format!("{} to {}", chunk_start.format("%Y-%m-%d"), chunk_end.format("%Y-%m-%d"));
                    eprintln!("Error processing {}: {}", range_str, e);
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Print summary
        stats.print_summary(false);
    }

    if stats.errors.load(Ordering::Relaxed) > 0 {
        std::process::exit(1);
    }
}
