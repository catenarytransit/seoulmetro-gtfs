use futures::stream::{self, StreamExt};
use osmpbfreader::{OsmObj, OsmPbfReader};
use regex::Regex;
use scraper::{Html, Selector};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::io::Write;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::task;

static TRAILING_COMMA_REGEX: OnceLock<Regex> = OnceLock::new();
static TIME_REGEX: OnceLock<Regex> = OnceLock::new();
static ROUTE_REGEX: OnceLock<Regex> = OnceLock::new();

#[derive(Debug, Clone)]
struct Station {
    uid: String,
    name: String,
    lat: f64,
    lon: f64,
    line_name: String,
    line_id: String,
}

#[derive(Debug, Clone)]
struct Route {
    id: String,
    short_name: String,
    long_name: String,
    color: String,
}

#[derive(Debug)]
struct TrainSchedule {
    station_uid: String,
    week_tag: String,
    train_no: String,
    line_name: String,
    in_out_tag: String,
    arrival_time: String,
    departure_time: String,
    dest_station: String,
}

async fn fetch_wikipedia_coords(client: &reqwest::Client, name: &str) -> Option<(f64, f64)> {
    let url = format!("https://ko.wikipedia.org/wiki/{}역", name);
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let html = resp.text().await.ok()?;
    let document = Html::parse_document(&html);
    let selector = Selector::parse(".geo").ok()?;

    if let Some(element) = document.select(&selector).next() {
        let text = element.text().collect::<Vec<_>>().join("");
        let parts: Vec<&str> = text.split(';').collect();
        if parts.len() >= 2 {
            let lat = parts[0].trim().parse::<f64>().ok()?;
            let lon = parts[1].trim().parse::<f64>().ok()?;
            return Some((lat, lon));
        }
    }
    None
}

async fn download_pbf_if_needed() -> Result<String, Box<dyn Error>> {
    let path = "stations.osm.pbf";
    if fs::metadata(path).is_ok() {
        println!("PBF file already exists: {}", path);
        return Ok(path.to_string());
    }

    println!("Downloading PBF file...");
    let url = "https://github.com/catenarytransit/osm-filter/releases/download/latest/korea-stations-latest.osm.pbf";
    let resp = reqwest::get(url).await?;
    let bytes = resp.bytes().await?;
    let mut file = fs::File::create(path)?;
    file.write_all(&bytes)?;
    println!("Downloaded PBF file to {}", path);
    Ok(path.to_string())
}

fn dist_sq(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let dx = p1.0 - p2.0;
    let dy = p1.1 - p2.1;
    dx * dx + dy * dy
}

fn get_best_candidate(candidates: &[(f64, f64)]) -> (f64, f64) {
    let seoul_center = (37.5665, 126.9780);
    let mut best_cand = candidates[0];
    let mut min_dist = dist_sq(best_cand, seoul_center);

    for &cand in &candidates[1..] {
        let d = dist_sq(cand, seoul_center);
        if d < min_dist {
            min_dist = d;
            best_cand = cand;
        }
    }
    best_cand
}

fn load_osm_stations(
    path: &str,
) -> Result<HashMap<String, Vec<(f64, f64)>>, Box<dyn Error + Send + Sync>> {
    println!("Reading OSM PBF data from {}...", path);
    let file = fs::File::open(path)?;
    let mut pbf = OsmPbfReader::new(file);
    let mut station_map: HashMap<String, Vec<(f64, f64)>> = HashMap::new();

    // We strictly only look for nodes for now to get coordinates easily.
    // If we need ways/relations, we need a way to calc centroid.
    for obj in pbf.iter().map(|r| r.unwrap()) {
        if let OsmObj::Node(node) = obj {
            let is_station = node.tags.contains("railway", "station")
                || node.tags.contains("railway", "halt")
                || node.tags.contains("station", "subway")
                || node.tags.contains("subway", "yes");

            if is_station {
                // Insert by all possible names
                let lat = node.lat();
                let lon = node.lon();
                let coords = (lat, lon);

                if let Some(name) = node.tags.get("name") {
                    station_map
                        .entry(name.to_string())
                        .or_default()
                        .push(coords);
                }
                if let Some(name_ko) = node.tags.get("name:ko") {
                    station_map
                        .entry(name_ko.to_string())
                        .or_default()
                        .push(coords);
                }
                if let Some(name_en) = node.tags.get("name:en") {
                    station_map
                        .entry(name_en.to_string())
                        .or_default()
                        .push(coords);
                }
            }
        }
    }

    println!("Loaded {} name entries from OSM PBF.", station_map.len());
    Ok(station_map)
}

async fn scrape_station(
    client: &reqwest::Client,
    station: Station,
    _index: usize,
) -> Vec<TrainSchedule> {
    // println!("Scraping #{} - {} (UID: {})", index, station.name, station.uid); // Optional logging, might require sync printing or too verbose
    // Add delay to be more patient and prevent timeouts (per-task delay)
    // Note: If using async context, use tokio::time::sleep instead of std::thread::sleep to avoid blocking the runtime thread
    //tokio::time::sleep(Duration::from_millis(100)).await;

    let time_regex = TIME_REGEX.get_or_init(|| Regex::new(r"(\d+)분").unwrap());
    let route_regex = ROUTE_REGEX.get_or_init(|| Regex::new(r"\((.+)\s*>\s*(.+)\)").unwrap());

    // Fetch
    let url = format!(
        "http://www.seoulmetro.co.kr/kr/getStationInfo.do?action=info&stationId={}",
        station.uid
    );
    let mut html_text = String::new();
    let mut success = false;

    // 1. Try with shared client (default timeout ~20s)
    // println!("Scraping {}...", station.name);
    match client
        .get(&url)
        .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do")
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
        )
        .header("Accept", "text/html, */*; q=0.01")
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await
    {
        Ok(resp) => match resp.text().await {
            Ok(text) => {
                html_text = text;
                success = true;
            }
            Err(e) => eprintln!(
                "Failed to read body for {} (default client): {}",
                station.name, e
            ),
        },
        Err(e) => eprintln!("Failed to fetch {} (default client): {}", station.name, e),
    }

    // 2. Retry with increasing timeouts if failed
    if !success {
        let timeouts = [580, 800, 800, 1000, 1000, 2000];
        for &t in &timeouts {
            eprintln!("Retrying {} with {}s timeout...", station.name, t);
            let new_client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(t))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to build client: {}", e);
                    continue;
                }
            };

            match new_client
                .get(&url)
                .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do")
                .send()
                .await
            {
                Ok(resp) => match resp.text().await {
                    Ok(text) => {
                        html_text = text;
                        success = true;
                        println!("Successfully fetched {} with {}s timeout.", station.name, t);
                        break;
                    }
                    Err(e) => eprintln!(
                        "Failed to read body for {} ({}s timeout): {}",
                        station.name, t, e
                    ),
                },
                Err(e) => eprintln!("Failed to fetch {} ({}s timeout): {}", station.name, t, e),
            }
        }
    }

    if !success {
        eprintln!("Failed to fetch/read {} after all retries.", station.name);
        return Vec::new();
    }

    // Parse
    let fragment = Html::parse_document(&html_text);
    let table_selector = Selector::parse("#page2 table.stationInfoAllTimeTable").unwrap();
    let a_selector = Selector::parse("tbody ul li a").unwrap();

    let mut station_schedules = Vec::new();

    for table in fragment.select(&table_selector) {
        let hour_str = table.value().attr("time").unwrap_or("00");

        for element in table.select(&a_selector) {
            let week_tag = element.value().attr("week").unwrap_or("1");
            let train_no = element.value().attr("train-no").unwrap_or("");
            let line_name = element.value().attr("line").unwrap_or("");
            let in_out = element.value().attr("inouttag").unwrap_or("1");
            let text = element.text().collect::<Vec<_>>().join("");

            if let Some(caps) = time_regex.captures(&text) {
                let min_str = &caps[1];
                let formatted_time = format!("{}:{}:00", hour_str, min_str);

                let dest = if let Some(route_caps) = route_regex.captures(&text) {
                    route_caps[2].trim().to_string()
                } else {
                    "Unknown".to_string()
                };

                station_schedules.push(TrainSchedule {
                    station_uid: station.uid.clone(),
                    week_tag: week_tag.to_string(),
                    train_no: train_no.to_string(),
                    line_name: line_name.to_string(),
                    in_out_tag: in_out.to_string(),
                    arrival_time: formatted_time.clone(),
                    departure_time: formatted_time,
                    dest_station: dest,
                });
            }
        }
    }

    if station_schedules.is_empty() {
        // println!("  Warning: Found 0 schedules for {}. HTML length: {}", station.name, html_text.len());
    } else {
        // println!("Fetched {} - {} schedules.", station.name, station_schedules.len());
    }

    station_schedules
}

type TrainKey = (String, String, String); // (train_no, week_tag, in_out_tag)

#[derive(Debug, Clone)]
struct DetailedStop {
    station_name: String,
    arrival_time: Option<String>,
    departure_time: Option<String>,
}

async fn fetch_train_details(
    client: &reqwest::Client,
    key: &TrainKey,
    line: &str,
    station_uid: &str,
) -> Option<Vec<DetailedStop>> {
    let (train_no, week_tag, in_out_tag) = key;
    let url = "http://www.seoulmetro.co.kr/kr/getTrainInfo.do";

    let params = [
        ("trainNo", train_no.as_str()),
        ("weekTag", week_tag.as_str()),
        ("inOutTag", in_out_tag.as_str()),
        ("line", line),
        ("stationId", station_uid),
    ];
    // println!("Fetching train details for {} {} {} (Line: {}, Stn: {})...", train_no, week_tag, in_out_tag, line, station_uid);

    // Retry logic
    let timeouts = [20, 40, 60, 100, 500]; // Increasing timeouts
    let mut html_text = String::new();
    let mut success = false;

    // 1. Try with shared client first
    match client
        .post(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
        )
        .header(
            "Referer",
            "http://www.seoulmetro.co.kr/kr/cyberStation.do?menuIdx=538",
        )
        .header("Origin", "http://www.seoulmetro.co.kr")
        .header("X-Requested-With", "XMLHttpRequest")
        .form(&params)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Ok(text) = resp.text().await {
                    html_text = text;
                    success = true;
                }
            }
        }
        Err(_) => {}
    }

    // 2. Retry loop if failed
    if !success {
        for &t in &timeouts {
            let new_client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(t))
                .build()
            {
                Ok(c) => c,
                Err(_) => continue,
            };

            match new_client.post(url)
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0")
                .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do?menuIdx=538")
                .header("Origin", "http://www.seoulmetro.co.kr")
                .header("X-Requested-With", "XMLHttpRequest")
                .form(&params)
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(text) = resp.text().await {
                            html_text = text;
                            success = true;
                            break;
                        }
                    }
                },
                Err(_) => {},
            }
        }
    }

    if !success {
        eprintln!(
            "Error: Failed to fetch details for train {} after all retries.",
            train_no
        );
        return None;
    }

    let document = Html::parse_document(&html_text);
    let table_selector = Selector::parse("table.stationInfoAllTimeTable").unwrap();
    let tr_selector = Selector::parse("tbody tr").unwrap();
    let td_selector = Selector::parse("td").unwrap();

    let mut stops = Vec::new();

    // Select the first table only
    if let Some(table) = document.select(&table_selector).next() {
        for tr in table.select(&tr_selector) {
            let tds: Vec<_> = tr.select(&td_selector).collect();
            if tds.len() >= 3 {
                let station_name = tds[0]
                    .text()
                    .collect::<Vec<_>>()
                    .join("")
                    .trim()
                    .to_string();
                let arr_raw = tds[1]
                    .text()
                    .collect::<Vec<_>>()
                    .join("")
                    .trim()
                    .to_string();
                let dep_raw = tds[2]
                    .text()
                    .collect::<Vec<_>>()
                    .join("")
                    .trim()
                    .to_string();

                let arrival_time = if arr_raw.is_empty() {
                    None
                } else {
                    Some(format!("{}:00", arr_raw))
                };
                let departure_time = if dep_raw.is_empty() {
                    None
                } else {
                    Some(format!("{}:00", dep_raw))
                };

                stops.push(DetailedStop {
                    station_name,
                    arrival_time,
                    departure_time,
                });
            }
        }
    }

    if stops.is_empty() {
        eprintln!(
            "Warning: Parsed 0 stops for train {} (HTML len: {})",
            train_no,
            html_text.len()
        );
        // Optional: print a snippet of HTML if it's suspicious
        if html_text.len() < 2000 {
            eprintln!(
                "HTML Content Snippet: {}",
                &html_text.chars().take(500).collect::<String>()
            );
        }
        None
    } else {
        Some(stops)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 1. Prepare Data Sources
    let pbf_path = download_pbf_if_needed().await?;
    // Blocking call for PBF reading since osmpbfreader is synchronous
    let osm_stations = task::spawn_blocking(move || load_osm_stations(&pbf_path))
        .await?
        .map_err(|e| e as Box<dyn Error>)?;

    // 2. Fetch station_data.js dynamically
    println!("Fetching station_data.js from server...");
    let station_data_url = "http://www.seoulmetro.co.kr/kr/getLineData.do";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent("Mozilla/5.0")
        .build()?;
    let resp = client
        .get(station_data_url)
        .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await?;
    let contents = resp.text().await?;

    let json_str = contents.trim();
    let json_str = json_str.strip_prefix("var lines = ").unwrap_or(json_str);
    let json_str = json_str.trim_end_matches(';');

    // Remove trailing commas to make it valid JSON
    let re_trailing = TRAILING_COMMA_REGEX.get_or_init(|| Regex::new(r",(\s*[}\]])").unwrap());
    let json_str_clean = re_trailing.replace_all(json_str, "$1");

    let lines_data: HashMap<String, Value> = serde_json::from_str(&json_str_clean)?;

    let mut stations: Vec<Station> = Vec::new();
    let mut station_uids = HashSet::new();
    let mut routes = HashMap::new();
    let mut route_name_map = HashMap::new();
    let mut range_cache: HashMap<String, (f64, f64)> = HashMap::new();

    for (line_key, line_val) in lines_data {
        // Extract route info
        let attr = line_val.get("attr");
        let label = attr
            .and_then(|a| a.get("data-label"))
            .and_then(|v| v.as_str())
            .unwrap_or(&line_key)
            .to_string();
        let color = attr
            .and_then(|a| a.get("data-color"))
            .and_then(|v| v.as_str())
            .unwrap_or("000000")
            .replace("#", "");

        let route = Route {
            id: line_key.clone(),
            short_name: label.clone(),
            long_name: label.clone(),
            color: color.clone(),
        };
        routes.insert(line_key.clone(), route);
        route_name_map.insert(label.clone(), line_key.clone());

        if let Some(stations_array) = line_val.get("stations").and_then(|s| s.as_array()) {
            let mut valid_stations = Vec::new();

            for s in stations_array {
                if s.get("station-nm").is_none() || s.get("data-uid").is_none() {
                    continue;
                }
                valid_stations.push(s);
            }

            if valid_stations.is_empty() {
                continue;
            }

            for s in valid_stations {
                let uid = s["data-uid"].as_str().unwrap().to_string();
                let raw_name = s["station-nm"]
                    .as_str()
                    .unwrap()
                    .to_string()
                    .replace("\r", "")
                    .replace("\n", " ");

                if station_uids.contains(&uid) {
                    continue;
                }

                station_uids.insert(uid.clone());
                let name_stripped = raw_name.split('(').next().unwrap().trim().to_string();

                let name_with_suffix = format!("{}역", name_stripped);
                let name_no_space = name_stripped.replace(" ", "");
                let name_no_space_suffix = format!("{}역", name_no_space);

                // Lookup priority:
                let (lat, lon) = if let Some(coords) = osm_stations.get(&raw_name) {
                    get_best_candidate(coords)
                } else if let Some(coords) = osm_stations.get(&name_stripped) {
                    get_best_candidate(coords)
                } else if let Some(coords) = osm_stations.get(&name_with_suffix) {
                    get_best_candidate(coords)
                } else if let Some(coords) = osm_stations.get(&name_no_space) {
                    get_best_candidate(coords)
                } else if let Some(coords) = osm_stations.get(&name_no_space_suffix) {
                    get_best_candidate(coords)
                } else if let Some(&coords) = range_cache.get(&raw_name) {
                    coords
                } else {
                    if let Some(coords) = fetch_wikipedia_coords(&client, &name_stripped).await {
                        println!("  Resolved {} via Wikipedia: {:?}", raw_name, coords);
                        range_cache.insert(raw_name.clone(), coords);
                        coords
                    } else {
                        // println!("  Failed to find coordinates for '{}' (Tried: {}, {}, {}, {})", raw_name, name_stripped, name_with_suffix, name_no_space, name_no_space_suffix);
                        (0.0, 0.0)
                    }
                };

                stations.push(Station {
                    uid,
                    name: raw_name.clone(),
                    lat,
                    lon,
                    line_name: line_key.clone(),
                    line_id: line_key.clone(),
                });
            }
        }
    }
    println!("Found {} unique stations to scrape.", stations.len());

    // 3. Scrape Timetables
    println!(
        "Starting concurrent scraping for {} stations...",
        stations.len()
    );

    // We clone the client for each task (it's cheap, Arc internally)
    // We move the station into the async block
    let fetches = stream::iter(stations.iter().cloned().enumerate())
        .map(|(i, station)| {
            let client = client.clone();
            async move { scrape_station(&client, station, i).await }
        })
        .buffer_unordered(16); // Concurrency limit

    let all_schedules_nested: Vec<Vec<TrainSchedule>> = fetches.collect().await;
    let all_schedules: Vec<TrainSchedule> = all_schedules_nested.into_iter().flatten().collect();

    println!(
        "Scraping completed. Found total {} station-schedules.",
        all_schedules.len()
    );

    // 3.5 Collect unique trains and fetch full details
    // Map key -> (line_name, station_uid)
    let mut unique_trains_map: HashMap<TrainKey, (String, String)> = HashMap::new();
    let mut train_line_map: HashMap<TrainKey, String> = HashMap::new(); // Store line name for the train

    for sched in &all_schedules {
        let key = (
            sched.train_no.clone(),
            sched.week_tag.clone(),
            sched.in_out_tag.clone(),
        );
        // We only need one instance. We prefer if we can match the station UID from schedule?
        // modifying unique_trains to be a HashMap.
        unique_trains_map
            .entry(key.clone())
            .or_insert((sched.line_name.clone(), sched.station_uid.clone()));
        train_line_map.insert(key, sched.line_name.clone());
    }

    let unique_trains_list: Vec<(TrainKey, String, String)> = unique_trains_map
        .into_iter()
        .map(|(key, (line, uid))| (key, line, uid))
        .collect();

    println!(
        "Fetch full details for {} trains...",
        unique_trains_list.len()
    );

    let fetched_details = stream::iter(unique_trains_list.into_iter())
        .map(|(key, line, uid)| {
            let client = client.clone();
            async move {
                let details = fetch_train_details(&client, &key, &line, &uid).await;
                (key, details)
            }
        })
        .buffer_unordered(128) // Concurrency
        .collect::<Vec<_>>()
        .await;

    // Build Station Name -> UID lookup
    let mut name_to_uid: HashMap<String, String> = HashMap::new();
    for s in &stations {
        // Raw name: "서울(1)"
        name_to_uid.insert(s.name.clone(), s.uid.clone());
        // Stripped: "서울"
        let stripped = s.name.split('(').next().unwrap().trim().to_string();
        name_to_uid.insert(stripped.clone(), s.uid.clone());
        name_to_uid.insert(format!("{}역", stripped), s.uid.clone());
    }

    // 4. GTFS Generation (Basic)
    println!("Generating GTFS files...");
    fs::create_dir_all("gtfs_output")?;

    // agency.txt
    let mut wtr = csv::Writer::from_path("gtfs_output/agency.txt")?;
    wtr.write_record(&["agency_id", "agency_name", "agency_url", "agency_timezone"])?;
    wtr.write_record(&[
        "1",
        "Seoul Metro",
        "http://www.seoulmetro.co.kr",
        "Asia/Seoul",
    ])?;
    wtr.flush()?;

    // stops.txt
    let mut wtr = csv::Writer::from_path("gtfs_output/stops.txt")?;
    wtr.write_record(&["stop_id", "stop_name", "stop_lat", "stop_lon"])?;
    for s in &stations {
        wtr.write_record(&[&s.uid, &s.name, &s.lat.to_string(), &s.lon.to_string()])?;
    }
    wtr.flush()?;

    // routes.txt
    let mut wtr = csv::Writer::from_path("gtfs_output/routes.txt")?;
    wtr.write_record(&[
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ])?;

    let mut sorted_routes: Vec<&Route> = routes.values().collect();
    sorted_routes.sort_by_key(|r| &r.id);

    let rail_route_ids: HashSet<&str> = ["1-GA", "1-GA-1", "A", "B", "G", "K", "KK", "S", "SH"]
        .iter()
        .cloned()
        .collect();
    let tram_route_ids: HashSet<&str> = ["E"].iter().cloned().collect();

    for route in sorted_routes {
        let route_type = if rail_route_ids.contains(route.id.as_str()) {
            "2"
        } else if tram_route_ids.contains(route.id.as_str()) {
            "0"
        } else {
            "1"
        };
        wtr.write_record(&[
            &route.id,
            &route.short_name,
            &route.long_name,
            route_type,
            &route.color,
            "FFFFFF",
        ])?;
    }
    wtr.flush()?;

    // Data collection structs
    // Key: (TrainNo, Direction, SignatureOfStops)
    // Value: Set of WeekTags (e.g., {"1", "2"})
    type TripSignature = Vec<(String, u32, u32)>; // (stop_id, arr_time_min, dep_time_min)
    #[derive(Hash, PartialEq, Eq, Clone)]
    struct TripKey {
        train_no: String,
        direction_id: String, // "0" or "1"
        signature: TripSignature,
    }
    let mut aggregated_trips: HashMap<TripKey, HashSet<String>> = HashMap::new();
    let mut trip_route_map: HashMap<TripKey, String> = HashMap::new();

    let mut trip_counter = 0;

    for (key, details_opt) in fetched_details {
        if let Some(stops) = details_opt {
            let (train_no, week_tag, direction_id_raw) = key;
            // Determine route from cached map
            let s_line = train_line_map
                .get(&(train_no.clone(), week_tag.clone(), direction_id_raw.clone()))
                .cloned()
                .unwrap_or_else(|| "Unknown".to_string());

            // Normalize s_line
            let s_line_clean = if s_line.starts_with('0') && s_line.ends_with("호선") {
                let trimmed = s_line.trim_start_matches('0');
                if route_name_map.contains_key(trimmed) {
                    trimmed.to_string()
                } else {
                    s_line.clone()
                }
            } else {
                match s_line.as_str() {
                    "김포도시철도" => "김포골드라인".to_string(),
                    "경의선" => "경의·중앙선".to_string(),
                    _ => s_line.clone(),
                }
            };

            let s_line_no_seon = s_line_clean.trim_end_matches("선");

            let route_id = if routes.contains_key(&s_line) {
                s_line.clone()
            } else if let Some(rid) = route_name_map.get(&s_line) {
                rid.clone()
            } else if let Some(rid) = route_name_map.get(&s_line_clean) {
                rid.clone()
            } else if let Some(rid) = route_name_map.get(s_line_no_seon) {
                rid.clone()
            } else {
                s_line.clone()
            };

            let gtfs_direction_id = if direction_id_raw == "1" { "0" } else { "1" };

            // Process stops to handle times and stop_ids
            let mut stops_with_time: TripSignature = Vec::new();

            for stop in &stops {
                let stop_id = if let Some(uid) = name_to_uid.get(&stop.station_name) {
                    uid.clone()
                } else {
                    continue;
                };

                // Time parsing
                let arr_s = stop.arrival_time.as_ref().or(stop.departure_time.as_ref());
                let dep_s = stop.departure_time.as_ref().or(stop.arrival_time.as_ref());

                if let (Some(arr), Some(dep)) = (arr_s, dep_s) {
                    fn parse_min(t: &str) -> u32 {
                        let parts: Vec<&str> = t.split(':').collect();
                        if parts.len() >= 2 {
                            let h: u32 = parts[0].parse().unwrap_or(0);
                            let m: u32 = parts[1].parse().unwrap_or(0);
                            h * 60 + m
                        } else {
                            0
                        }
                    }
                    let arr_m = parse_min(arr);
                    let dep_m = parse_min(dep);
                    stops_with_time.push((stop_id, arr_m, dep_m));
                }
            }

            if stops_with_time.is_empty() {
                continue;
            }

            // Calculate offsets and real times
            let mut processed_signature: TripSignature = Vec::new();
            let mut offset = 0;
            let mut last_time = 0;
            let mut max_time_found = 0;
            let mut valid_trip = true;

            for (stop_id, arr_m, dep_m) in stops_with_time {
                let mut real_arr = arr_m + offset;
                if real_arr < last_time {
                    offset += 1440;
                    real_arr += 1440;
                }

                let mut real_dep = dep_m + offset;
                if real_dep < real_arr {
                    real_dep += 1440;
                }

                last_time = real_dep;
                if real_dep > max_time_found {
                    max_time_found = real_dep;
                }

                processed_signature.push((stop_id, real_arr, real_dep));
            }

            // *** 28-Hour Check ***
            if max_time_found > 28 * 60 {
                // println!("Skipping trip {} due to excessive time: {} min ({:.1} hours)", train_no, max_time_found, max_time_found as f64 / 60.0);
                valid_trip = false;
            }

            if valid_trip {
                let t_key = TripKey {
                    train_no: train_no.clone(),
                    direction_id: gtfs_direction_id.to_string(),
                    signature: processed_signature,
                };

                aggregated_trips
                    .entry(t_key.clone())
                    .or_default()
                    .insert(week_tag);
                trip_route_map.entry(t_key).or_insert(route_id); // keep first route found
                trip_counter += 1;
            }
        }
    }

    println!("Aggregated {} unique valid trips.", aggregated_trips.len());

    // Generate Calendar and write Trips
    // We map sets of week_tags to a ServiceID.
    // e.g. {"1"} -> "S1", {"1", "2"} -> "S1_2".

    let mut service_id_map: HashMap<String, HashSet<String>> = HashMap::new(); // ServiceID -> Set<WeekTags>

    // Sort keys for deterministic output
    let mut final_trips: Vec<_> = aggregated_trips.into_iter().collect();
    final_trips.sort_by(|a, b| a.0.train_no.cmp(&b.0.train_no));

    let mut wtr_trips = csv::Writer::from_path("gtfs_output/trips.txt")?;
    wtr_trips.write_record(&[
        "route_id",
        "service_id",
        "trip_id",
        "trip_short_name",
        "direction_id",
    ])?;

    let mut wtr_st = csv::Writer::from_path("gtfs_output/stop_times.txt")?;
    wtr_st.write_record(&[
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
    ])?;

    // Track map to handle duplicates: BaseTripID -> List of Signatures
    let mut trip_signatures: HashMap<String, Vec<TripSignature>> = HashMap::new();

    for (key, week_tags) in final_trips {
        // Create normalized service ID
        let mut sorted_tags: Vec<_> = week_tags.iter().collect();
        sorted_tags.sort();
        let service_id = format!(
            "S{}",
            sorted_tags
                .into_iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("_")
        );

        // Register service ID
        service_id_map
            .entry(service_id.clone())
            .or_insert_with(|| week_tags.clone());

        let base_trip_id = format!("T_{}_{}", service_id, key.train_no);

        // Deduplication & Uniquification Logic
        let signatures = trip_signatures.entry(base_trip_id.clone()).or_default();
        let mut is_exact_duplicate = false;
        for sig in signatures.iter() {
            if sig == &key.signature {
                is_exact_duplicate = true;
                break;
            }
        }

        if is_exact_duplicate {
            // println!("Skipping duplicate trip: {}", base_trip_id);
            continue;
        }

        // New unique variant for this ID
        let final_trip_id = if signatures.is_empty() {
            base_trip_id.clone()
        } else {
            format!("{}_{}", base_trip_id, signatures.len() + 1)
        };

        signatures.push(key.signature.clone());

        let route_id = trip_route_map.get(&key).unwrap();

        wtr_trips.write_record(&[
            route_id,
            &service_id,
            &final_trip_id,
            &key.train_no,
            &key.direction_id,
        ])?;

        for (i, (stop_id, arr_m, dep_m)) in key.signature.into_iter().enumerate() {
            let fmt_time = |m: u32| format!("{:02}:{:02}:00", m / 60, m % 60);
            wtr_st.write_record(&[
                &final_trip_id,
                &fmt_time(arr_m),
                &fmt_time(dep_m),
                &stop_id,
                &(i + 1).to_string(),
            ])?;
        }
    }

    wtr_trips.flush()?;
    wtr_st.flush()?;

    // Write dynamic calendar.txt
    let mut wtr_cal = csv::Writer::from_path("gtfs_output/calendar.txt")?;
    wtr_cal.write_record(&[
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ])?;

    let mut sorted_services: Vec<_> = service_id_map.keys().collect();
    sorted_services.sort();

    for s_id in sorted_services {
        let tags = service_id_map.get(s_id).unwrap();
        let is_weekday = tags.contains("1");
        let is_saturday = tags.contains("2");
        let is_sunday = tags.contains("3");

        let mon = if is_weekday { "1" } else { "0" };
        let tue = if is_weekday { "1" } else { "0" };
        let wed = if is_weekday { "1" } else { "0" };
        let thu = if is_weekday { "1" } else { "0" };
        let fri = if is_weekday { "1" } else { "0" };
        let sat = if is_saturday { "1" } else { "0" };
        let sun = if is_sunday { "1" } else { "0" };

        wtr_cal.write_record(&[
            s_id, mon, tue, wed, thu, fri, sat, sun, "20250101", "20281231",
        ])?;
    }
    wtr_cal.flush()?;

    println!("Done! Generated files.");
    Ok(())
}
