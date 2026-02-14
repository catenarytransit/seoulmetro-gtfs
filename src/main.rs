use regex::Regex;
use scraper::{Html, Selector};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::time::Duration;
use std::io::Write; 
use std::sync::OnceLock;
use osmpbfreader::{OsmPbfReader, OsmObj};
use tokio::task;
use futures::stream::{self, StreamExt};

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

fn load_osm_stations(path: &str) -> Result<HashMap<String, Vec<(f64, f64)>>, Box<dyn Error + Send + Sync>> {
    println!("Reading OSM PBF data from {}...", path);
    let file = fs::File::open(path)?;
    let mut pbf = OsmPbfReader::new(file);
    let mut station_map: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    
    // We strictly only look for nodes for now to get coordinates easily.
    // If we need ways/relations, we need a way to calc centroid.
    for obj in pbf.iter().map(|r| r.unwrap()) {
        if let OsmObj::Node(node) = obj {
            let is_station = node.tags.contains("railway", "station") || 
                             node.tags.contains("railway", "halt") ||
                             node.tags.contains("station", "subway") ||
                             node.tags.contains("subway", "yes");
            
            if is_station {
                // Insert by all possible names
                let lat = node.lat();
                let lon = node.lon();
                let coords = (lat, lon);
                
                if let Some(name) = node.tags.get("name") {
                    station_map.entry(name.to_string()).or_default().push(coords);
                }
                if let Some(name_ko) = node.tags.get("name:ko") {
                    station_map.entry(name_ko.to_string()).or_default().push(coords);
                }
                if let Some(name_en) = node.tags.get("name:en") {
                    station_map.entry(name_en.to_string()).or_default().push(coords);
                }
            }
        }
    }
    
    println!("Loaded {} name entries from OSM PBF.", station_map.len());
    Ok(station_map)
}

async fn scrape_station(client: &reqwest::Client, station: Station, _index: usize) -> Vec<TrainSchedule> {
    // println!("Scraping #{} - {} (UID: {})", index, station.name, station.uid); // Optional logging, might require sync printing or too verbose
    // Add delay to be more patient and prevent timeouts (per-task delay)
    // Note: If using async context, use tokio::time::sleep instead of std::thread::sleep to avoid blocking the runtime thread
    //tokio::time::sleep(Duration::from_millis(100)).await;

    let time_regex = TIME_REGEX.get_or_init(|| Regex::new(r"(\d+)분").unwrap());
    let route_regex = ROUTE_REGEX.get_or_init(|| Regex::new(r"\((.+)\s*>\s*(.+)\)").unwrap());

    // Fetch
    let url = format!("http://www.seoulmetro.co.kr/kr/getStationInfo.do?action=info&stationId={}", station.uid);
    let mut html_text = String::new();
    let mut success = false;

    // 1. Try with shared client (default timeout ~20s)
    // println!("Scraping {}...", station.name);
    match client.get(&url)
        .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0")
        .header("Accept", "text/html, */*; q=0.01")
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await 
    {
        Ok(resp) => {
            match resp.text().await {
                Ok(text) => {
                    html_text = text;
                    success = true;
                },
                Err(e) => eprintln!("Failed to read body for {} (default client): {}", station.name, e),
            }
        },
        Err(e) => eprintln!("Failed to fetch {} (default client): {}", station.name, e),
    }

    // 2. Retry with increasing timeouts if failed
    if !success {
        let timeouts = [580, 1000]; 
        for &t in &timeouts {
             eprintln!("Retrying {} with {}s timeout...", station.name, t);
             let new_client = match reqwest::Client::builder()
                 .timeout(Duration::from_secs(t))
                 .build() {
                     Ok(c) => c,
                     Err(e) => {
                         eprintln!("Failed to build client: {}", e);
                         continue;
                     }
                 };
                 
             match new_client.get(&url)
                .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do")
                .send()
                .await 
             {
                 Ok(resp) => {
                     match resp.text().await {
                         Ok(text) => {
                             html_text = text;
                             success = true;
                             println!("Successfully fetched {} with {}s timeout.", station.name, t);
                             break;
                         },
                         Err(e) => eprintln!("Failed to read body for {} ({}s timeout): {}", station.name, t, e),
                     }
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
        println!("Fetched {} - {} schedules.", station.name, station_schedules.len());
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

async fn fetch_train_details(client: &reqwest::Client, key: &TrainKey) -> Option<Vec<DetailedStop>> {
    let (train_no, week_tag, in_out_tag) = key;
    let url = "http://www.seoulmetro.co.kr/kr/getTrainInfo.do";
    
    let params = [
        ("trainNo", train_no.as_str()),
        ("weekTag", week_tag.as_str()),
        ("inOutTag", in_out_tag.as_str()),
    ];
    println!("Fetching train details for {} {} {}...", train_no, week_tag, in_out_tag);

    let resp = client.post(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0")
        .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do?menuIdx=538")
        .header("Origin", "http://www.seoulmetro.co.kr")
        .header("X-Requested-With", "XMLHttpRequest")
        .form(&params)
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let html = resp.text().await.ok()?;
    // if html.len() < 500 {
    //      println!("  Short response for {}: {}", train_no, html);
    // }
    let document = Html::parse_document(&html);
    let tr_selector = Selector::parse("table.stationInfoAllTimeTable tbody tr").unwrap();
    let td_selector = Selector::parse("td").unwrap();

    let mut stops = Vec::new();

    for tr in document.select(&tr_selector) {
        let tds: Vec<_> = tr.select(&td_selector).collect();
        if tds.len() >= 3 {
            let station_name = tds[0].text().collect::<Vec<_>>().join("").trim().to_string();
            let arr_raw = tds[1].text().collect::<Vec<_>>().join("").trim().to_string();
            let dep_raw = tds[2].text().collect::<Vec<_>>().join("").trim().to_string();

            let arrival_time = if arr_raw.is_empty() { None } else { Some(format!("{}:00", arr_raw)) };
            let departure_time = if dep_raw.is_empty() { None } else { Some(format!("{}:00", dep_raw)) };

            stops.push(DetailedStop {
                station_name,
                arrival_time,
                departure_time,
            });
        }
    }
    
    if stops.is_empty() {
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
    let osm_stations = task::spawn_blocking(move || {
        load_osm_stations(&pbf_path)
    }).await?
      .map_err(|e| e as Box<dyn Error>)?;

    // 2. Fetch station_data.js dynamically
    println!("Fetching station_data.js from server...");
    let station_data_url = "http://www.seoulmetro.co.kr/kr/getLineData.do";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent("Mozilla/5.0")
        .build()?;
    let resp = client.get(station_data_url)
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
        let label = attr.and_then(|a| a.get("data-label")).and_then(|v| v.as_str()).unwrap_or(&line_key).to_string();
        let color = attr.and_then(|a| a.get("data-color")).and_then(|v| v.as_str()).unwrap_or("000000").replace("#", "");

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
                     let raw_name = s["station-nm"].as_str().unwrap().to_string().replace("\r", "").replace("\n", " ");
                     
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
    println!("Starting concurrent scraping for {} stations...", stations.len());


    
    // We clone the client for each task (it's cheap, Arc internally)
    // We move the station into the async block
    let fetches = stream::iter(stations.iter().cloned().enumerate())
        .map(|(i, station)| {
            let client = client.clone();
            async move {
                scrape_station(&client, station, i).await
            }
        })
        .buffer_unordered(16); // Concurrency limit

    let all_schedules_nested: Vec<Vec<TrainSchedule>> = fetches.collect().await;
    let all_schedules: Vec<TrainSchedule> = all_schedules_nested.into_iter().flatten().collect();

    println!("Scraping completed. Found total {} station-schedules.", all_schedules.len());
    
    // 3.5 Collect unique trains and fetch full details
    let mut unique_trains: HashSet<TrainKey> = HashSet::new();
    let mut train_line_map: HashMap<TrainKey, String> = HashMap::new(); // Store line name for the train

    for sched in &all_schedules {
        let key = (sched.train_no.clone(), sched.week_tag.clone(), sched.in_out_tag.clone());
        unique_trains.insert(key.clone());
        train_line_map.insert(key, sched.line_name.clone());
    }

    println!("Identified {} unique trains. Fetching full details...", unique_trains.len());

    let fetched_details = stream::iter(unique_trains.into_iter())
        .map(|key| {
            let client = client.clone();
            async move {
                let details = fetch_train_details(&client, &key).await;
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
    wtr.write_record(&["1", "Seoul Metro", "http://www.seoulmetro.co.kr", "Asia/Seoul"])?;
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
    wtr.write_record(&["route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"])?;
    
    let mut sorted_routes: Vec<&Route> = routes.values().collect();
    sorted_routes.sort_by_key(|r| &r.id);

    let rail_route_ids: HashSet<&str> = ["1-GA", "1-GA-1", "A", "B", "G", "K", "KK", "S", "SH"].iter().cloned().collect();
    let tram_route_ids: HashSet<&str> = ["E"].iter().cloned().collect();

    for route in sorted_routes {
        let route_type = if rail_route_ids.contains(route.id.as_str()) { "2" } else if tram_route_ids.contains(route.id.as_str()) { "0" } else { "1" };
        wtr.write_record(&[&route.id, &route.short_name, &route.long_name, route_type, &route.color, "FFFFFF"])?;
    }
    wtr.flush()?;
    
    // calendar.txt
    let mut wtr = csv::Writer::from_path("gtfs_output/calendar.txt")?;
    wtr.write_record(&["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"])?;
    wtr.write_record(&["S1", "1", "1", "1", "1", "1", "0", "0", "20250101", "20281231"])?;
    wtr.write_record(&["S2", "0", "0", "0", "0", "0", "1", "0", "20250101", "20281231"])?;
    wtr.write_record(&["S3", "0", "0", "0", "0", "0", "0", "1", "20250101", "20281231"])?;
    wtr.flush()?;
    
    // trips.txt & stop_times.txt
    let mut wtr_trips = csv::Writer::from_path("gtfs_output/trips.txt")?;
    wtr_trips.write_record(&["route_id", "service_id", "trip_id", "trip_short_name", "direction_id"])?;
    
    let mut wtr_st = csv::Writer::from_path("gtfs_output/stop_times.txt")?;
    wtr_st.write_record(&["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])?;

    let mut trip_counter = 0;
    
    for (key, details_opt) in fetched_details {
        if let Some(stops) = details_opt {
            let (train_no, week_tag, direction_id) = key;
             // Determine route from cached map
             let s_line = train_line_map.get(&(train_no.clone(), week_tag.clone(), direction_id.clone()))
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
                 // println!("Warning: Could not link scraped line '{}'", s_line);
                 s_line.clone() 
            };

            let service_id = format!("S{}", week_tag);
            let trip_id = format!("T_{}_{}", service_id, train_no);
            // direction_id from station scrape might be "1" or "2", user provided "1" in example. 
            // GTFS uses 0 and 1. The input `inOutTag` is 1 or 2 (Inner/Outer or Up/Down).
            // Let's map 1->0, 2->1 for now, or just use it as is if it fits constraints (0 or 1).
            // Actually parameter is "inOutTag".
            let gtfs_direction_id = if direction_id == "1" { "0" } else { "1" };

            wtr_trips.write_record(&[&route_id, &service_id, &trip_id, &train_no, gtfs_direction_id])?;

            // Process stops to handle times and stop_ids
            // Parse times to minutes for midnight logic
            let mut stops_with_time = Vec::new();
            for stop in stops {
                // Try to find stop_id
                let stop_id = if let Some(uid) = name_to_uid.get(&stop.station_name) {
                    uid.clone()
                } else {
                    // println!("Warning: Station name '{}' not found in station list.", stop.station_name);
                    continue; 
                };

                // Time parsing
                // If arrival is None, use departure. If departure is None, use arrival.
                let arr_s = stop.arrival_time.as_ref().or(stop.departure_time.as_ref());
                let dep_s = stop.departure_time.as_ref().or(stop.arrival_time.as_ref());

                if let (Some(arr), Some(dep)) = (arr_s, dep_s) {
                     fn parse_min(t: &str) -> u32 {
                         let parts: Vec<&str> = t.split(':').collect();
                         if parts.len() >= 2 {
                             let h: u32 = parts[0].parse().unwrap_or(0);
                             let m: u32 = parts[1].parse().unwrap_or(0);
                             h * 60 + m
                         } else { 0 }
                     }
                     let arr_m = parse_min(arr);
                     let dep_m = parse_min(dep);
                     stops_with_time.push((stop_id, arr_m, dep_m));
                }
            }
            
            if stops_with_time.is_empty() { continue; }

            // Midnight crossing check
            let min_t = stops_with_time.first().map(|x| x.1).unwrap_or(0);
            let max_t = stops_with_time.last().map(|x| x.2).unwrap_or(0);
            
            // Heuristic: if we encounter a large drop in time, it crossed midnight.
            // Or if overall span looks like it wrapped.
            // A simple per-stop check: if current time < previous time, add 24h (1440m).
            
            let mut offset = 0;
            let mut last_time = 0;
            
            for (i, (stop_id, arr_m, dep_m)) in stops_with_time.into_iter().enumerate() {
                let mut real_arr = arr_m + offset;
                if real_arr < last_time {
                    offset += 1440;
                    real_arr += 1440;
                }
                
                let mut real_dep = dep_m + offset;
                if real_dep < real_arr {
                    // This case handles if departure wraps immediately (rare but possible across 00:00:00?)
                    // Or if input data has dep < arr (invalid).
                    // Actually, if dep < arr, it probably crossed midnight *during* the stop?
                    // But our offset logic usually handles "current vs previous stop".
                    // If dep < arr matches local, it means it wrapped.
                    // Let's assume monotonic within stop.
                    if dep_m < arr_m {
                         real_dep += 1440; // This should be covered by offset if arr didn't trigger
                    }
                }
                
                last_time = real_dep;

                let fmt_time = |m: u32| {
                    format!("{:02}:{:02}:00", m/60, m%60)
                };

                wtr_st.write_record(&[
                    &trip_id,
                    &fmt_time(real_arr),
                    &fmt_time(real_dep),
                    &stop_id,
                    &(i+1).to_string()
                ])?;
            }
            trip_counter += 1;
        }
    }
    
    wtr_trips.flush()?;
    wtr_st.flush()?;
    
    println!("Done! Generated trips for {} trains.", trip_counter);
    Ok(())
}
