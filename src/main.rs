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

fn load_osm_stations(path: &str) -> Result<HashMap<String, (f64, f64)>, Box<dyn Error + Send + Sync>> {
    println!("Reading OSM PBF data from {}...", path);
    let file = fs::File::open(path)?;
    let mut pbf = OsmPbfReader::new(file);
    let mut station_map = HashMap::new();
    
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
                
                if let Some(name) = node.tags.get("name") {
                    station_map.insert(name.to_string(), (lat, lon));
                }
                if let Some(name_ko) = node.tags.get("name:ko") {
                    station_map.insert(name_ko.to_string(), (lat, lon));
                }
                if let Some(name_en) = node.tags.get("name:en") {
                    station_map.insert(name_en.to_string(), (lat, lon));
                }
            }
        }
    }
    
    println!("Loaded {} locations from OSM PBF.", station_map.len());
    Ok(station_map)
}

async fn scrape_station(client: &reqwest::Client, station: Station, index: usize) -> Vec<TrainSchedule> {
    // println!("Scraping #{} - {} (UID: {})", index, station.name, station.uid); // Optional logging, might require sync printing or too verbose
    // Add delay to be more patient and prevent timeouts (per-task delay)
    // Note: If using async context, use tokio::time::sleep instead of std::thread::sleep to avoid blocking the runtime thread
    //tokio::time::sleep(Duration::from_millis(100)).await;

    let time_regex = TIME_REGEX.get_or_init(|| Regex::new(r"(\d+)분").unwrap());
    let route_regex = ROUTE_REGEX.get_or_init(|| Regex::new(r"\((.+)\s*>\s*(.+)\)").unwrap());

    // Fetch
    let url = format!("http://www.seoulmetro.co.kr/kr/getStationInfo.do?action=info&stationId={}", station.uid);
    let mut resp = None;
    for attempt in 0..16 {
        match client.get(&url)
            .header("Referer", "http://www.seoulmetro.co.kr/kr/cyberStation.do")
            .send()
            .await {
            Ok(r) => {
                resp = Some(r);
                break;
            },
            Err(e) => {
                if attempt < 8 {
                    // Backoff
                    tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                } else {
                    eprintln!("Failed to fetch {} after returns: {}", station.name, e);
                    return Vec::new();
                }
            }
        }
    }
    let resp = resp.unwrap();
    
    let html_text = match resp.text().await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Failed to read body {}: {}", station.name, e);
            return Vec::new();
        }
    };

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
        .timeout(Duration::from_secs(10))
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

                // Only take the first and last station
                let stations_to_scrape = if valid_stations.len() > 1 {
                    vec![valid_stations[0], valid_stations[valid_stations.len() - 1]]
                } else {
                    vec![valid_stations[0]]
                };

                for s in stations_to_scrape {
                     let uid = s["data-uid"].as_str().unwrap().to_string();
                     let raw_name = s["station-nm"].as_str().unwrap().to_string().replace("\r", "").replace("\n", " ");
                     
                     if station_uids.contains(&uid) {
                         continue;
                     }
                     station_uids.insert(uid.clone());

                    let name_stripped = raw_name.split('(').next().unwrap().trim().to_string();

                 // Filter for termini based on line label
                 let is_terminus = match label.as_str() {
                     "1호선" => ["회기", "연천", "서울", "두정", "구로", "인천", "금천구청", "광명", "병점", "서동탄", "천안", "신창", "청량리"].contains(&name_stripped.as_str()),
                     "2호선" => ["시청", "성수", "신설동", "신도림", "까치산"].contains(&name_stripped.as_str()),
                     "3호선" => ["지축", "오금", "대화"].contains(&name_stripped.as_str()),
                     "4호선" => ["남태령", "금정", "오이도", "불암산", "진접"].contains(&name_stripped.as_str()),
                     "5호선" => ["방화", "상일동", "강동", "마천", "하남검단산"].contains(&name_stripped.as_str()),
                     "6호선" => ["응암", "신내"].contains(&name_stripped.as_str()),
                     "7호선" => ["장암", "석남"].contains(&name_stripped.as_str()),
                     "8호선" => ["암사", "모란", "별내", "암사역사공원", "장자호수공원", "동구릉", "다산"].contains(&name_stripped.as_str()),
                     "9호선" => ["개화", "중앙보훈병원"].contains(&name_stripped.as_str()),
                     "우이신설경전철" => ["북한산우이", "신설동"].contains(&name_stripped.as_str()),
                     "신림선" => ["샛강", "관악산"].contains(&name_stripped.as_str()),
                     "공항철도" => ["서울", "인천공항2터미널"].contains(&name_stripped.as_str()),
                     "경의중앙선" => ["서울", "문산", "임진강", "용산", "가좌", "회기", "청량리", "용문", "지평"].contains(&name_stripped.as_str()),
                     "경춘선" => ["청량리", "상봉", "망우", "광운대", "춘천"].contains(&name_stripped.as_str()),
                     "경강선" => ["판교", "여주"].contains(&name_stripped.as_str()),
                     "수인분당선" => ["청량리", "왕십리", "수원", "인천"].contains(&name_stripped.as_str()),
                     "신분당선" => ["신사", "광교"].contains(&name_stripped.as_str()),
                     "서해선" => ["일산", "원시"].contains(&name_stripped.as_str()),
                     "인천1호선" => ["검단호수공원", "송도달빛축제공원"].contains(&name_stripped.as_str()),
                     "인천2호선" => ["검단오류", "운연"].contains(&name_stripped.as_str()),
                     "자기부상철도" => ["인천공항1터미널", "용유"].contains(&name_stripped.as_str()), 
                     "의정부경전철" => ["발곡", "차량기지 임시승강장", "탑석"].contains(&name_stripped.as_str()),
                     "용인에버라인" => ["기흥", "전대·에버랜드"].contains(&name_stripped.as_str()),
                     "김포골드라인" => ["양촌", "김포공항"].contains(&name_stripped.as_str()),
                     "GTX-A" => ["수서", "동탄", "운정중앙", "서울"].contains(&name_stripped.as_str()),
                     _ => false,
                 };

                 if !is_terminus {
                     continue;
                 }
                 
                 station_uids.insert(uid.clone());

                 let name_with_suffix = format!("{}역", name_stripped);
                 let name_no_space = name_stripped.replace(" ", "");
                 let name_no_space_suffix = format!("{}역", name_no_space);
                 
                 // Lookup priority: 
                 let (lat, lon) = if let Some(&coords) = osm_stations.get(&raw_name) {
                     coords
                 } else if let Some(&coords) = osm_stations.get(&name_stripped) {
                     coords
                 } else if let Some(&coords) = osm_stations.get(&name_with_suffix) {
                     coords
                 } else if let Some(&coords) = osm_stations.get(&name_no_space) {
                     coords
                 } else if let Some(&coords) = osm_stations.get(&name_no_space_suffix) {
                     coords
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
                     name: raw_name,
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
        .buffer_unordered(4); // Concurrency limit

    let all_schedules_nested: Vec<Vec<TrainSchedule>> = fetches.collect().await;
    let all_schedules: Vec<TrainSchedule> = all_schedules_nested.into_iter().flatten().collect();

    println!("Scraping completed. Found total {} schedules.", all_schedules.len());
    
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

    for route in sorted_routes {
        wtr.write_record(&[&route.id, &route.short_name, &route.long_name, "1", &route.color, "FFFFFF"])?;
    }
    wtr.flush()?;
    
    // calendar.txt
    let mut wtr = csv::Writer::from_path("gtfs_output/calendar.txt")?;
    wtr.write_record(&["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"])?;
    wtr.write_record(&["S1", "1", "1", "1", "1", "1", "0", "0", "20240101", "20251231"])?;
    wtr.write_record(&["S2", "0", "0", "0", "0", "0", "1", "0", "20240101", "20251231"])?;
    wtr.write_record(&["S3", "0", "0", "0", "0", "0", "0", "1", "20240101", "20251231"])?;
    wtr.flush()?;
    
    // stop_times.txt and trips.txt
    let mut trip_map: HashMap<(String, String), Vec<&TrainSchedule>> = HashMap::new();
    for sched in &all_schedules {
        let key = (sched.train_no.clone(), sched.week_tag.clone());
        trip_map.entry(key).or_insert(Vec::new()).push(sched);
    }
    
    let mut wtr_trips = csv::Writer::from_path("gtfs_output/trips.txt")?;
    wtr_trips.write_record(&["route_id", "service_id", "trip_id", "trip_short_name", "direction_id"])?;
    
    let mut wtr_st = csv::Writer::from_path("gtfs_output/stop_times.txt")?;
    wtr_st.write_record(&["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])?;
    
    for ((train_no, week_tag), events) in trip_map {
        let mut events = events.clone();
        events.sort_by(|a, b| a.arrival_time.cmp(&b.arrival_time));
        
        let s_line = &events[0].line_name;
        
        // Normalize s_line
        let s_line_clean = if s_line.starts_with('0') && s_line.ends_with("호선") {
            let trimmed = s_line.trim_start_matches('0');
            if route_name_map.contains_key(trimmed) {
                trimmed.to_string()
            } else {
                 s_line.clone()
            }
        } else {
            s_line.clone()
        };

        let route_id = if routes.contains_key(s_line) {
            s_line.clone()
        } else if let Some(rid) = route_name_map.get(s_line) {
            rid.clone()
        } else if let Some(rid) = route_name_map.get(&s_line_clean) {
            rid.clone()
        } else {
             println!("Warning: Could not link scraped line '{}' (clean: '{}') to a known route from station_data.js", s_line, s_line_clean);
             s_line.clone() 
        };
        
        let service_id = format!("S{}", week_tag);
        let trip_id = format!("T_{}_{}", service_id, train_no);
        let direction_id = if events[0].in_out_tag == "1" { "0" } else { "1" };
        
        wtr_trips.write_record(&[route_id.as_str(), service_id.as_str(), trip_id.as_str(), train_no.as_str(), direction_id])?;
        
        for (i, bev) in events.iter().enumerate() {
            wtr_st.write_record(&[
                trip_id.as_str(),
                bev.arrival_time.as_str(),
                bev.departure_time.as_str(),
                bev.station_uid.as_str(),
                (i+1).to_string().as_str()
            ])?;
        }
    }
    
    wtr_trips.flush()?;
    wtr_st.flush()?;
    
    println!("Done!");
    Ok(())
}
