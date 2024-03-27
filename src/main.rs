// Try Vec, HashMap,
// BTreeMap is slower -> always has to be ordered

use std::fs::{self};
use std::io::{self, BufRead, BufWriter, Write};
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

fn main() {
    eprintln!("Starting...");
    let start = Instant::now();

    let mut weather_stations = HashMap::with_capacity(500);

    read_and_process_file("measurements-100000000.txt", &mut weather_stations);

    print_results(weather_stations);

    eprintln!("Complete Time: {:?}", start.elapsed());
}

// Will be all stored as 10 times of the actual value (having no values after the comma)
struct WheaterStation {
    count: u32,
    sum: i32,
    min: i32,
    max: i32,
}

// Time only reading 100_000_000: 4.56s
// Reusing String as line buffer. 9.4s
fn read_and_process_file(input_file: &str, weather_stations: &mut HashMap<String, WheaterStation>) {
    let start = Instant::now();

    let file = fs::File::open(input_file).unwrap();
    let mut reader = io::BufReader::with_capacity(1_000_000, file);

    let mut line = String::with_capacity(100);
    while reader.read_line(&mut line).unwrap() > 0 {
        let (station_name, measurement) = line.trim().split_once(';').unwrap();
        let station_name = station_name.to_string();
        // This is more performant, than parsing once but on an allocated string.
        let measurement = measurement.split_once('.').unwrap();
        let measurement =
            (measurement.0.parse::<i32>().unwrap() * 10) + (measurement.1.parse::<i32>().unwrap());

        weather_stations
            .entry(station_name)
            .and_modify(|ws| {
                ws.count += 1;
                ws.sum += measurement;
                if measurement < ws.min {
                    ws.min = measurement;
                }
                if measurement > ws.max {
                    ws.max = measurement;
                }
            })
            .or_insert_with(|| WheaterStation {
                count: 1,
                sum: measurement,
                min: measurement,
                max: measurement,
            });

        line.clear();
    }

    eprintln!("Reading and processing: {:?}", start.elapsed());
}

// printing 100_000_000
// using no lock: 1.9ms
// using a lock: 1.1ms
// using BufWriter: 309.9µs
fn print_results(weather_stations: HashMap<String, WheaterStation>) {
    let start = Instant::now();

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    weather_stations
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .for_each(|(k, v)| {
            writeln!(
                writer,
                "{}:{}/{}/{};",
                k,
                v.min / 10,
                v.sum / (v.count as i32) / 10,
                v.max / 10
            )
            .unwrap();
        });
    writer.flush().unwrap();

    eprintln!("Sorting and printing: {:?}", start.elapsed());
}
