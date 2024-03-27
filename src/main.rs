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

struct WheaterStation {
    count: u32,
    sum: f32,
    min: f32,
    max: f32,
}

fn read_and_process_file(input_file: &str, weather_stations: &mut HashMap<String, WheaterStation>) {
    let file = fs::File::open(input_file).unwrap();
    let reader = io::BufReader::with_capacity(1_000_000, file);
    eprintln!("cap of buf reader {:?}", reader.capacity());
    let contents = reader.lines();

    contents
        .map(|e| e.unwrap())
        .filter(|line| !line.is_empty())
        .for_each(|line| {
            let (station_name, measurement) = line.split_once(';').unwrap();
            let station_name = station_name.to_string();
            let measurement = measurement.parse::<f32>().unwrap();
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
        })
}

// printing 100_000_000
// using no lock: 1.9 ms
// using a lock: 1.1 ms
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
                v.min,
                v.sum / (v.count as f32),
                v.max
            )
            .unwrap();
        });
    writer.flush().unwrap();

    eprintln!("Sorting and printing: {:?}", start.elapsed());
}
