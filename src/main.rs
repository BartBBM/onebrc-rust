// Try Vec, HashMap,
// BTreeMap is slower -> always has to be ordered

use std::fs::{self};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

const WORKERS: usize = 10;
const CHUNK_SIZE: usize = 1_000_000 + 100;

// Will be all stored as 10 times of the actual value (having no values after the comma)
struct WeatherStation {
    count: u32,
    sum: i32,
    min: i16,
    max: i16,
}
fn main() {
    eprintln!("Starting...");
    let start = Instant::now();

    let mut senders = Vec::with_capacity(WORKERS);
    let mut receivers = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let (sender, receiver) = mpsc::channel();
        senders.push(sender);
        receivers.push(receiver);
    }
    let reader_thread = read_chunks(senders, "measurements-1000_000_000.txt".to_string());

    let mut worker_thread_handles = Vec::with_capacity(WORKERS);
    receivers
        .into_iter()
        .for_each(|rcv| worker_thread_handles.push(process_chunks(rcv)));

    reader_thread.join().unwrap();
    let fun = worker_thread_handles.into_iter().map(|h| h.join().unwrap());

    let weather_stations = merge_results(fun);
    print_results(weather_stations);

    eprintln!("Complete Time: {:?}", start.elapsed());
}

fn read_chunks(senders: Vec<Sender<(Vec<u8>, usize)>>, input_file: String) -> JoinHandle<()> {
    thread::spawn(move || {
        let start = Instant::now();

        let file = fs::File::open(input_file).unwrap();
        let mut reader = io::BufReader::with_capacity(10_000_000, file);

        let mut chunk_count = 0;
        loop {
            let mut buffer: Vec<u8> = vec![0; CHUNK_SIZE];
            let mut read_bytes = reader.read(&mut buffer[0..CHUNK_SIZE - 100]).unwrap();

            // indicates EOF
            if read_bytes == 0 {
                break;
            }

            // could be maybe wrong
            if read_bytes <= CHUNK_SIZE - 100 {
                const NEWLINE: u8 = 10;
                let mut buffer_till_newline: Vec<u8> = Vec::with_capacity(100);
                let additional_bytes_read = reader
                    .read_until(NEWLINE, &mut buffer_till_newline)
                    .unwrap();
                let mut buffer_end_slice = &mut buffer[read_bytes..];
                buffer_end_slice.write_all(&buffer_till_newline).unwrap();
                read_bytes += additional_bytes_read;
            }

            // println!("read bytes {}", read_bytes);
            senders[chunk_count % WORKERS]
                .send((buffer, read_bytes))
                .unwrap();
            chunk_count += 1;
        }

        eprintln!("Time reading and sending: {:?}", start.elapsed());
    })
}

fn process_chunks(
    receiver: Receiver<(Vec<u8>, usize)>,
) -> JoinHandle<HashMap<String, WeatherStation>> {
    thread::spawn(move || {
        let start = Instant::now();
        let mut weather_stations = HashMap::with_capacity(500);

        // let mut counter = 0;
        for received in receiver {
            // println!("### I received part {counter} len {}, ", received.1);
            // counter += 1;
            received.0[..received.1]
                .lines()
                .map(|e| e.unwrap())
                .for_each(|line| {
                    let (station_name, measurement) = line.trim().split_once(';').unwrap();
                    let station_name = station_name.to_string();
                    // This is more performant, than parsing once but on an allocated string.
                    let measurement = measurement.split_once('.').unwrap();
                    let measurement = (measurement.0.parse::<i16>().unwrap() * 10)
                        + (measurement.1.parse::<i16>().unwrap());

                    weather_stations
                        .entry(station_name)
                        .and_modify(|ws: &mut WeatherStation| {
                            ws.count += 1;
                            ws.sum += measurement as i32;
                            if measurement < ws.min {
                                ws.min = measurement;
                            }
                            if measurement > ws.max {
                                ws.max = measurement;
                            }
                        })
                        .or_insert_with(|| WeatherStation {
                            count: 1,
                            sum: measurement as i32,
                            min: measurement,
                            max: measurement,
                        });

                    // line.clear();
                });
        }

        eprintln!("Processing: {:?}", start.elapsed());
        weather_stations
    })
}

// Time only reading 100_000_000: 4.56s
// Reusing String as line buffer. 9.4s
/* fn read_and_process_file(input_file: &str, weather_stations: &mut HashMap<String, WeatherStation>) {
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
            (measurement.0.parse::<i16>().unwrap() * 10) + (measurement.1.parse::<i16>().unwrap());

        weather_stations
            .entry(station_name)
            .and_modify(|ws| {
                ws.count += 1;
                ws.sum += measurement as i32;
                if measurement < ws.min {
                    ws.min = measurement;
                }
                if measurement > ws.max {
                    ws.max = measurement;
                }
            })
            .or_insert_with(|| WeatherStation {
                count: 1,
                sum: measurement as i32,
                min: measurement,
                max: measurement,
            });

        line.clear();
    }

    eprintln!("Reading and processing: {:?}", start.elapsed());
} */

fn merge_results(
    fun: std::iter::Map<
        std::vec::IntoIter<JoinHandle<HashMap<String, WeatherStation>>>,
        impl FnMut(JoinHandle<HashMap<String, WeatherStation>>) -> HashMap<String, WeatherStation>,
    >,
) -> HashMap<String, WeatherStation> {
    let start = Instant::now();

    let fun = fun
        .reduce(|mut acc, e| {
            e.into_iter().for_each(|record_in_e| {
                acc.entry(record_in_e.0)
                    .and_modify(|ws_in_acc| {
                        ws_in_acc.count += record_in_e.1.count;
                        ws_in_acc.sum += record_in_e.1.sum;
                        if record_in_e.1.min < ws_in_acc.min {
                            ws_in_acc.min = record_in_e.1.min;
                        }
                        if record_in_e.1.max > ws_in_acc.max {
                            ws_in_acc.max = record_in_e.1.max;
                        }
                    })
                    .or_insert_with(|| record_in_e.1);
            });
            acc
        })
        .unwrap();

    eprintln!("Merging: {:?}", start.elapsed());
    fun
}

// printing 100_000_000
// using no lock: 1.9ms
// using a lock: 1.1ms
// using BufWriter: 309.9µs
fn print_results(weather_stations: HashMap<String, WeatherStation>) {
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
