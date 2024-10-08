mod config;
mod consumer;
mod producer;
mod units;

use clap::clap_app;
use config::{ConsumerBenchmark, ProducerBenchmark};

fn main() {
    let matches = clap_app!(app =>
        (name: "kafka benchmark")
        (@arg benchmark_type: +takes_value +required "Benchmark type ('producer' or 'consumer')")
        (@arg config: +takes_value +required "The configuration file")
        (@arg scenario: +takes_value +required "The scenario you want to execute")
    )
    .get_matches();

    env_logger::init();

    let config_file = matches.value_of("config").unwrap();
    let scenario_name = matches.value_of("scenario").unwrap();

    match matches.value_of("benchmark_type").unwrap() {
        "consumer" => consumer::run(&ConsumerBenchmark::from_file(config_file), scenario_name),
        "producer" => producer::run(&ProducerBenchmark::from_file(config_file), scenario_name),
        _ => println!("Undefined benchmark type. Please use 'producer' or 'consumer'"),
    }
}
