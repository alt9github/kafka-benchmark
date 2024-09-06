use super::config::{ProducerBenchmark, ProducerScenario, ProducerType};
use super::units::{Bytes, Messages, Seconds};
use content::CachedMessages;
use futures::{self, Future};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{
    BaseProducer, BaseRecord, DeliveryResult, FutureProducer, FutureRecord, Producer,
    ProducerContext,
};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::ClientContext;
use std::cmp;
use std::iter::{IntoIterator, Iterator};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod content;

struct BenchmarkProducerContext {
    failure_counter: Arc<AtomicUsize>,
}

impl BenchmarkProducerContext {
    fn new() -> BenchmarkProducerContext {
        BenchmarkProducerContext {
            failure_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ClientContext for BenchmarkProducerContext {}

impl ProducerContext for BenchmarkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, r: &DeliveryResult, _: Self::DeliveryOpaque) {
        if r.is_err() {
            self.failure_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn base_producer_thread(
    thread_id: u64,
    scenario: &ProducerScenario,
    cache: &CachedMessages,
) -> ThreadStats {
    let producer_context = BenchmarkProducerContext::new();
    let failure_counter = Arc::clone(&producer_context.failure_counter);
    let producer: BaseProducer<BenchmarkProducerContext> = scenario
        .client_config()
        .create_with_context(producer_context)
        .expect("Producer creation failed");
    let base_record = BaseRecord {
        topic: &scenario.topic,
        partition: None,
        payload: Some("warmup"),
        key: None,
        timestamp: None,
        headers: None,
        delivery_opaque: (),
    };

    producer
        .send::<str, str>(base_record)
        .expect("Producer error");
    failure_counter.store(0, Ordering::Relaxed);
    producer.flush(Duration::from_secs(10)).unwrap();

    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    for (count, content) in cache
        .into_iter()
        .take(per_thread_messages as usize)
        .enumerate()
    {
        loop {
            let base_record = BaseRecord {
                topic: &scenario.topic,
                partition: Some(count as i32 % 3),
                payload: Some(content),
                key: None,
                timestamp: None,
                headers: None,
                delivery_opaque: (),
            };

            match producer.send::<[u8], [u8]>(base_record) {
                Err((e, _record))
                    if e.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) =>
                {
                    producer.poll(Duration::from_millis(10));
                    continue;
                }
                Err((e, _record)) => {
                    println!("Error {:?}", e);
                    break;
                }
                Ok(_) => break,
            }
        }
        producer.poll(Duration::from_secs(0));
    }
    producer.flush(Duration::from_secs(120)).unwrap();
    ThreadStats::new(start.elapsed(), failure_counter.load(Ordering::Relaxed))
}

async fn wait_all(futures: &mut Vec<impl Future<Output = OwnedDeliveryResult> + Sized>) -> usize {
    let mut failures = 0;
    for future in futures.drain(..) {
        if let Err((kafka_error, _message)) = future.await {
            println!("Kafka error: {:?}", kafka_error);
            failures += 1;
        }
    }
    failures
}

async fn future_producer_thread(
    thread_id: u64,
    scenario: &ProducerScenario,
    cache: &CachedMessages,
) -> ThreadStats {
    println!("Thread {} started", thread_id);
    let producer: FutureProducer<_> = scenario
        .client_config()
        .create()
        .expect("Producer creation failed");

    let base_record = FutureRecord {
        topic: &scenario.topic,
        partition: None,
        payload: Some("warmup"),
        key: None,
        timestamp: None,
        headers: None,
    };
    producer
        .send_result::<str, str>(base_record)
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    let mut failures = 0;
    let mut futures = Vec::with_capacity(1_000_000);
    for (count, content) in cache
        .into_iter()
        .take(per_thread_messages as usize)
        .enumerate()
    {
        let future_record = FutureRecord {
            topic: &scenario.topic,
            partition: Some(count as i32 % 3),
            payload: Some(content),
            key: None,
            timestamp: None,
            headers: None,
        };

        futures.push(producer.send::<[u8], [u8], _>(future_record, Timeout::Never));
        if futures.len() >= 1_000_000 {
            failures += wait_all(&mut futures).await;
        }
    }
    failures += wait_all(&mut futures).await;
    producer.flush(Duration::from_secs(120)).unwrap();
    ThreadStats::new(start.elapsed(), failures)
}

pub fn run(config: &ProducerBenchmark, scenario_name: &str) {
    let scenario = config
        .scenarios
        .get(scenario_name)
        .expect("The specified scenario cannot be found");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .worker_threads(scenario.threads as usize)
        .build()
        .unwrap();

    let cache = Arc::new(CachedMessages::new(scenario.message_size, 1_000_000));
    println!(
        "Scenario: {}, repeat {} times, {} seconds pause after each",
        scenario_name, scenario.repeat_times, scenario.repeat_pause
    );
    runtime.block_on(async {
        let mut benchmark_stats = ProducerBenchmarkStats::new(scenario);
        for i in 0..scenario.repeat_times {
            let mut scenario_stats = ProducerRunStats::new(scenario);

            let mut join_set = tokio::task::JoinSet::new();

            for i in 0..scenario.threads {
                let scenario = scenario.clone();
                let cache = Arc::clone(&cache);
                join_set.spawn(async move {
                    match scenario.producer_type {
                        ProducerType::BaseProducer => base_producer_thread(i, &scenario, &cache),
                        ProducerType::FutureProducer => {
                            future_producer_thread(i, &scenario, &cache).await
                        }
                    }
                });
            }

            for stats in join_set.join_all().await {
                scenario_stats.merge_thread_stats(&stats);
            }

            scenario_stats.print();
            benchmark_stats.add_stat(scenario_stats);
            if i != scenario.repeat_times - 1 {
                tokio::time::sleep(Duration::from_secs(scenario.repeat_pause)).await;
            }
        }
        benchmark_stats.print();
    });
}

#[derive(Debug)]
pub struct ThreadStats {
    duration: Duration,
    failure_count: usize,
}

impl ThreadStats {
    pub fn new(duration: Duration, failure_count: usize) -> ThreadStats {
        ThreadStats {
            duration,
            failure_count,
        }
    }
}

#[derive(Debug)]
pub struct ProducerRunStats<'a> {
    scenario: &'a ProducerScenario,
    failure_count: usize,
    duration: Duration,
}

impl<'a> ProducerRunStats<'a> {
    pub fn new(scenario: &'a ProducerScenario) -> ProducerRunStats<'a> {
        ProducerRunStats {
            scenario,
            failure_count: 0,
            duration: Duration::from_secs(0),
        }
    }

    pub fn merge_thread_stats(&mut self, thread_stats: &ThreadStats) {
        self.failure_count += thread_stats.failure_count;
        self.duration = cmp::max(self.duration, thread_stats.duration);
    }

    pub fn print(&self) {
        let time = Seconds(self.duration);
        let messages = Messages::from(self.scenario.message_count);
        let bytes = Bytes::from(self.scenario.message_count * self.scenario.message_size);

        if self.failure_count != 0 {
            println!(
                "Warning: {} messages failed to be delivered",
                self.failure_count
            );
        }

        println!(
            "* Produced {} ({}) in {} using {} thread{}\n    {}\n    {}",
            messages,
            bytes,
            time,
            self.scenario.threads,
            if self.scenario.threads > 1 { "s" } else { "" },
            messages / time,
            bytes / time
        );
    }
}

pub struct ProducerBenchmarkStats<'a> {
    scenario: &'a ProducerScenario,
    stats: Vec<ProducerRunStats<'a>>,
}

impl<'a> ProducerBenchmarkStats<'a> {
    pub fn new(scenario: &'a ProducerScenario) -> ProducerBenchmarkStats<'a> {
        ProducerBenchmarkStats {
            scenario,
            stats: Vec::new(),
        }
    }

    pub fn add_stat(&mut self, scenario_stat: ProducerRunStats<'a>) {
        self.stats.push(scenario_stat)
    }

    pub fn print(&self) {
        let time = Seconds(self.stats.iter().map(|stat| stat.duration).sum());
        let messages = Messages::from(self.scenario.message_count * self.stats.len() as u64);
        let bytes = Bytes(messages.0 * self.scenario.message_size as f64);

        println!("Average: {}, {}", messages / time, bytes / time);
    }
}
