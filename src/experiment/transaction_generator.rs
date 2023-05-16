// use crate::experiment::performance_counter::PERFORMANCE_COUNTER;
use crate::handler::new_transaction;
use crate::miner::memory_pool::MemoryPool;
use crate::network::server::Handle as ServerHandle;

use crate::wallet::Wallet;
use crossbeam::channel;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time;
use tracing::{debug, error, info};

pub enum ControlSignal {
    Start(u64),
    Step(u64),
    Stop,
    SetArrivalDistribution(ArrivalDistribution),
    SetValueDistribution(ValueDistribution),
}

pub enum ArrivalDistribution {
    Uniform(UniformArrival),
}

pub struct UniformArrival {
    pub interval: u64, // ms
}

pub enum ValueDistribution {
    Uniform(UniformValue),
}

pub struct UniformValue {
    pub min: u64,
    pub max: u64,
}

enum State {
    Continuous(u64),
    Paused,
    Step(u64),
}

pub struct TransactionGenerator {
    wallets: Vec<Wallet>,
    server: ServerHandle,
    mempool: Arc<Mutex<MemoryPool>>,
    control_chan: channel::Receiver<ControlSignal>,
    arrival_distribution: ArrivalDistribution,
    value_distribution: ValueDistribution,
    state: State,
}

impl TransactionGenerator {
    pub fn new(
        wallets: Vec<Wallet>,
        server: &ServerHandle,
        mempool: &Arc<Mutex<MemoryPool>>,
    ) -> (Self, channel::Sender<ControlSignal>) {
        let (tx, rx) = channel::unbounded();
        let instance = Self {
            wallets,
            server: server.clone(),
            mempool: Arc::clone(mempool),
            control_chan: rx,
            arrival_distribution: ArrivalDistribution::Uniform(UniformArrival { interval: 100 }),
            value_distribution: ValueDistribution::Uniform(UniformValue { min: 50, max: 100 }),
            state: State::Continuous(10000),
        };
        (instance, tx)
    }

    fn handle_control_signal(&mut self, signal: ControlSignal) {
        match signal {
            ControlSignal::Start(t) => {
                self.state = State::Continuous(t);
                info!("Transaction generator started");
            }
            ControlSignal::Stop => {
                self.state = State::Paused;
                info!("Transaction generator paused");
            }
            ControlSignal::Step(num) => {
                self.state = State::Step(num);
                info!(
                    "Transaction generator started to generate {} transactions",
                    num
                );
            }
            ControlSignal::SetArrivalDistribution(new) => {
                self.arrival_distribution = new;
            }
            ControlSignal::SetValueDistribution(new) => {
                self.value_distribution = new;
            }
        }
    }

    pub fn start(mut self) -> JoinHandle<()> {
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            // TODO: make it flexible
            loop {
                let addr_input = rng.gen_range(0, self.wallets.len());
                let addr_output = rng.gen_range(0, self.wallets.len());
                if addr_input == addr_output {
                    continue;
                }
                let addr_output = self.wallets[addr_output].address();

                let tx_gen_start = time::Instant::now();
                // check the current state and try to receive control message
                match self.state {
                    State::Continuous(_) | State::Step(_) => match self.control_chan.try_recv() {
                        Ok(signal) => {
                            self.handle_control_signal(signal);
                            continue;
                        }
                        Err(channel::TryRecvError::Empty) => {}
                        Err(channel::TryRecvError::Disconnected) => {
                            panic!("Transaction generator control channel detached")
                        }
                    },
                    State::Paused => {
                        // block until we get a signal
                        let signal = self.control_chan.recv().unwrap();
                        self.handle_control_signal(signal);
                        continue;
                    }
                }
                // check whether the mempool is already full
                if let State::Continuous(throttle) = self.state {
                    let size = { self.mempool.lock().unwrap().len() };
                    if size as u64 >= throttle {
                        // if the mempool is full, just skip this transaction
                        let interval: u64 = match &self.arrival_distribution {
                            ArrivalDistribution::Uniform(d) => d.interval,
                        };
                        let interval = time::Duration::from_micros(interval);
                        thread::sleep(interval);
                        continue;
                    }
                }
                let value: u64 = match &self.value_distribution {
                    ValueDistribution::Uniform(d) => {
                        if d.min == d.max {
                            d.min
                        } else {
                            rng.gen_range(d.min, d.max)
                        }
                    }
                };
                let transaction = self.wallets[addr_input].create_transaction(addr_output, value);
                // PERFORMANCE_COUNTER.record_generate_transaction(&transaction);
                match transaction {
                    Ok(t) => {
                        new_transaction(t, &self.mempool, &self.server);
                        // if we are in stepping mode, decrease the step count
                        if let State::Step(step_count) = self.state {
                            if step_count - 1 == 0 {
                                self.state = State::Paused;
                            } else {
                                self.state = State::Step(step_count - 1);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to generate transaction: {}", e);
                    }
                };
                let interval: u64 = match &self.arrival_distribution {
                    ArrivalDistribution::Uniform(d) => d.interval,
                };
                let interval = time::Duration::from_micros(interval);
                let time_spent = time::Instant::now().duration_since(tx_gen_start);
                let interval = {
                    if interval > time_spent {
                        interval - time_spent
                    } else {
                        time::Duration::new(0, 0)
                    }
                };
                thread::sleep(interval);
            }
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use tokio::sync::mpsc::{self};
    use tracing::{debug, Level};
    use tracing_subscriber::FmtSubscriber;

    use crate::{
        experiment::transaction_generator, miner::memory_pool::MemoryPool, network::server, wallet,
    };

    fn init() {
        // a builder for `FmtSubscriber`.
        let subscriber = FmtSubscriber::builder()
            // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
            // will be written to stdout.
            .with_max_level(Level::TRACE)
            // completes the builder.
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }

    #[test]
    fn test_txgen() {
        init();
        let mempool_size = 1_000_000;
        let mempool = MemoryPool::new(mempool_size);
        let mempool = Arc::new(std::sync::Mutex::new(mempool));
        debug!("Initialized mempool, maximum size set to {}", mempool_size);

        // start the p2p server
        // parse p2p server address
        let p2p_addr = "127.0.0.1:8079".parse::<SocketAddr>().unwrap();

        // create channels between server and worker, worker and miner, miner and worker
        let (msg_tx, _msg_rx) = mpsc::channel(100);

        let (server_ctx, server) = server::new(p2p_addr, msg_tx).unwrap();
        server_ctx.start().unwrap();

        let wallets = wallet::util::load_wallets(1_000_000);

        let (txgen_ctx, _txgen_control_chan) =
            transaction_generator::TransactionGenerator::new(wallets, &server, &mempool);
        let handle = txgen_ctx.start();

        handle.join().unwrap();
    }
}
