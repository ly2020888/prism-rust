pub mod memory_pool;

use crate::block::content;
use crate::block::header::Header;
use crate::block::{Block, Content};
use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;
use crate::config::*;
use crate::crypto::hash::{Hashable, H256};
use crate::crypto::merkle::MerkleTree;
// use crate::experiment::performance_counter::PERFORMANCE_COUNTER;
// use crate::handler::new_validated_block;
use crate::network::message::Message;
use crate::network::server::Handle as ServerHandle;

use log::info;

use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use memory_pool::MemoryPool;
use std::time;
use std::time::SystemTime;

use rand::distributions::Distribution;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::thread;

use rand::Rng;

enum ControlSignal {
    Start(u64, bool), // the number controls the lambda of interval between block generation
    Step,
    Exit,
}

#[derive(Ord, Eq, PartialOrd, PartialEq)]
pub enum ContextUpdateSignal {
    // New proposer block comes, we need to update all contents' parent
    NewProposerBlock,
    // New voter block comes, we need to update that voter chain
    NewVoterBlock,
}

enum OperatingState {
    Paused,
    Run(u64, bool),
    Step,
    ShutDown,
}

pub struct Context {
    blockdb: Arc<BlockDatabase>,
    blockchain: Arc<BlockChain>,
    mempool: Arc<Mutex<MemoryPool>>,
    /// Channel for receiving control signal
    control_chan: Receiver<ControlSignal>,
    /// Channel for notifying miner of new content
    context_update_chan: Receiver<ContextUpdateSignal>,
    context_update_tx: Sender<ContextUpdateSignal>,
    server: ServerHandle,
    header: Header,
    config: BlockchainConfig,
}

#[derive(Clone)]
pub struct Handle {
    // Channel for sending signal to the miner thread
    control_chan: Sender<ControlSignal>,
}

pub fn new(
    mempool: &Arc<Mutex<MemoryPool>>,
    blockchain: &Arc<BlockChain>,
    blockdb: &Arc<BlockDatabase>,
    ctx_update_source: Receiver<ContextUpdateSignal>,
    ctx_update_tx: &Sender<ContextUpdateSignal>,
    server: &ServerHandle,
    config: BlockchainConfig,
) -> (Context, Handle) {
    let (signal_chan_sender, signal_chan_receiver) = unbounded();

    let ctx = Context {
        blockdb: Arc::clone(blockdb),
        blockchain: Arc::clone(blockchain),
        mempool: Arc::clone(mempool),
        control_chan: signal_chan_receiver,
        context_update_chan: ctx_update_source,
        context_update_tx: ctx_update_tx.clone(),
        server: server.clone(),
        header: Header {
            parent: config.proposer_genesis,
            timestamp: get_time(),
            nonce: 0,
            content_merkle_root: H256::default(),
        },

        config,
    };

    let handle = Handle {
        control_chan: signal_chan_sender,
    };

    (ctx, handle)
}

impl Handle {
    pub fn exit(&self) {
        self.control_chan.send(ControlSignal::Exit).unwrap();
    }

    pub fn start(&self, lambda: u64, lazy: bool) {
        self.control_chan
            .send(ControlSignal::Start(lambda, lazy))
            .unwrap();
    }

    pub fn step(&self) {
        self.control_chan.send(ControlSignal::Step).unwrap();
    }
}

impl Context {
    pub fn start(mut self) {
        thread::Builder::new()
            .name("miner".to_string())
            .spawn(move || {
                self.miner_loop();
            })
            .unwrap();
        info!("Miner initialized into paused mode");
    }

    fn miner_loop(&mut self) {
        let mut rng = rand::thread_rng();

        // main mining loop
        loop {
            let block_start = time::Instant::now();

            // check whether there is new content through context update channel
            let mut new_voter_block: bool = false;
            let mut new_proposer_block: bool = false;
            for sig in self.context_update_chan.try_iter() {
                match sig {
                    ContextUpdateSignal::NewProposerBlock => new_proposer_block = true,
                    ContextUpdateSignal::NewVoterBlock => new_voter_block = true,
                }
            }
            // 收到出块信号
        }
    }

    /// Given a valid header, sortition its hash and create the block
    fn produce_block(&self, header_hash: H256) -> Block {
        unimplemented!();
    }
}

/// Get the current UNIX timestamp
fn get_time() -> u128 {
    let cur_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    match cur_time {
        Ok(v) => {
            return v.as_millis();
        }
        Err(e) => println!("Error parsing time: {:?}", e),
    }
    // TODO: there should be a better way of handling this, or just unwrap and panic
    0
}

#[cfg(test)]
mod tests {}
