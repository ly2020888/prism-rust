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

use tracing::{error, info};

use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use memory_pool::MemoryPool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;

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
    operating_state: OperatingState,
    server: ServerHandle,
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
        operating_state: OperatingState::Paused,
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

    fn handle_control_signal(&mut self, signal: ControlSignal) {
        match signal {
            ControlSignal::Exit => {
                info!("Miner shutting down");
                self.operating_state = OperatingState::ShutDown;
            }
            ControlSignal::Start(i, l) => {
                info!(
                    "Miner starting in continuous mode with lambda {} and lazy mode {}",
                    i, l
                );
                self.operating_state = OperatingState::Run(i, l);
            }
            ControlSignal::Step => {
                info!("Miner starting in stepping mode");
                self.operating_state = OperatingState::Step;
            }
        }
    }

    fn miner_loop(&mut self) {
        let mut rng = rand::thread_rng();

        // main mining loop
        loop {
            // let block_start = time::Instant::now();

            // check and react to control signals
            match self.operating_state {
                OperatingState::Paused => {
                    let signal = self.control_chan.recv().unwrap();
                    self.handle_control_signal(signal);
                    continue;
                }
                OperatingState::ShutDown => {
                    return;
                }
                _ => match self.control_chan.try_recv() {
                    Ok(signal) => {
                        self.handle_control_signal(signal);
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => panic!("Miner control channel detached"),
                },
            }

            // check whether there is new content through context update channel

            if let OperatingState::ShutDown = self.operating_state {
                return;
            }

            // 收到生产区块的请求
            // 1. 打包区块，立刻进行本地接受区块的步骤（共识）。
            // 2. 广播区块。

            // 领导者区块
            // 第一步：构造区块体
            let time_stamp = get_time();
            let chain_id = self.config.node_id;
            let parent_hash = self.blockchain.get_parent_hash(chain_id as usize);
            let height = self.blockchain.get_current_height();
            let weight = self.config.block_weight;
            let mut block_content =
                content::Content::new(chain_id, parent_hash, vec![], vec![], weight, height);

            // 第二步：打包交易
            let number_of_tx = self.config.tx_txs;
            let txs = self.mempool.lock().unwrap().get_transactions(number_of_tx);
            let block_ref = self.blockchain.get_block_ref(chain_id);

            let merkle_tree_root = MerkleTree::new(&txs).root();
            block_content.transactions = txs;

            // 第一个指针指向自己的先辈区块，第二个指针指向某个区块
            block_content.refs.push(parent_hash);
            block_content.refs.push(block_ref);

            // 第二步，构造区块头
            let mut block_header = Header::new(parent_hash, time_stamp, chain_id, merkle_tree_root);

            block_header.hash = Some(block_header.hash());

            let block = match self.context_update_chan.recv() {
                Ok(sig) => match sig {
                    ContextUpdateSignal::NewProposerBlock => {
                        let content = Content::Proposer(block_content);
                        let block = Block::from_header(block_header, content);
                    }
                    ContextUpdateSignal::NewVoterBlock => {
                        let content = Content::Voter(block_content);
                        let block = Block::from_header(block_header, content);
                    }
                },
                Err(e) => {
                    error!("context_update_chan error:{}", e);
                    return;
                }
            }

            // 启动本地共识
            let result = self.blockchain.concensus(&block);
            match result {
                Ok(_) => {}
                Err(e) => {
                    error!("{:?}", e);
                }
            }
            // 广播区块
            self.server
                .broadcast(Message::NewBlockHashes(vec![block_header.hash.unwrap()]));
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
