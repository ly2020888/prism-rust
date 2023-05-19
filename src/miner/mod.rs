pub mod memory_pool;

use crate::block::content;
use crate::block::header::Header;
use crate::block::{Block, Content};
use crate::blockchain::BlockChain;
use crate::config::*;
use crate::crypto::hash::Hashable;
use crate::crypto::merkle::MerkleTree;
use crate::network::message::Message;
// use crate::experiment::performance_counter::PERFORMANCE_COUNTER;
// use crate::handler::new_validated_block;
use crate::network::server::Handle as ServerHandle;

use tracing::{debug, error, info};

use memory_pool::MemoryPool;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
enum ControlSignal {
    Start(u64, bool), // the number controls the lambda of interval between block generation
    Step,
    Exit,
}

#[derive(Ord, Eq, PartialOrd, PartialEq, Debug)]
pub enum ContextUpdateSignal {
    // New proposer block comes, we need to update all contents' parent
    NewProposerBlock,
    // New voter block comes, we need to update that voter chain
    NewVoterBlock,
}

#[derive(Debug)]
enum OperatingState {
    Paused,
    Run(u64, bool),
    Step,
    ShutDown,
}

pub struct Context {
    blockchain: Arc<BlockChain>,
    mempool: Arc<Mutex<MemoryPool>>,
    /// Channel for receiving control signal
    control_chan: UnboundedReceiver<ControlSignal>,
    /// Channel for notifying miner of new content
    context_update_chan: UnboundedReceiver<ContextUpdateSignal>,
    _context_update_tx: UnboundedSender<ContextUpdateSignal>,
    operating_state: OperatingState,
    server: ServerHandle,
    config: BlockchainConfig,
}

#[derive(Clone)]
pub struct Handle {
    // Channel for sending signal to the miner thread
    control_chan: UnboundedSender<ControlSignal>,
}

pub fn new(
    mempool: &Arc<Mutex<MemoryPool>>,
    blockchain: &Arc<BlockChain>,
    ctx_update_source: UnboundedReceiver<ContextUpdateSignal>,
    ctx_update_tx: &UnboundedSender<ContextUpdateSignal>,
    server: &ServerHandle,
    config: BlockchainConfig,
) -> (Context, Handle) {
    let (signal_chan_sender, signal_chan_receiver) = unbounded_channel();

    let ctx = Context {
        blockchain: Arc::clone(blockchain),
        mempool: Arc::clone(mempool),
        control_chan: signal_chan_receiver,
        context_update_chan: ctx_update_source,
        _context_update_tx: ctx_update_tx.clone(),
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
    pub async fn start(mut self) {
        tokio::spawn(async move {
            self.miner_loop().await;
        });

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

    async fn miner_loop(&mut self) {
        // main mining loop
        loop {
            // let block_start = time::Instant::now();

            // check and react to control signals
            match self.operating_state {
                OperatingState::Paused => {
                    let signal = self.control_chan.recv().await.unwrap();
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

            let block = match self.context_update_chan.try_recv() {
                Ok(sig) => match sig {
                    ContextUpdateSignal::NewProposerBlock => {
                        let content = Content::Proposer(block_content);
                        Block::from_header(block_header, content)
                    }
                    ContextUpdateSignal::NewVoterBlock => {
                        let content = Content::Voter(block_content);
                        Block::from_header(block_header, content)
                    }
                },
                Err(e) => {
                    error!(
                        "context_update_chan 错误, 生产区块线程收到关闭信号，已退出: {}",
                        e
                    );
                    return;
                }
            };
            debug!("生产区块：{:?}", block);
            // 启动本地共识
            // let result = self.blockchain.concensus(&block);
            // match result {
            //     Ok(_) => {}
            //     Err(e) => {
            //         error!("{:?}", e);
            //     }
            // }

            // 广播区块
            self.server
                .broadcast(Message::NewBlockHashes(vec![block_header.hash.unwrap()]));
        }
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
mod tests {
    use super::*;
    use crate::{
        blockdb::BlockDatabase, experiment::transaction_generator, miner::memory_pool::MemoryPool,
        network::server, wallet,
    };
    use std::{net::SocketAddr, sync::Arc};
    use tokio::sync::mpsc;
    use tracing::{debug, Level};
    use tracing_subscriber::FmtSubscriber;

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
    fn test_mine_block() {
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
        txgen_ctx.start();

        let (test_channel_tx, test_channel_rc) = unbounded_channel::<ContextUpdateSignal>();

        let test_config = BlockchainConfig::new(1, 256, 1000, 100.0, 10, 0, 10);

        // init block database
        let blockdb = BlockDatabase::new("./rocksdb/blockcdb", test_config.clone()).unwrap();
        let blockdb = Arc::new(blockdb);
        debug!("Initialized block database");

        // start the miner
        // 注：miner无挖矿动作，仅仅是按照固定速率打包区块
        // TODO:添加挖矿逻辑

        // init blockchain database
        // 共识层
        let blockchain =
            BlockChain::new("./rocksdb/blockchain", blockdb, test_config.clone()).unwrap();
        let blockchain = Arc::new(blockchain);
        debug!("Initialized blockchain database");

        let (miner_ctx, miner) = super::new(
            &mempool,
            &blockchain,
            test_channel_rc,
            &test_channel_tx,
            &server,
            test_config,
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        miner.start(10, false);
        rt.block_on(async move {
            test_channel_tx
                .clone()
                .send(ContextUpdateSignal::NewProposerBlock)
                .unwrap();
            test_channel_tx
                .clone()
                .send(ContextUpdateSignal::NewProposerBlock)
                .unwrap();

            miner_ctx.start().await;
        });
        //
    }
}
