extern crate clap;

use tokio::sync::mpsc;
use tokio::sync::mpsc::unbounded_channel;
// use prism::api::Server as ApiServer;
use prism::config::BlockchainConfig;
use prism::{blockchain::BlockChain, wallet};
use prism::{blockdb::BlockDatabase, experiment::transaction_generator::TransactionGenerator};
// use prism::experiment::transaction_generator::TransactionGenerator;
// use prism::ledger_manager::LedgerManager;
use crate::clap::Parser;
use prism::balancedb::BalanceDatabase;
use prism::miner;
use prism::miner::memory_pool::MemoryPool;
use prism::network::server;
use prism::network::worker;
// use prism::visualization::Server as VisualizationServer;
use prism::cmd;
use std::net;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time;

use tracing::{debug, error, info};

#[tokio::main]
async fn main() {
    // parse command line arguments
    let cli = cmd::Cli::parse();
    // init logger

    // init config struct
    let voter_chains: u16 = cli.voter_chains;
    let tx_throughput: u32 = cli.tx_throughput;
    let block_size: u32 = cli.block_size;
    let comfirm: u32 = cli.confirm_confidence;
    let block_rate = cli.block_rate;
    let p2p_id = cli.p2p_id;
    let block_weight = cli.block_weight;
    let config = BlockchainConfig::new(
        voter_chains,
        block_size,
        tx_throughput,
        block_rate,
        comfirm,
        p2p_id,
        block_weight,
    );
    info!("block rate set to {} blks/s", config.block_rate);

    info!("Transaction rate set to {} tps", config.tx_throughput);

    // init mempool
    let mempool_size = cli.mempool_size;
    let mempool = MemoryPool::new(mempool_size);
    let mempool = Arc::new(std::sync::Mutex::new(mempool));
    debug!("Initialized mempool, maximum size set to {}", mempool_size);

    // init block database
    let blockdb = BlockDatabase::new(cli.block_db, config.clone()).unwrap();
    let blockdb = Arc::new(blockdb);
    debug!("Initialized block database");

    // init balance database
    let balancedb = BalanceDatabase::new(cli.balancedb).unwrap();
    let balancedb = Arc::new(balancedb);
    debug!("Initialized balance database");

    // init blockchain database
    // 共识层
    let blockchain = BlockChain::new(cli.blockchain_db, blockdb.clone(), config.clone()).unwrap();
    let blockchain = Arc::new(blockchain);
    debug!("Initialized blockchain database");

    // start thread to update ledger
    let tx_workers = cli.execution_workers;

    // parse p2p server address
    let p2p_addr = cli.p2p.parse::<SocketAddr>().unwrap();

    // create channels between server and worker, worker and miner, miner and worker
    let (msg_tx, msg_rx) = mpsc::channel(100); // TODO: make this buffer adjustable
    let (ctx_tx, ctx_rx) = unbounded_channel();
    let ctx_tx_miner = ctx_tx.clone();

    // start the p2p server
    let (server_ctx, server) = server::new(p2p_addr, msg_tx).unwrap();
    server_ctx.start().unwrap();

    // start the worker
    let p2p_workers = cli.p2p_workers;
    let worker_ctx = worker::new(
        p2p_workers,
        msg_rx,
        &blockchain,
        &blockdb,
        &balancedb,
        &mempool,
        ctx_tx,
        &server,
        config.clone(),
    );
    worker_ctx.start();

    // start the miner
    // 注：miner无挖矿动作，仅仅是按照固定速率打包区块
    // TODO:添加挖矿逻辑
    let (miner_ctx, _miner) = miner::new(
        &mempool,
        &blockchain,
        &blockdb,
        ctx_rx,
        &ctx_tx_miner,
        &server,
        config.clone(),
    );
    miner_ctx.start().await;

    // connect to known peers
    let known_peers: Vec<String> = cli.known_peer;
    connect_known_peers(known_peers, server.clone());

    // fund the given addresses
    let wallets = wallet::util::load_wallets(cli.fund_value);

    // start the transaction generator
    // txgen_control_chan 交易控制通道，暂时用不到
    let (txgen_ctx, _txgen_control_chan) = TransactionGenerator::new(wallets, &server, &mempool);
    txgen_ctx.start();

    // // start the API server
    // ApiServer::start(
    //     api_addr,
    //     &wallet,
    //     &blockchain,
    //     &utxodb,
    //     &server,
    //     &miner,
    //     &mempool,
    //     txgen_control_chan,
    // );

    // // start the visualization server
    // if let Some(addr) = matches.value_of("visualization") {
    //     let addr = addr.parse::<net::SocketAddr>().unwrap_or_else(|e| {
    //         error!("Error parsing visualization server socket address: {}", e);
    //         process::exit(1);
    //     });
    //     info!("Starting visualization server at {}", &addr);
    //     VisualizationServer::start(addr, &blockchain, &blockdb, &utxodb);
    // }

    loop {
        std::thread::park();
    }
}

fn connect_known_peers(known_peers: Vec<String>, server: server::Handle) {
    thread::spawn(move || {
        for peer in known_peers {
            loop {
                let addr = match peer.parse::<net::SocketAddr>() {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Error parsing peer address {}: {}", &peer, e);
                        break;
                    }
                };
                match server.connect(addr) {
                    Ok(_) => {
                        info!("Connected to outgoing peer {}", &addr);
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Error connecting to peer {}, retrying in one second: {}",
                            addr, e
                        );
                        thread::sleep(time::Duration::from_millis(1000));
                        continue;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {}
