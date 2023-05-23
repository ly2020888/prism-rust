extern crate clap;

use tokio::sync::mpsc::unbounded_channel;
// use prism::api::Server as ApiServer;
use prism::config::BlockchainConfig;
use prism::{blockchain::BlockChain, wallet};
use prism::{blockdb::BlockDatabase, experiment::transaction_generator::TransactionGenerator};
use tracing_subscriber::FmtSubscriber;
// use prism::experiment::transaction_generator::TransactionGenerator;
// use prism::ledger_manager::LedgerManager;
use crate::clap::Parser;
use prism::balancedb::BalanceDatabase;
use prism::miner;
use prism::miner::memory_pool::MemoryPool;
use prism::network::server;
use prism::network::worker;
// use prism::visualization::Server as VisualizationServer;
use prism::cmd::{self, Cli};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, Level};

#[tokio::main]
async fn main() {
    // parse command line arguments
    let cli = cmd::Cli::parse();
    init(&cli);

    // init config struct
    let voter_chains: u16 = cli.voter_chains;
    let tx_throughput: u32 = cli.tx_throughput;
    let block_size: u32 = cli.block_size;
    let comfirm: u32 = cli.confirm_confidence;
    let p2p_id = cli.p2p_id;
    let block_weight = cli.block_weight;
    let config = BlockchainConfig::new(
        voter_chains,
        block_size,
        tx_throughput,
        comfirm,
        p2p_id,
        block_weight,
    );

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
    let blockchain = BlockChain::new(
        cli.blockchain_db,
        blockdb.clone(),
        config.clone(),
        balancedb.clone(),
    )
    .unwrap();
    let blockchain = Arc::new(blockchain);
    debug!("Initialized blockchain database");

    // parse p2p server address
    let p2p_addr = cli.p2p.parse::<SocketAddr>().unwrap();

    // create channels between server and worker, worker and miner, miner and worker
    let (msg_tx, msg_rx) = unbounded_channel(); // TODO: make this buffer adjustable
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
    let (miner_ctx, _miner) = miner::new(
        &mempool,
        &blockchain,
        &blockdb,
        &balancedb,
        ctx_rx,
        &ctx_tx_miner,
        &server,
        config.clone(),
    );
    miner_ctx.start().await;

    // connect to known peers
    let known_peers: Vec<String> = cli.known_peer;
    server::connect_known_peers(known_peers, server.clone());

    // fund the given addresses
    let wallets = wallet::util::load_wallets(cli.numbers_addr, cli.fund_value);

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

fn init(cli: &Cli) {
    if Path::new(&cli.block_db).exists() {
        fs::remove_dir_all(&cli.block_db).unwrap();
    }
    fs::create_dir(cli.block_db.clone()).unwrap();

    if Path::new(&cli.balancedb).exists() {
        fs::remove_dir_all(&cli.balancedb).unwrap();
    }
    fs::create_dir(cli.balancedb.clone()).unwrap();

    if Path::new(&cli.blockchain_db).exists() {
        fs::remove_dir_all(&cli.blockchain_db).unwrap();
    }
    fs::create_dir(cli.blockchain_db.clone()).unwrap();

    if Path::new("./wallets").exists() {
        fs::remove_dir_all("./wallets").unwrap();
    }
    fs::create_dir("./wallets").unwrap();
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[cfg(test)]
mod tests {}
