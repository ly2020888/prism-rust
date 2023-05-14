#[macro_use]
extern crate clap;

use base64::{
    alphabet,
    engine::{self, general_purpose},
    Engine as _,
};
use clap::Command;
use crossbeam::channel;
use ed25519_dalek::Keypair;
use log::{debug, error, info};
use tokio::sync::mpsc;
// use prism::api::Server as ApiServer;
use prism::blockchain::BlockChain;
use prism::blockdb::BlockDatabase;
use prism::config::BlockchainConfig;
use prism::crypto::hash::H256;
// use prism::experiment::transaction_generator::TransactionGenerator;
// use prism::ledger_manager::LedgerManager;
use crate::clap::Parser;
use prism::balancedb::BalanceDatabase;
use prism::miner;
use prism::miner::memory_pool::MemoryPool;
use prism::network::server;
use prism::network::worker;
use prism::transaction::Address;
// use prism::visualization::Server as VisualizationServer;
use prism::cmd;
use prism::wallet::Wallet;
use rand::rngs::OsRng;
use std::net;
use std::process;
use std::sync::Arc;
use std::thread;
use std::time;
use std::{convert::TryInto, net::SocketAddr};
fn main() {
    // parse command line arguments
    let cli = cmd::Cli::parse();
    // init logger

    // init config struct
    let voter_chains: u16 = cli.voter_chains;
    let tx_throughput: u32 = cli.tx_throughput;
    let block_size: u32 = cli.block_size;
    let comfirm: u32 = cli.confirm_confidence;
    let block_rate = cli.block_rate;
    let config =
        BlockchainConfig::new(voter_chains, block_size, tx_throughput, block_rate, comfirm);
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
    let utxodb = BalanceDatabase::new(cli.balancedb).unwrap();
    let utxodb = Arc::new(utxodb);
    debug!("Initialized balance database");

    // init blockchain database
    // 共识层
    let blockchain = BlockChain::new(cli.blockchain_db, config.clone()).unwrap();
    let blockchain = Arc::new(blockchain);
    debug!("Initialized blockchain database");

    // init wallet database
    // let wallet = Wallet::new();
    // let wallet = Arc::new(wallet);
    // debug!("Initialized wallet");

    // // load wallet keys
    // if let Some(wallet_keys) = matches.values_of("load_key_path") {
    //     for key_path in wallet_keys {
    //         let content = match std::fs::read_to_string(&key_path) {
    //             Ok(c) => c,
    //             Err(e) => {
    //                 error!("Error loading key pair at {}: {}", &key_path, &e);
    //                 process::exit(1);
    //             }
    //         };
    //         let engine = Engine {};
    //         let decoded = match Engine::base64::decode(&content.trim()) {
    //             Ok(d) => d,
    //             Err(e) => {
    //                 error!("Error decoding key pair at {}: {}", &key_path, &e);
    //                 process::exit(1);
    //             }
    //         };
    //         let keypair = Keypair::from_bytes(&decoded).unwrap();
    //         match wallet.load_keypair(keypair) {
    //             Ok(a) => info!("Loaded key pair for address {}", &a),
    //             Err(e) => {
    //                 error!("Error loading key pair into wallet: {}", &e);
    //                 process::exit(1);
    //             }
    //         }
    //     }
    // }

    // start thread to update ledger
    let tx_workers = cli.execution_workers;

    // let ledger_manager = LedgerManager::new(&blockdb, &blockchain, &utxodb, &wallet);
    // ledger_manager.start(tx_buffer, tx_workers);
    // debug!(
    //     "Initialized ledger manager with buffer size {} and {} workers",
    //     tx_buffer, tx_workers
    // );

    // parse p2p server address
    let p2p_addr = cli.p2p.parse::<SocketAddr>().unwrap();

    // // parse api server address
    // let api_addr = matches
    //     .value_of("api_addr")
    //     .unwrap()
    //     .parse::<net::SocketAddr>()
    //     .unwrap_or_else(|e| {
    //         error!("Error parsing API server address: {}", e);
    //         process::exit(1);
    //     });

    // create channels between server and worker, worker and miner, miner and worker
    // let (msg_tx, msg_rx) = piper::chan(100); // TODO: make this buffer adjustable
    // let (ctx_tx, ctx_rx) = channel::unbounded();
    // let ctx_tx_miner = ctx_tx.clone();

    // // start the p2p server
    // let (server_ctx, server) = server::new(p2p_addr, msg_tx).unwrap();
    // server_ctx.start().unwrap();

    // // start the worker
    // let p2p_workers = matches
    //     .value_of("p2p_workers")
    //     .unwrap()
    //     .parse::<usize>()
    //     .unwrap_or_else(|e| {
    //         error!("Error parsing P2P workers: {}", e);
    //         process::exit(1);
    //     });
    // let worker_ctx = worker::new(
    //     p2p_workers,
    //     msg_rx,
    //     &blockchain,
    //     &blockdb,
    //     &utxodb,
    //     &wallet,
    //     &mempool,
    //     ctx_tx,
    //     &server,
    //     config.clone(),
    // );
    // worker_ctx.start();

    // // start the miner
    // let (miner_ctx, miner) = miner::new(
    //     &mempool,
    //     &blockchain,
    //     &blockdb,
    //     ctx_rx,
    //     &ctx_tx_miner,
    //     &server,
    //     config.clone(),
    // );
    // miner_ctx.start();

    // // connect to known peers
    // if let Some(known_peers) = matches.values_of("known_peer") {
    //     let known_peers: Vec<String> = known_peers.map(|x| x.to_owned()).collect();
    //     let server = server.clone();
    //     thread::spawn(move || {
    //         for peer in known_peers {
    //             loop {
    //                 let addr = match peer.parse::<net::SocketAddr>() {
    //                     Ok(x) => x,
    //                     Err(e) => {
    //                         error!("Error parsing peer address {}: {}", &peer, e);
    //                         break;
    //                     }
    //                 };
    //                 match server.connect(addr) {
    //                     Ok(_) => {
    //                         info!("Connected to outgoing peer {}", &addr);
    //                         break;
    //                     }
    //                     Err(e) => {
    //                         error!(
    //                             "Error connecting to peer {}, retrying in one second: {}",
    //                             addr, e
    //                         );
    //                         thread::sleep(time::Duration::from_millis(1000));
    //                         continue;
    //                     }
    //                 }
    //             }
    //         }
    //     });
    // }

    // // fund the given addresses
    // if let Some(fund_addrs) = matches.values_of("init_fund_addr") {
    //     let num_coins = matches
    //         .value_of("init_fund_coins")
    //         .unwrap()
    //         .parse::<usize>()
    //         .unwrap_or_else(|e| {
    //             error!("Error parsing number of initial fund coins: {}", e);
    //             process::exit(1);
    //         });
    //     let coin_value = matches
    //         .value_of("init_fund_value")
    //         .unwrap()
    //         .parse::<u64>()
    //         .unwrap_or_else(|e| {
    //             error!("Error parsing value of initial fund coins: {}", e);
    //             process::exit(1);
    //         });
    //     let mut addrs = vec![];
    //     for addr in fund_addrs {
    //         let decoded = match base64::decode(&addr.trim()) {
    //             Ok(d) => d,
    //             Err(e) => {
    //                 error!("Error decoding address {}: {}", &addr.trim(), e);
    //                 process::exit(1);
    //             }
    //         };
    //         let addr_bytes: [u8; 32] = (&decoded[0..32]).try_into().unwrap();
    //         let hash: H256 = addr_bytes.into();
    //         addrs.push(hash);
    //     }
    //     info!(
    //         "Funding {} addresses with {} initial coins of {}",
    //         addrs.len(),
    //         num_coins,
    //         coin_value
    //     );
    //     prism::experiment::ico(&addrs, &utxodb, &wallet, num_coins, coin_value).unwrap();
    // }

    // // create wallet key pair if there is none
    // if wallet.addresses().unwrap().is_empty() {
    //     wallet.generate_keypair().unwrap();
    // }

    // // start the transaction generator
    // let (txgen_ctx, txgen_control_chan) = TransactionGenerator::new(&wallet, &server, &mempool);
    // txgen_ctx.start();

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
