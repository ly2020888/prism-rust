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
use prism::blockdb::BlockDatabase;
use prism::config::BlockchainConfig;
use prism::crypto::hash::H256;
use prism::{blockchain::BlockChain, transaction::Account, wallet};
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
    let balancedb = BalanceDatabase::new(cli.balancedb).unwrap();
    let balancedb = Arc::new(balancedb);
    debug!("Initialized balance database");

    // init blockchain database
    // 共识层
    let blockchain = BlockChain::new(cli.blockchain_db, config.clone()).unwrap();
    let blockchain = Arc::new(blockchain);
    debug!("Initialized blockchain database");

    // start thread to update ledger
    let tx_workers = cli.execution_workers;

    // parse p2p server address
    let p2p_addr = cli.p2p.parse::<SocketAddr>().unwrap();

    // create channels between server and worker, worker and miner, miner and worker
    let (msg_tx, msg_rx) = mpsc::channel(100); // TODO: make this buffer adjustable
    let (ctx_tx, ctx_rx) = channel::unbounded();
    let ctx_tx_miner = ctx_tx.clone();

    // start the p2p server
    let (server_ctx, server) = server::new(p2p_addr, msg_tx).unwrap();
    server_ctx.start().await.unwrap();

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
    let (miner_ctx, miner) = miner::new(
        &mempool,
        &blockchain,
        &blockdb,
        ctx_rx,
        &ctx_tx_miner,
        &server,
        config.clone(),
    );
    miner_ctx.start();

    // connect to known peers
    let known_peers: Vec<String> = cli.known_peer;
    connect_known_peers(known_peers, server.clone());

    // fund the given addresses
    let wallets = create_wallets(cli.fund_addr, cli.fund_value);
    load_keypair(wallets, cli.load_key_path);

    // start the transaction generator
    let (txgen_ctx, txgen_control_chan) = TransactionGenerator::new(&wallet, &server, &mempool);
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

fn create_wallets(fund_addrs: Vec<String>, value: u64) -> Vec<Wallet> {
    let mut addrs = vec![];
    for addr in fund_addrs {
        let decoded = match general_purpose::STANDARD.decode(&addr.trim()) {
            Ok(d) => d,
            Err(e) => {
                error!("Error decoding address {}: {}", &addr.trim(), e);
                process::exit(1);
            }
        };
        let addr_bytes: [u8; 32] = (&decoded[0..32]).try_into().unwrap();
        let hash: H256 = addr_bytes.into();
        addrs.push(hash);
    }
    let mut wallets: Vec<Wallet> = vec![];

    for addr in addrs {
        let mut wallet = Wallet::new(Account {
            address: addr,
            balance: value,
        });

        // create wallet key pair if there is none
        if !wallet.has() {
            wallet.generate_keypair();
        }
        wallets.push(wallet);
    }

    wallets
}

fn load_keypair(mut wallet: Vec<Wallet>, wallet_keys: Vec<String>) {
    for (idx, key_path) in wallet_keys.iter().enumerate() {
        let content = match std::fs::read_to_string(&key_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Error loading key pair at {}: {}", &key_path, &e);
                process::exit(1);
            }
        };
        let decoded = match general_purpose::STANDARD.decode(&content.trim()) {
            Ok(d) => d,
            Err(e) => {
                error!("Error decoding key pair at {}: {}", &key_path, &e);
                process::exit(1);
            }
        };
        let keypair = Keypair::from_bytes(&decoded);
        match keypair {
            Ok(a) => {
                wallet[idx].load_keypair(a);
                info!("Loaded key pair for address {}", &wallet[idx].address())
            }
            Err(e) => {
                error!("Error loading key pair into wallet: {}", &e);
                process::exit(1);
            }
        }
    }
}
