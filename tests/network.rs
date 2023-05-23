use prism::blockchain::BlockChain;
use prism::miner;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::unbounded_channel;
use tracing::{debug, Level};
use tracing_subscriber::FmtSubscriber;
use {
    prism::balancedb::BalanceDatabase,
    prism::blockdb::BlockDatabase,
    prism::config::BlockchainConfig,
    prism::experiment::transaction_generator,
    prism::miner::memory_pool::MemoryPool,
    prism::miner::ContextUpdateSignal,
    prism::network::{server, worker},
    prism::wallet,
};

fn init() {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

/// 下面的代码是一个测试空间，非正式的测试代码
#[test]
fn test_mine_block() {
    init();
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
        let mempool_size = 1_000_000;
        let mempool = MemoryPool::new(mempool_size);
        let mempool = Arc::new(std::sync::Mutex::new(mempool));
        debug!("Initialized mempool, maximum size set to {}", mempool_size);

        // start the p2p server
        // parse p2p server address
        let p2p_addr = "127.0.0.1:7000".parse::<SocketAddr>().unwrap();

        // create channels between server and worker, worker and miner, miner and worker
        let (msg_tx, _msg_rx) = unbounded_channel();

        let (server_ctx, server) = server::new(p2p_addr, msg_tx).unwrap();
        server_ctx.start().unwrap();

        let known_peers = vec!["127.0.0.1:7001".to_string()];
        server::connect_known_peers(known_peers, server.clone());

        let wallets = wallet::util::load_wallets(10, 1_000_000);

        let (txgen_ctx, _txgen_control_chan) =
            transaction_generator::TransactionGenerator::new(wallets, &server, &mempool);
        txgen_ctx.start();

        let (test_channel_tx, test_channel_rc) = unbounded_channel::<ContextUpdateSignal>();

        let test_config = BlockchainConfig::new(2, 256, 1000, 10, 1, 10);

        // init block database
        let blockdb = BlockDatabase::new("./rocksdb/blockcdb", test_config.clone()).unwrap();
        let blockdb = Arc::new(blockdb);
        debug!("Initialized block database");

        // init balance database
        let balancedb = BalanceDatabase::new("./rocksdb/balancedb").unwrap();
        let balancedb = Arc::new(balancedb);
        debug!("Initialized balance database");

        // start the miner
        // 注：miner无挖矿动作，仅仅是按照固定速率打包区块
        // TODO:添加挖矿逻辑

        // init blockchain database
        // 共识层
        let blockchain = BlockChain::new(
            "./rocksdb/blockchain",
            blockdb.clone(),
            test_config.clone(),
            balancedb.clone(),
        )
        .unwrap();

        let blockchain = Arc::new(blockchain);
        debug!("Initialized blockchain database");

        let (miner_ctx, miner) = miner::new(
            &mempool,
            &blockchain,
            &blockdb,
            &balancedb,
            test_channel_rc,
            &test_channel_tx,
            &server,
            test_config.clone(),
        );

        miner.start(10, false);
        miner_ctx.start().await;

        test_channel_tx
            .clone()
            .send(ContextUpdateSignal::NewProposerBlock)
            .unwrap();

        let p2p_workers = 1;
        let worker_ctx = worker::new(
            p2p_workers,
            _msg_rx,
            &blockchain,
            &blockdb,
            &balancedb,
            &mempool,
            test_channel_tx,
            &server,
            test_config,
        );
        worker_ctx.start();

        loop {
            std::thread::park();
        }
    });
}
