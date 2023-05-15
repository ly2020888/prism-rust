use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Sets the IP address and the port of the P2P server
    #[arg(short, long, default_value_t = String::from("127.0.0.1:6000"))]
    pub p2p: String,

    /// Sets the peers to connect to at start
    #[arg(long)]
    pub known_peer: Vec<String>,

    /// Sets the path to the block database
    #[arg(long,default_value_t = String::from("/tmp/prism-blocks.rocksdb"))]
    pub block_db: String,

    /// Sets the path to the balance database
    #[arg(long,default_value_t = String::from("/tmp/prism-balancedb.rocksdb"))]
    pub balancedb: String,

    /// Sets the path to the blockchain database
    #[arg(long,default_value_t = String::from("/tmp/prism-blockchain_db.rocksdb"))]
    pub blockchain_db: String,

    /// Endows the given address an initial fund in the genesis block
    #[arg(long)]
    pub fund_addr: Vec<String>,

    /// Sets the value of each initial coin
    #[arg(long, default_value_t = 100)]
    pub fund_value: u64,

    /// Loads a key pair into the wallet from the given path
    #[arg(short, long)]
    pub load_key_path: Vec<String>,

    /// Sets the maximum number of transactions for the memory pool
    #[arg(short, long)]
    pub mempool_size: u64,

    /// Sets the number of voter chains
    #[arg(long)]
    pub voter_chains: u16,

    /// Sets the number of worker threads for transaction execution
    #[arg(short, long, default_value_t = 8)]
    pub execution_workers: u64,

    /// Sets the number of worker threads for transaction execution
    #[arg(long, default_value_t = 16)]
    pub p2p_workers: u64,

    /// Sets the  chain mining rate
    #[arg(long, default_value_t = 16)]
    pub mining_rate: u64,

    /// lets -log(epsilon) for confirmation
    #[arg(long, default_value_t = 16)]
    pub confirm_confidence: u32,

    /// Set simulate the transaction rate, 0 indicates that no limited
    #[arg(long, default_value_t = 0)]
    pub tx_throughput: u32,

    /// Set simulate the block size, 0 indicates that no more than 16KB
    #[arg(long, default_value_t = 0)]
    pub block_size: u32,

    /// Set simulate the block rate,
    #[arg(long)]
    pub block_rate: f32,
}
