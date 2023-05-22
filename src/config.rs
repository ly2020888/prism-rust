use crate::crypto::hash::H256;
use bigint::uint::U256;

// const PROPOSER_TX_REF_HEADROOM: f32 = 10.0;
// const SORTITION_PRECISION: u64 = std::u64::MAX;
// const DECONFIRM_HEADROOM: f32 = 1.05;

const AVG_TX_SIZE: u32 = 168; // average size of a transaction (in Bytes)

// Chain IDs
pub const PROPOSER_INDEX: u16 = 0;
pub const TRANSACTION_INDEX: u16 = 1;
pub const FIRST_VOTER_INDEX: u16 = 2;

#[derive(Clone)]
pub struct BlockchainConfig {
    /// Number of voter chains.
    pub voter_chains: u16,

    /// Maximum size of a  block in terms of transactions.
    pub tx_txs: u32,

    /// 预估的区块大小
    pub block_size: u32,

    /// 创世区块哈希
    pub genesis_hashes: Vec<H256>,

    /// 总交易产生速度
    pub tx_throughput: u32,

    /// 出块速度控制
    pub block_rate: f32,

    /// 提交权重门限
    pub confirm: u32,

    /// 启动的节点的编号，范围应当是[0,voter_chains)
    pub node_id: u16,

    /// 单个区块的权重
    pub block_weight: u32,
}

impl BlockchainConfig {
    pub fn new(
        voter_chains: u16,
        block_size: u32,
        tx_throughput: u32,
        confirm: u32,
        node_id: u16,
        block_weight: u32,
    ) -> Self {
        let tx_txs = block_size / AVG_TX_SIZE;

        let genesis_hashes: Vec<H256> = {
            let mut v: Vec<H256> = vec![];
            for chain_num in 0..voter_chains {
                let mut raw_hash: [u8; 32] = [0; 32];
                let bytes = (chain_num + FIRST_VOTER_INDEX).to_be_bytes();
                raw_hash[30] = bytes[0];
                raw_hash[31] = bytes[1];
                v.push(raw_hash.into());
            }
            v
        };
        let block_rate: f32 = {
            let tx_thruput: f32 = tx_throughput as f32;
            let tx_txs: f32 = tx_txs as f32;
            tx_thruput / tx_txs
        };

        if node_id >= voter_chains {
            panic!("请确保节点的编号范围位于总结点数量内，请保证节点的编号不会重复");
        }

        Self {
            voter_chains,
            tx_txs,
            block_size,
            genesis_hashes,
            tx_throughput,
            block_rate,
            confirm,
            node_id,
            block_weight,
        }
    }
}
