/// ! 共识层
mod column;
use crate::block::{Block, Content};
use crate::blockdb::BlockDatabase;
use crate::config::*;
use crate::crypto::hash::{Hashable, H256};
use column::*;

//use crate::experiment::performance_counter::PERFORMANCE_COUNTER;
use bincode::{deserialize, serialize};
use rand::Rng;
use rocksdb::{ColumnFamilyDescriptor, MergeOperands, Options, WriteBatch, DB};
use statrs::distribution::{Discrete, Poisson};
use tracing::{debug, info, warn};

use std::collections::{BTreeMap, HashMap, HashSet};

use std::ops::{DerefMut, Range};
use std::sync::{Arc, Mutex};

pub type Result<T> = std::result::Result<T, rocksdb::Error>;

pub struct BlockChain {
    db: DB,
    unconfirmed_proposers: Mutex<HashSet<H256>>,
    proposer_ledger_tip: Mutex<u64>,
    blockdb: Arc<BlockDatabase>,
    voter_ledger_tips: Mutex<Vec<H256>>,
    config: BlockchainConfig,
}

// Functions to edit the blockchain
impl BlockChain {
    /// Open the blockchain database at the given path, and create missing column families.
    /// This function also populates the metadata fields with default values, and those
    /// fields must be initialized later.
    fn open<P: AsRef<std::path::Path>>(
        path: P,
        blockdb: Arc<BlockDatabase>,
        config: BlockchainConfig,
    ) -> Result<Self> {
        let mut cfs: Vec<ColumnFamilyDescriptor> = vec![];
        macro_rules! add_cf {
            ($cf:expr) => {{
                let cf_option = Options::default();
                let cf = ColumnFamilyDescriptor::new($cf, cf_option);
                cfs.push(cf);
            }};
            ($cf:expr, $merge_op:expr) => {{
                let mut cf_option = Options::default();
                cf_option.set_merge_operator("mo", $merge_op, $merge_op);
                let cf = ColumnFamilyDescriptor::new($cf, cf_option);
                cfs.push(cf);
            }};
        }

        // hash | u16(chain number)
        add_cf!(VOTER_NODE_CHAIN_CF);

        // level (u64) to hash of block (proposer hash)
        add_cf!(PROPOSER_TREE_LEVEL_CF);

        // the  parent of a block's hash
        add_cf!(PARENT_NEIGHBOR_CF);

        // the block 's proposer level
        add_cf!(PROPOSER_NODE_LEVEL_CF);

        // all weights on a uncomfirmed proposer block
        // hash | u64
        add_cf!(PROPOSER_VOTE_COUNT_CF, u64_plus_merge);

        // the block (voter and proposer included) 's ref neighbor
        add_cf!(BLOCK_REF_NEIGHBOR_CF, h256_vec_append_merge);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        let mut voter_best: Vec<Mutex<(H256, u64)>> = vec![];
        for _ in 0..config.voter_chains {
            voter_best.push(Mutex::new((H256::default(), 0)));
        }

        let blockchain_db = Self {
            db,
            blockdb,
            unconfirmed_proposers: Mutex::new(HashSet::new()),
            proposer_ledger_tip: Mutex::new(0),
            voter_ledger_tips: Mutex::new(vec![H256::default(); config.voter_chains as usize]),
            config,
        };

        Ok(blockchain_db)
    }

    /// Destroy the existing database at the given path, create a new one, and initialize the content.

    pub fn new<P: AsRef<std::path::Path>>(
        path: P,
        blockdb: Arc<BlockDatabase>,
        config: BlockchainConfig,
    ) -> Result<Self> {
        DB::destroy(&Options::default(), &path)?;
        let db = Self::open(&path, blockdb, config)?;
        // get cf handles
        let voter_node_chain_cf = db.db.cf_handle(VOTER_NODE_CHAIN_CF).unwrap();
        let proposer_tree_level_cf = db.db.cf_handle(PROPOSER_TREE_LEVEL_CF).unwrap();

        let parent_neighbor_cf = db.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let proposer_vote_count_cf = db.db.cf_handle(PROPOSER_VOTE_COUNT_CF).unwrap();
        let block_ref_neighbor_cf = db.db.cf_handle(BLOCK_REF_NEIGHBOR_CF).unwrap();
        let proposer_node_level_cf = db.db.cf_handle(PROPOSER_NODE_LEVEL_CF).unwrap();

        // insert genesis blocks
        let mut wb = WriteBatch::default();

        // voter genesis blocks
        let mut voter_ledger_tips = db.voter_ledger_tips.lock().unwrap();
        for chain_num in 0..db.config.voter_chains {
            wb.put_cf(
                voter_node_chain_cf,
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
                serialize(&(chain_num as u16)).unwrap(),
            );

            wb.put_cf(
                proposer_node_level_cf,
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
                serialize(&(0 as u64)).unwrap(),
            );
            voter_ledger_tips[chain_num as usize] = db.config.genesis_hashes[chain_num as usize];
        }
        drop(voter_ledger_tips);
        db.db.write(wb)?;

        Ok(db)
    }

    pub fn concensus(&self, block: &Block) -> Result<()> {
        // get cf handles
        let voter_node_chain_cf = self.db.cf_handle(VOTER_NODE_CHAIN_CF).unwrap();
        let proposer_tree_level_cf = self.db.cf_handle(PROPOSER_TREE_LEVEL_CF).unwrap();

        let parent_neighbor_cf = self.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let proposer_vote_count_cf = self.db.cf_handle(PROPOSER_VOTE_COUNT_CF).unwrap();
        let block_ref_neighbor_cf = self.db.cf_handle(BLOCK_REF_NEIGHBOR_CF).unwrap();
        let proposer_node_level_cf = self.db.cf_handle(PROPOSER_NODE_LEVEL_CF).unwrap();

        let mut wb = WriteBatch::default();

        macro_rules! get_value {
            ($cf:expr, $key:expr) => {{
                deserialize(
                    &self
                        .db
                        .get_pinned_cf($cf, serialize(&$key).unwrap())?
                        .unwrap(),
                )
                .unwrap()
            }};
        }

        macro_rules! put_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.put_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap());
            }};
        }

        macro_rules! merge_value {
            ($cf:expr, $key:expr, $value:expr) => {{
                wb.merge_cf($cf, serialize(&$key).unwrap(), serialize(&$value).unwrap());
            }};
        }

        // insert parent link
        let block_hash = block.header.hash.unwrap();
        let parent_hash = block.header.parent;
        put_value!(parent_neighbor_cf, block_hash, parent_hash);

        match &block.content {
            Content::Proposer(content) => {
                // 收到一个proposer区块，更新相关的列族，更新累计权重。
                // add ref'ed blocks
                // note that the parent is the first proposer block that we refer
                let mut refed_proposer: Vec<H256> = vec![parent_hash];
                refed_proposer.extend(&content.refs);
                put_value!(block_ref_neighbor_cf, block_hash, refed_proposer);

                // get current block level
                let parent_level: u64 = get_value!(proposer_node_level_cf, parent_hash);
                let self_level = parent_level + 1;
                // set current block level
                put_value!(proposer_node_level_cf, block_hash, self_level as u64);
                put_value!(proposer_tree_level_cf, self_level, block_hash);

                // 更新proposer_ledger_tip，新的proposer区块变为tip
                let mut proposer_tip = self.proposer_ledger_tip.lock().unwrap();
                assert!(*proposer_tip < self_level);
                *proposer_tip = self_level;
                drop(proposer_tip);

                // 保存proposer区块所属的链 voter_node_chain_cf
                put_value!(voter_node_chain_cf, block_hash, content.chain_id);

                // 保存proposer区块父区块的hash parent_neighbor_cf
                put_value!(parent_neighbor_cf, block_hash, parent_hash);

                // 更新累计权重 proposer_vote_count_cf
                let mut unconfirmed_proposers = self.unconfirmed_proposers.lock().unwrap();
                for proposer_block in unconfirmed_proposers.iter() {
                    merge_value!(proposer_vote_count_cf, proposer_block, content.weight);
                }
                // mark outself as unconfirmed proposer
                // This could happen before committing to database, since this block has to become
                // the leader or be referred by a leader. However, both requires the block to be
                // committed to the database. For the same reason, this should not happen after
                // committing to the database (think about the case where this block immediately
                // becomes the leader and is ready to be confirmed).
                unconfirmed_proposers.insert(block_hash);
                drop(unconfirmed_proposers);

                debug!(
                    "Adding proposer block {:.8} at level {}",
                    block_hash, self_level
                );
            }
            Content::Voter(content) => {
                // 收到一个voter区块，更新相关的列族，更新累计权重。
                // add voter parent
                let voter_parent_hash = content.parent;
                put_value!(voter_parent_neighbor_cf, block_hash, voter_parent_hash);
                // get current block level and chain number
                let voter_parent_level: u64 = get_value!(voter_node_level_cf, voter_parent_hash);
                let voter_parent_chain: u16 = get_value!(voter_node_chain_cf, voter_parent_hash);
                let self_level = voter_parent_level + 1;
                let self_chain = voter_parent_chain;
                // set current block level and chain number
                put_value!(voter_node_level_cf, block_hash, self_level as u64);
                put_value!(voter_node_chain_cf, block_hash, self_chain as u16);
                merge_value!(
                    voter_tree_level_count_cf,
                    (self_chain as u16, self_level as u64),
                    1 as u64
                );
                // add voting blocks for the proposer
                // 在这里更新权重
                for proposer_hash in &content.refs {
                    merge_value!(proposer_vote_count_cf, proposer_hash, 1 as u64);
                }
                // add voted blocks and set deepest voted level
                put_value!(vote_neighbor_cf, block_hash, content.refs);
                // set the voted level to be until proposer parent
                let proposer_parent_level: u64 = get_value!(proposer_node_level_cf, parent_hash);
                put_value!(
                    voter_node_voted_level_cf,
                    block_hash,
                    proposer_parent_level as u64
                );

                self.db.write(wb)?;

                debug!(
                    "Adding voter block {:.8} at chain {} level {}",
                    block_hash, self_chain, self_level
                );
            }
        }
        // 检查是存在可以提交的区块
        self.try_comfirm_proposer();

        Ok(())
    }

    pub fn get_parent_hash(&self, chain_id: usize) -> H256 {
        assert!(chain_id as u16 <= self.config.voter_chains);
        let chains_hashs = self.voter_ledger_tips.lock().unwrap();
        chains_hashs[chain_id]
    }

    pub fn get_current_height(&self) -> u64 {
        let height = self.proposer_ledger_tip.lock().unwrap();
        let height = height.to_owned();
        height
    }

    pub fn get_block_ref(&self, chain_id: u16) -> H256 {
        let mut rng = rand::thread_rng();
        assert!(chain_id <= self.config.voter_chains);

        let rand_ref = loop {
            let rand_num = rng.gen_range(0, self.config.voter_chains);
            if rand_num != chain_id {
                break rand_num;
            }
            if self.config.voter_chains == 1 {
                break 0;
            }
        };
        let chains_hashs = self.voter_ledger_tips.lock().unwrap();
        chains_hashs[rand_ref as usize]
    }

    /// Check whether the given transaction block exists in the database.
    pub fn contains_transaction(&self, hash: &H256) -> Result<bool> {
        let parent_neighbor_cf = self.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        match self
            .db
            .get_pinned_cf(parent_neighbor_cf, serialize(&hash).unwrap())?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// Get the chain number of the voter block
    pub fn voter_chain_number(&self, hash: &H256) -> Result<u16> {
        let voter_node_chain_cf = self.db.cf_handle(VOTER_NODE_CHAIN_CF).unwrap();
        let chain: u16 = deserialize(
            &self
                .db
                .get_pinned_cf(voter_node_chain_cf, serialize(&hash).unwrap())?
                .unwrap(),
        )
        .unwrap();
        Ok(chain)
    }

    pub fn try_comfirm_proposer(&self) {
        unimplemented!();
    }
}

fn vote_vec_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &rocksdb::merge_operator::MergeOperands,
) -> Option<Vec<u8>> {
    let mut existing: Vec<(u16, u64)> = match existing_val {
        Some(v) => deserialize(v).unwrap(),
        None => vec![],
    };
    for op in operands {
        // parse the operation as add(true)/remove(false), chain(u16), level(u64)
        let operation: (bool, u16, u64) = deserialize(op).unwrap();
        match operation.0 {
            true => {
                if !existing.contains(&(operation.1, operation.2)) {
                    existing.push((operation.1, operation.2));
                }
            }
            false => {
                match existing.iter().position(|&x| x.0 == operation.1) {
                    Some(p) => existing.swap_remove(p),
                    None => continue, // TODO: potential bug here - what if we delete a nonexisting item
                };
            }
        }
    }
    let result: Vec<u8> = serialize(&existing).unwrap();
    Some(result)
}

fn h256_vec_append_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut existing: Vec<H256> = match existing_val {
        Some(v) => deserialize(v).unwrap(),
        None => vec![],
    };
    for op in operands {
        let new_hash: H256 = deserialize(op).unwrap();
        if !existing.contains(&new_hash) {
            existing.push(new_hash);
        }
    }
    let result: Vec<u8> = serialize(&existing).unwrap();
    Some(result)
}

fn u64_plus_merge(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &rocksdb::merge_operator::MergeOperands,
) -> Option<Vec<u8>> {
    let mut existing: u64 = match existing_val {
        Some(v) => deserialize(v).unwrap(),
        None => 0,
    };
    for op in operands {
        let to_add: u64 = deserialize(op).unwrap();
        existing += to_add;
    }
    let result: Vec<u8> = serialize(&existing).unwrap();
    Some(result)
}
