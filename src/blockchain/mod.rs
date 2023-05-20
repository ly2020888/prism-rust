/// ! 共识层
mod column;
use crate::block::{content, header, Block, Content};
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
        // only proposer block
        add_cf!(PROPOSER_TREE_LEVEL_CF);

        // the  parent of a block's hash
        add_cf!(PARENT_NEIGHBOR_CF);

        // the block 's proposer level
        add_cf!(PROPOSER_NODE_LEVEL_CF);

        // record werther the block is proposer block
        add_cf!(PROPOSER_CF);

        // all weights on a uncomfirmed proposer block
        // hash | u64
        add_cf!(PROPOSER_VOTE_COUNT_CF, u64_plus_merge);

        // the block (voter and proposer included) 's ref neighbor
        add_cf!(BLOCK_REF_NEIGHBOR_CF, h256_vec_append_merge);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(&opts, path, cfs)?;

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
            // 创世区块的父区块就是自己
            wb.put_cf(
                parent_neighbor_cf,
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
            );
            // 创世区块的初始累计权重

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
        let proposer_cf = self.db.cf_handle(PROPOSER_CF).unwrap();

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

                // set current block level
                put_value!(proposer_node_level_cf, block_hash, content.height);
                put_value!(proposer_tree_level_cf, content.height, block_hash);

                // 更新proposer_ledger_tip，新的proposer区块变为tip
                let mut proposer_tip = self.proposer_ledger_tip.lock().unwrap();
                assert!(*proposer_tip < content.height);
                *proposer_tip = content.height;
                drop(proposer_tip);

                // 保存proposer区块所属的链 voter_node_chain_cf
                put_value!(voter_node_chain_cf, block_hash, content.chain_id);

                // 保存proposer区块父区块的hash parent_neighbor_cf
                put_value!(parent_neighbor_cf, block_hash, parent_hash);

                // 更新先前累计权重 proposer_vote_count_cf
                let mut unconfirmed_proposers = self.unconfirmed_proposers.lock().unwrap();
                for proposer_block in unconfirmed_proposers.iter() {
                    merge_value!(
                        proposer_vote_count_cf,
                        proposer_block,
                        content.weight as u64
                    );
                }

                // 插入自己的初始累计权重
                put_value!(proposer_vote_count_cf, block_hash, content.weight as u64);

                // mark outself as unconfirmed proposer
                // This could happen before committing to database, since this block has to become
                // the leader or be referred by a leader. However, both requires the block to be
                // committed to the database. For the same reason, this should not happen after
                // committing to the database (think about the case where this block immediately
                // becomes the leader and is ready to be confirmed).
                unconfirmed_proposers.insert(block_hash);
                drop(unconfirmed_proposers);

                // 更新链上最末端区块
                self.update_tips(content.chain_id, &block_hash);

                // 记录收到的proposer区块
                put_value!(proposer_cf, block_hash, content.height);
                self.db.write(wb)?;

                debug!(
                    "Adding proposer block {:.8} at level {}",
                    block_hash, content.height
                );
            }
            Content::Voter(content) => {
                // 收到一个voter区块，更新相关的列族，更新累计权重。

                // add ref'ed blocks
                // note that the parent is the first proposer block that we refer
                let mut refed_proposer: Vec<H256> = vec![parent_hash];
                refed_proposer.extend(&content.refs);
                put_value!(block_ref_neighbor_cf, block_hash, refed_proposer);

                // 添加普通区块的领导者区块等级
                put_value!(proposer_node_level_cf, block_hash, content.height);
                // add voter parent
                let voter_parent_hash = content.parent;
                put_value!(parent_neighbor_cf, block_hash, voter_parent_hash);
                // get current block level and chain number
                let voter_parent_chain: u16 = get_value!(voter_node_chain_cf, voter_parent_hash);
                let self_chain = voter_parent_chain;

                put_value!(voter_node_chain_cf, block_hash, self_chain);

                // add voting blocks for the proposer
                // 在这里更新权重
                let unconfirmed_proposers = self.unconfirmed_proposers.lock().unwrap();
                for proposer_hash in unconfirmed_proposers.iter() {
                    let proposer_height: u64 = get_value!(proposer_node_level_cf, proposer_hash);
                    if proposer_height <= content.height {
                        merge_value!(proposer_vote_count_cf, proposer_hash, content.weight as u64);
                    }
                }
                drop(unconfirmed_proposers);

                // 更新链上最末端区块
                self.update_tips(content.chain_id, &block_hash);

                self.db.write(wb)?;
                debug!(
                    "Adding voter block {:.8} at chain {} level {}",
                    block_hash, self_chain, content.height
                );
            }
        }

        // 检查是存在可以提交的区块
        self.try_confirm_proposer()?;

        Ok(())
    }

    pub fn get_parent_hash(&self, chain_id: usize) -> H256 {
        assert!(chain_id as u16 <= self.config.voter_chains);
        let chains_hashs = self.voter_ledger_tips.lock().unwrap();
        chains_hashs[chain_id]
    }

    pub fn get_proposer_height(&self) -> u64 {
        let height = self.proposer_ledger_tip.lock().unwrap();
        *height + 1
    }

    pub fn make_proposer_valid(&self, content: &mut content::Content, parent: &H256) -> Result<()> {
        if content.height == 1 {
            return Ok(());
        }
        let (res, _) = self.is_proposer_block(content.parent)?;
        if res {
            return Ok(());
        }
        for hash in &content.refs {
            let (res, height) = self.is_proposer_block(*hash)?;
            if res && height + 1 == content.height {
                return Ok(());
            }
        }
        // 无合法的领导者区块引用，现在手动更新
        let now_proposer_height = self.proposer_ledger_tip.lock().unwrap();
        let proposer_tree_level_cf = self.db.cf_handle(PROPOSER_TREE_LEVEL_CF).unwrap();
        let hash: H256 = deserialize(
            &self
                .db
                .get_pinned_cf(
                    proposer_tree_level_cf,
                    serialize(&*now_proposer_height).unwrap(),
                )?
                .unwrap(),
        )
        .unwrap();
        let refs = vec![parent.clone(), hash];
        content.refs = refs;
        Ok(())
    }

    pub fn get_voter_height(&self, refs: Vec<H256>) -> u64 {
        // 普通区块的proposer_height必须进行计算
        let proposer_node_level_cf = self.db.cf_handle(PROPOSER_NODE_LEVEL_CF).unwrap();
        let mut max: u64 = 0;
        for r in refs {
            let height: u64 = deserialize(
                &self
                    .db
                    .get_pinned_cf(proposer_node_level_cf, serialize(&r).unwrap())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            if height >= max {
                max = height;
            }
        }
        max
    }

    pub fn is_proposer_block(&self, hash: H256) -> Result<(bool, u64)> {
        let proposer_cf = self.db.cf_handle(PROPOSER_CF).unwrap();
        match self
            .db
            .get_pinned_cf(proposer_cf, serialize(&hash).unwrap())?
        {
            Some(height) => {
                let height = deserialize(&height).unwrap();
                Ok((true, height))
            }
            None => Ok((false, 0)),
        }
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
    fn update_tips(&self, chain_id: u16, hash: &H256) {
        // 更新tips
        let mut chains_hashs = self.voter_ledger_tips.lock().unwrap();
        (*chains_hashs)[chain_id as usize] = hash.clone();
        drop(chains_hashs);
    }

    fn try_confirm_proposer(&self) -> Result<()> {
        let proposer_vote_count_cf = self.db.cf_handle(PROPOSER_VOTE_COUNT_CF).unwrap();
        let uncomfirmed_blocks = self.unconfirmed_proposers.lock().unwrap();

        for proposer_block in uncomfirmed_blocks.iter() {
            let weight: u64 = deserialize(
                &self
                    .db
                    .get_pinned_cf(proposer_vote_count_cf, serialize(proposer_block).unwrap())?
                    .unwrap(),
            )
            .unwrap();
            debug!("未提交proposer区块{:.8}, weight:{}", proposer_block, weight);
        }
        // unimplemented!();
        Ok(())
    }

    // 这个函数负责从rocksdb中读取未到达阈值的领导者区块
    fn _load_unconfirmed(&self) {
        unimplemented!();
    }
}

fn _vote_vec_merge(
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
