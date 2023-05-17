/// ! 共识层
use crate::block::{Block, Content};
use crate::config::*;
use crate::crypto::hash::{Hashable, H256};

//use crate::experiment::performance_counter::PERFORMANCE_COUNTER;
use bincode::{deserialize, serialize};
use log::{debug, info, warn};
use rand::Rng;
use rocksdb::{ColumnFamilyDescriptor, MergeOperands, Options, WriteBatch, DB};
use statrs::distribution::{Discrete, Poisson};

use std::collections::{BTreeMap, HashMap, HashSet};

use std::ops::{DerefMut, Range};
use std::sync::Mutex;

// Column family names for node/chain metadata
const PROPOSER_NODE_LEVEL_CF: &str = "PROPOSER_NODE_LEVEL"; // hash to node level (u64)
const VOTER_NODE_LEVEL_CF: &str = "VOTER_NODE_LEVEL"; // hash to node level (u64)
const VOTER_NODE_CHAIN_CF: &str = "VOTER_NODE_CHAIN"; // hash to chain number (u16)
const VOTER_TREE_LEVEL_COUNT_CF: &str = "VOTER_TREE_LEVEL_COUNT_CF"; // chain number and level (u16, u64) to number of blocks (u64)
const PROPOSER_TREE_LEVEL_CF: &str = "PROPOSER_TREE_LEVEL"; // level (u64) to hashes of blocks (Vec<hash>)
const VOTER_NODE_VOTED_LEVEL_CF: &str = "VOTER_NODE_VOTED_LEVEL"; // hash to max. voted level (u64)
const PROPOSER_NODE_VOTE_CF: &str = "PROPOSER_NODE_VOTE"; // hash to level and chain number of main chain votes (Vec<u16, u64>)
const PROPOSER_LEADER_SEQUENCE_CF: &str = "PROPOSER_LEADER_SEQUENCE"; // level (u64) to hash of leader block.
const PROPOSER_LEDGER_ORDER_CF: &str = "PROPOSER_LEDGER_ORDER"; // level (u64) to the list of proposer blocks confirmed
                                                                // by this level, including the leader itself. The list
                                                                // is in the order that those blocks should live in the ledger.
const PROPOSER_VOTE_COUNT_CF: &str = "PROPOSER_VOTE_COUNT"; // number of all votes on a block

// Column family names for graph neighbors
const PARENT_NEIGHBOR_CF: &str = "GRAPH_PARENT_NEIGHBOR"; // the proposer parent of a block
const VOTE_NEIGHBOR_CF: &str = "GRAPH_VOTE_NEIGHBOR"; // neighbors associated by a vote
const VOTER_PARENT_NEIGHBOR_CF: &str = "GRAPH_VOTER_PARENT_NEIGHBOR"; // the voter parent of a block
const TRANSACTION_REF_NEIGHBOR_CF: &str = "GRAPH_TRANSACTION_REF_NEIGHBOR";
const PROPOSER_REF_NEIGHBOR_CF: &str = "GRAPH_PROPOSER_REF_NEIGHBOR";

pub type Result<T> = std::result::Result<T, rocksdb::Error>;

// cf_handle is a lightweight operation, it takes 44000 micro seconds to get 100000 cf handles

pub struct BlockChain {
    db: DB,
    unconfirmed_proposers: Mutex<HashSet<H256>>,
    unreferred_proposers: Mutex<HashSet<H256>>,
    proposer_ledger_tip: Mutex<u64>,
    voter_ledger_tips: Mutex<Vec<H256>>,
    config: BlockchainConfig,
}

// Functions to edit the blockchain
impl BlockChain {
    /// Open the blockchain database at the given path, and create missing column families.
    /// This function also populates the metadata fields with default values, and those
    /// fields must be initialized later.
    fn open<P: AsRef<std::path::Path>>(path: P, config: BlockchainConfig) -> Result<Self> {
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
        add_cf!(PROPOSER_NODE_LEVEL_CF);
        add_cf!(VOTER_NODE_LEVEL_CF);
        add_cf!(VOTER_NODE_CHAIN_CF);
        add_cf!(VOTER_NODE_VOTED_LEVEL_CF);
        add_cf!(PROPOSER_LEADER_SEQUENCE_CF);
        add_cf!(PROPOSER_LEDGER_ORDER_CF);
        add_cf!(PROPOSER_TREE_LEVEL_CF, h256_vec_append_merge);
        add_cf!(PROPOSER_NODE_VOTE_CF, vote_vec_merge);
        add_cf!(PARENT_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(VOTE_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(VOTER_TREE_LEVEL_COUNT_CF, u64_plus_merge);
        add_cf!(PROPOSER_VOTE_COUNT_CF, u64_plus_merge);
        add_cf!(VOTER_PARENT_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(TRANSACTION_REF_NEIGHBOR_CF, h256_vec_append_merge);
        add_cf!(PROPOSER_REF_NEIGHBOR_CF, h256_vec_append_merge);

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
            unconfirmed_proposers: Mutex::new(HashSet::new()),
            unreferred_proposers: Mutex::new(HashSet::new()),
            proposer_ledger_tip: Mutex::new(0),
            voter_ledger_tips: Mutex::new(vec![H256::default(); config.voter_chains as usize]),
            config,
        };

        Ok(blockchain_db)
    }

    /// Destroy the existing database at the given path, create a new one, and initialize the content.
    pub fn new<P: AsRef<std::path::Path>>(path: P, config: BlockchainConfig) -> Result<Self> {
        DB::destroy(&Options::default(), &path)?;
        let db = Self::open(&path, config)?;
        // get cf handles
        let proposer_node_level_cf = db.db.cf_handle(PROPOSER_NODE_LEVEL_CF).unwrap();
        let voter_node_level_cf = db.db.cf_handle(VOTER_NODE_LEVEL_CF).unwrap();
        let voter_node_chain_cf = db.db.cf_handle(VOTER_NODE_CHAIN_CF).unwrap();
        let voter_node_voted_level_cf = db.db.cf_handle(VOTER_NODE_VOTED_LEVEL_CF).unwrap();
        let proposer_node_vote_cf = db.db.cf_handle(PROPOSER_NODE_VOTE_CF).unwrap();
        let proposer_tree_level_cf = db.db.cf_handle(PROPOSER_TREE_LEVEL_CF).unwrap();
        let parent_neighbor_cf = db.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let vote_neighbor_cf = db.db.cf_handle(VOTE_NEIGHBOR_CF).unwrap();
        let voter_tree_level_count_cf = db.db.cf_handle(VOTER_TREE_LEVEL_COUNT_CF).unwrap();
        let proposer_vote_count_cf = db.db.cf_handle(PROPOSER_VOTE_COUNT_CF).unwrap();
        let proposer_leader_sequence_cf = db.db.cf_handle(PROPOSER_LEADER_SEQUENCE_CF).unwrap();
        let proposer_ledger_order_cf = db.db.cf_handle(PROPOSER_LEDGER_ORDER_CF).unwrap();
        let proposer_ref_neighbor_cf = db.db.cf_handle(PROPOSER_REF_NEIGHBOR_CF).unwrap();
        let transaction_ref_neighbor_cf = db.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();

        // insert genesis blocks
        let mut wb = WriteBatch::default();

        // voter genesis blocks
        let mut voter_ledger_tips = db.voter_ledger_tips.lock().unwrap();
        for chain_num in 0..db.config.voter_chains {
            wb.put_cf(
                voter_node_level_cf,
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
                serialize(&(0 as u64)).unwrap(),
            );
            wb.put_cf(
                voter_node_voted_level_cf,
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
                serialize(&(0 as u64)).unwrap(),
            );
            wb.put_cf(
                voter_node_chain_cf,
                serialize(&db.config.genesis_hashes[chain_num as usize]).unwrap(),
                serialize(&(chain_num as u16)).unwrap(),
            );
            wb.merge_cf(
                voter_tree_level_count_cf,
                serialize(&(chain_num as u16, 0 as u64)).unwrap(),
                serialize(&(1 as u64)).unwrap(),
            );

            voter_ledger_tips[chain_num as usize] = db.config.genesis_hashes[chain_num as usize];
        }
        drop(voter_ledger_tips);
        db.db.write(wb)?;

        Ok(db)
    }

    pub fn concensus(&self, block: &Block) -> Result<()> {
        // get cf handles
        let proposer_node_level_cf = self.db.cf_handle(PROPOSER_NODE_LEVEL_CF).unwrap();
        let voter_node_level_cf = self.db.cf_handle(VOTER_NODE_LEVEL_CF).unwrap();
        let voter_node_chain_cf = self.db.cf_handle(VOTER_NODE_CHAIN_CF).unwrap();
        let voter_node_voted_level_cf = self.db.cf_handle(VOTER_NODE_VOTED_LEVEL_CF).unwrap();
        let proposer_tree_level_cf = self.db.cf_handle(PROPOSER_TREE_LEVEL_CF).unwrap();
        let parent_neighbor_cf = self.db.cf_handle(PARENT_NEIGHBOR_CF).unwrap();
        let vote_neighbor_cf = self.db.cf_handle(VOTE_NEIGHBOR_CF).unwrap();
        let proposer_vote_count_cf = self.db.cf_handle(PROPOSER_VOTE_COUNT_CF).unwrap();
        let voter_parent_neighbor_cf = self.db.cf_handle(VOTER_PARENT_NEIGHBOR_CF).unwrap();
        let transaction_ref_neighbor_cf = self.db.cf_handle(TRANSACTION_REF_NEIGHBOR_CF).unwrap();
        let proposer_ref_neighbor_cf = self.db.cf_handle(PROPOSER_REF_NEIGHBOR_CF).unwrap();
        let voter_tree_level_count_cf = self.db.cf_handle(VOTER_TREE_LEVEL_COUNT_CF).unwrap();

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
        let block_hash = block.hash();
        let parent_hash = block.header.parent;
        put_value!(parent_neighbor_cf, block_hash, parent_hash);

        match &block.content {
            Content::Proposer(content) => {
                // add ref'ed blocks
                // note that the parent is the first proposer block that we refer
                let mut refed_proposer: Vec<H256> = vec![parent_hash];
                refed_proposer.extend(&content.refs);
                put_value!(proposer_ref_neighbor_cf, block_hash, refed_proposer);

                // get current block level
                let parent_level: u64 = get_value!(proposer_node_level_cf, parent_hash);
                let self_level = parent_level + 1;
                // set current block level
                put_value!(proposer_node_level_cf, block_hash, self_level as u64);
                merge_value!(proposer_tree_level_cf, self_level, block_hash);

                // mark ourself as unreferred proposer
                // This should happen before committing to the database, since we want this
                // add operation to happen before later block deletes it. NOTE: we could do this
                // after committing to the database. The solution is to add a "pre-delete" set in
                // unreferred_proposers that collects the entries to delete before they are even
                // inserted. If we have this, we don't need to add/remove entries in order.
                let mut unreferred_proposers = self.unreferred_proposers.lock().unwrap();
                unreferred_proposers.insert(block_hash);
                drop(unreferred_proposers);

                // mark outself as unconfirmed proposer
                // This could happen before committing to database, since this block has to become
                // the leader or be referred by a leader. However, both requires the block to be
                // committed to the database. For the same reason, this should not happen after
                // committing to the database (think about the case where this block immediately
                // becomes the leader and is ready to be confirmed).
                let mut unconfirmed_proposers = self.unconfirmed_proposers.lock().unwrap();
                unconfirmed_proposers.insert(block_hash);
                drop(unconfirmed_proposers);

                // remove referenced proposer and transaction blocks from the unreferred list
                // This could happen after committing to the database. It's because that we are
                // only removing transaction blocks here, and the entries we are trying to remove
                // are guaranteed to be already there (since they are inserted before the
                // corresponding transaction blocks are committed).
                let mut unreferred_proposers = self.unreferred_proposers.lock().unwrap();
                for ref_hash in &content.refs {
                    unreferred_proposers.remove(&ref_hash);
                }
                unreferred_proposers.remove(&parent_hash);
                drop(unreferred_proposers);

                debug!(
                    "Adding proposer block {:.8} at level {}",
                    block_hash, self_level
                );
            }
            Content::Voter(content) => {
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
