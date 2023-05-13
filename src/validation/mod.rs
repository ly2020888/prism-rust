mod proposer_block;
mod transaction;
mod voter_block;
use crate::block::{Block, Content};
use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;
use crate::crypto::hash::H256;

/// The result of block validation.
#[derive(Debug)]
pub enum BlockResult {
    /// The validation passes.
    Pass,
    /// Some references are missing.
    MissingReferences(Vec<H256>),
    /// Proposer Ref level > parent
    WrongProposerRef,
    /// A voter block has a out-of-range chain number.
    WrongChainNumber,
    EmptyTransaction,
    ZeroValue,
    InsufficientInput,
    WrongSignature,
}

impl std::fmt::Display for BlockResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BlockResult::Pass => write!(f, "validation passed"),
            BlockResult::MissingReferences(_) => write!(f, "referred blocks not in system"),
            BlockResult::WrongProposerRef => {
                write!(f, "referred proposer blocks level larger than parent")
            }
            BlockResult::WrongChainNumber => write!(f, "chain number out of range"),
            BlockResult::EmptyTransaction => write!(f, "empty transaction input or output"),
            BlockResult::ZeroValue => {
                write!(f, "transaction input or output value contains a zero")
            }
            BlockResult::InsufficientInput => write!(f, "insufficient input"),
            BlockResult::WrongSignature => write!(f, "signature mismatch"),
        }
    }
}

/// Validate a block that already passes pow and sortition test. See if parents/refs are missing.
pub fn check_data_availability(
    block: &Block,
    blockchain: &BlockChain,
    blockdb: &BlockDatabase,
) -> BlockResult {
    let mut missing = vec![];

    // check whether the parent exists
    let parent = block.header.parent;
    let parent_availability = check_block_exists(parent, blockchain);
    if !parent_availability {
        missing.push(parent);
    }

    // match the block type and check content
    match &block.content {
        Content::Proposer(content) => {
            // check for missing references
            let missing_refs = proposer_block::get_missing_references(content, blockchain, blockdb);
            if !missing_refs.is_empty() {
                missing.extend_from_slice(&missing_refs);
            }
        }
        Content::Voter(content) => {
            // check for missing references
            let missing_refs = voter_block::get_missing_references(content, blockchain, blockdb);
            if !missing_refs.is_empty() {
                missing.extend_from_slice(&missing_refs);
            }
        }
    }

    if !missing.is_empty() {
        BlockResult::MissingReferences(missing)
    } else {
        BlockResult::Pass
    }
}

/// Check whether a transaction block exists in the block database.
fn check_block_exists(hash: H256, blockchain: &BlockChain) -> bool {
    match blockchain.contains_transaction(&hash) {
        Err(e) => panic!("Blockchain error {}", e),
        Ok(b) => b,
    }
}
