mod proposer_block;
mod transaction;
mod voter_block;

use tracing::info;

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
    /// 错误的领导者区块，不符合连接规则
    WrongProposerBlock,
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
            BlockResult::WrongProposerBlock => write!(f, "错误的领导者区块，不符合连接规则"),
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
            // 之所以要和1进行比较，是因为在一开始区块链中仅仅存在普通区块，连接规则不可能被满足，
            // 因此高度为1的proposer区块跳过验证
            // 一旦proposer区块的高度>1之后，proposer区块需要满足连接规则
            if content.height != 1 {
                let mut refs = vec![content.parent];
                refs.extend(&content.refs);
                let res = check_proposer_refs(refs, content.height, &blockchain);
                if !res {
                    return BlockResult::WrongProposerBlock;
                }
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

fn check_proposer_refs(refs: Vec<H256>, height: u64, blockchain: &BlockChain) -> bool {
    // 如果proposer区块没有引用proposer区块
    // 如果引用了但是高度不严格等于其加1
    // 均验证不通过
    for hash in refs {
        match blockchain.is_proposer_block(hash) {
            Ok((true, inner_height)) => {
                if inner_height + 1 == height {
                    return true;
                }
            }
            Ok((false, _)) => {
                continue;
            }
            Err(e) => {
                info!("{:?}", e);
                return false;
            }
        }
    }

    return false;
}
