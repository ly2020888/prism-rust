use super::Block;
use super::Content as BlockContent;
use crate::crypto::hash::{Hashable, H256};
use crate::crypto::merkle::MerkleTree;
//use crate::experiment::performance_counter::PayloadSize;
use crate::transaction::Transaction;

/// The content of a voter block.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Content {
    /// ID of the voter chain this block is attaching to.
    pub chain_number: u16,
    /// Hash of the parent voter block.
    pub parent: H256,

    /// 区块所包含的交易信息
    pub transactions: Vec<Transaction>,

    /// 普通区块的引用
    pub refs: Vec<H256>,

    pub weight: u64,

    pub height: u64,
}

impl Content {
    /// Create new voter block content.
    pub fn new(
        chain_number: u16,
        parent: H256,
        transactions: Vec<Transaction>,
        refs: Vec<H256>,
        weight: u64,
        height: u64,
    ) -> Self {
        Self {
            chain_number,
            parent,
            transactions,
            refs,
            weight,
            height,
        }
    }
}

// impl PayloadSize for Content {
//     fn size(&self) -> usize {
//         std::mem::size_of::<u16>()
//             + std::mem::size_of::<H256>()
//             + self.refs.len() * std::mem::size_of::<H256>()
//     }
// }

impl Hashable for Content {
    fn hash(&self) -> H256 {
        // TODO: we are hashing in a merkle tree. why do we need so?
        let merkle_tree = MerkleTree::new(&self.refs);
        let mut bytes = [0u8; 66];
        bytes[..2].copy_from_slice(&self.chain_number.to_be_bytes());
        bytes[2..34].copy_from_slice(self.parent.as_ref());
        bytes[34..66].copy_from_slice(merkle_tree.root().as_ref());
        ring::digest::digest(&ring::digest::SHA256, &bytes).into()
    }
}

/// Generate the genesis block of the voter chain with the given chain ID.
pub fn voter_genesis(chain_num: u16) -> Block {
    let all_zero: [u8; 32] = [0; 32];
    let content = Content {
        chain_number: chain_num,
        parent: all_zero.into(),
        transactions: vec![],
        refs: vec![],
        weight: 0,
        height: 0,
    };
    // TODO: this block will definitely not pass validation. We depend on the fact that genesis
    // blocks are added to the system at initialization. Seems like a moderate hack.
    Block::new(
        all_zero.into(),
        0,
        0,
        all_zero.into(),
        BlockContent::Voter(content),
    )
}

/// Generate the genesis block of the voter chain with the given chain ID.
pub fn proposer_genesis(chain_num: u16) -> Block {
    let all_zero: [u8; 32] = [0; 32];
    let content = Content {
        chain_number: chain_num,
        parent: all_zero.into(),
        transactions: vec![],
        refs: vec![],
        weight: 0,
        height: 0,
    };
    // TODO: this block will definitely not pass validation. We depend on the fact that genesis
    // blocks are added to the system at initialization. Seems like a moderate hack.
    Block::new(
        all_zero.into(),
        0,
        0,
        all_zero.into(),
        BlockContent::Proposer(content),
    )
}

#[cfg(test)]
pub mod test {}
