pub mod content;
pub mod header;
use crate::crypto::hash::{Hashable, H256};
use crate::experiment::performance_counter::PayloadSize;
use header::Header;

/// A block in the Prism blockchain.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    /// The header of the block.
    pub header: header::Header,
    /// The content of the block. It could contain transactions, references, or votes, depending on
    /// the block type.
    pub content: Content,
}

impl Block {
    /// Create a new block.
    pub fn new(
        parent: H256,
        timestamp: u128,
        nonce: u32,
        content_merkle_root: H256,
        content: Content,
    ) -> Self {
        let header = Header::new(parent, timestamp, nonce, content_merkle_root);
        Self { header, content }
    }

    // TODO: use another name
    /// Create a new block from header.
    pub fn from_header(
        header: header::Header,
        content: Content,
        sortition_proof: Vec<H256>,
    ) -> Self {
        Self { header, content }
    }
}

impl Hashable for Block {
    fn hash(&self) -> H256 {
        // TODO: we are only hashing the header here.
        self.header.hash()
    }
}

impl PayloadSize for Block {
    fn size(&self) -> usize {
        std::mem::size_of::<header::Header>() + self.content.size()
    }
}

/// The content of a block. It could be transaction content, proposer content, or voter content,
/// depending on the type of the block.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Content {
    /// Proposer block content.
    Proposer(content::Content),
    /// Voter block content.
    Voter(content::Content),
}

impl Hashable for Content {
    fn hash(&self) -> H256 {
        match self {
            Content::Proposer(c) => c.hash(),
            Content::Voter(c) => c.hash(),
        }
    }
}

impl PayloadSize for Content {
    fn size(&self) -> usize {
        // TODO: we are not counting the 2 bits that are used to store block type
        match self {
            Content::Proposer(c) => c.size(),
            Content::Voter(c) => c.size(),
        }
    }
}

#[cfg(any(test, feature = "test-utilities"))]
pub mod tests {

    use super::*;
    use crate::transaction::Transaction;
    use rand::Rng;

    macro_rules! random_nonce {
        () => {{
            let mut rng = rand::thread_rng();
            let random_u32: u32 = rng.gen();
            random_u32
        }};
    }

    pub fn proposer_block(
        parent: H256,
        timestamp: u128,
        chain_number: u16,
        refs: Vec<H256>,
        transactions: Vec<Transaction>,
    ) -> Block {
        let content = Content::Proposer(content::Content {
            refs,
            transactions,
            parent,
            chain_number,
        });
        let content_hash = content.hash();
        Block::new(parent, timestamp, random_nonce!(), content_hash, content)
    }

    pub fn voter_block(
        parent: H256,
        timestamp: u128,
        chain_number: u16,
        refs: Vec<H256>,
        transactions: Vec<Transaction>,
    ) -> Block {
        let content = Content::Voter(content::Content {
            refs,
            transactions,
            parent,
            chain_number,
        });
        let content_hash = content.hash();
        Block::new(parent, timestamp, random_nonce!(), content_hash, content)
    }
}
