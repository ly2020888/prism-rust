use super::check_block_exists;
use crate::block::content::Content;

use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;

use crate::crypto::hash::H256;

pub fn get_missing_references(
    content: &Content,
    blockchain: &BlockChain,
    _blockdb: &BlockDatabase,
) -> Vec<H256> {
    let mut missing_blocks = vec![];

    // check the voter parent
    let voter_parent = check_block_exists(content.parent, blockchain);
    if !voter_parent {
        missing_blocks.push(content.parent);
    }

    // check the votes
    for prop_hash in content.refs.iter() {
        let avail = check_block_exists(*prop_hash, blockchain);
        if !avail {
            missing_blocks.push(*prop_hash);
        }
    }

    missing_blocks
}

pub fn check_chain_number(content: &Content, blockchain: &BlockChain) -> bool {
    let chain_num = blockchain.voter_chain_number(&content.parent).unwrap();
    chain_num == content.chain_id
}
