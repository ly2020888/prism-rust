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
    let mut missing_blocks: Vec<H256> = vec![];

    // check whether the tx block referred are present
    for tx_block_hash in content.refs.iter() {
        let tx_block = check_block_exists(*tx_block_hash, blockchain);
        if !tx_block {
            missing_blocks.push(*tx_block_hash);
        }
    }

    let tx_block = check_block_exists(content.parent, blockchain);
    if !tx_block {
        missing_blocks.push(content.parent);
    }
    missing_blocks
}
