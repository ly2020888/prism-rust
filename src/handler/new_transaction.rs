use crate::crypto::hash::Hashable;
use crate::miner::memory_pool::MemoryPool;

use crate::network::server::Handle;
use crate::transaction::Transaction;
use std::sync::{Arc, Mutex};

/// Handler for new transaction
// We may want to add the result of memory pool check
pub fn new_transaction(
    transaction: Transaction,
    mempool: &Arc<Mutex<MemoryPool>>,
    _server: &Handle,
) {
    let mut mempool = mempool.lock().unwrap();
    // memory pool check
    if !mempool.contains(&transaction.hash()) {
        // if check passes, insert the new transaction into the mempool
        //server.broadcast(Message::NewTransactionHashes(vec![transaction.hash()]));
        mempool.insert(transaction);
    }
    drop(mempool);
}
