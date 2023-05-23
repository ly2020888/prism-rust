// Column family names for node/chain metadata
/// hash to node level (u64)
// pub const VOTER_NODE_LEVEL_CF: &str = "VOTER_NODE_LEVEL";

/// hash to chain number (u16)
pub const VOTER_NODE_CHAIN_CF: &str = "VOTER_NODE_CHAIN";

/// chain number and level (u16, u64) to number of blocks (u64)
// pub const VOTER_TREE_LEVEL_COUNT_CF: &str = "VOTER_TREE_LEVEL_COUNT_CF";

/// level (u64) to hashes of blocks (Vec<hash>)
pub const PROPOSER_TREE_LEVEL_CF: &str = "PROPOSER_TREE_LEVEL";

/// number of all votes on a block
pub const PROPOSER_VOTE_COUNT_CF: &str = "PROPOSER_VOTE_COUNT";

// Column family names for graph neighbors

/// the proposer parent of a block
pub const PARENT_NEIGHBOR_CF: &str = "GRAPH_PARENT_NEIGHBOR";

/// neighbors associated by a vote
// pub const VOTE_NEIGHBOR_CF: &str = "GRAPH_VOTE_NEIGHBOR";

/// the voter parent of a block
// pub const VOTER_PARENT_NEIGHBOR_CF: &str = "GRAPH_VOTER_PARENT_NEIGHBOR";

/// the block (voter and proposer included) 's ref neighbor
pub const BLOCK_REF_NEIGHBOR_CF: &str = "GRAPH_BLOCK_REF_NEIGHBOR";

/// the block 's proposer level
pub const PROPOSER_NODE_LEVEL_CF: &str = "PROPOSER_NODE_LEVEL_CF";

pub const PROPOSER_CF: &str = "PROPOSER_CF";
