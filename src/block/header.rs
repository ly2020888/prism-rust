use crate::crypto::hash::{Hashable, H256};

/// The header of a block.
#[derive(Serialize, Deserialize, Clone, Debug, Hash, Copy)]
pub struct Header {
    /// Hash of this block
    pub hash: Option<H256>,
    /// Hash of the parent voter block.
    pub parent: H256,
    /// Block creation time in UNIX format.
    pub timestamp: u128,
    /// Proof of work nonce.
    pub chain_id: u16,
    /// Merkle root of the block content.
    pub content_merkle_root: H256,
}

impl Header {
    /// Create a new block header.
    pub fn new(parent: H256, timestamp: u128, chain_id: u16, content_merkle_root: H256) -> Self {
        Self {
            hash: None,
            parent,
            timestamp,
            chain_id,
            content_merkle_root,
        }
    }
}

impl Hashable for Header {
    fn hash(&self) -> H256 {
        let serialized = bincode::serialize(self).unwrap();
        let digest = ring::digest::digest(&ring::digest::SHA256, &serialized);
        digest.into()
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::header::Header;

    use crate::crypto::hash::{Hashable, H256};

    /// The hash should match
    /// header.hash() should be used in condition that header.hash == None
    #[test]
    fn test_hash() {
        let header = sample_header();
        let header_hash_should_be = header.hash.unwrap();
        println!("{:?}", header_hash_should_be);
    }

    #[macro_export]
    macro_rules! gen_hashed_data {
        () => {{
            vec![
                (&hex!("0a0b0c0d0e0f0e0d0a0b0c0d0e0f0e0d0a0b0c0d0e0f0e0d0a0b0c0d0e0f0e0d")).into(),
                (&hex!("0102010201020102010201020102010201020102010201020102010201020102")).into(),
                (&hex!("0a0a0a0a0b0b0b0b0a0a0a0a0b0b0b0b0a0a0a0a0b0b0b0b0a0a0a0a0b0b0b0b")).into(),
                (&hex!("0403020108070605040302010807060504030201080706050403020108070605")).into(),
                (&hex!("1a2a3a4a1a2a3a4a1a2a3a4a1a2a3a4a1a2a3a4a1a2a3a4a1a2a3a4a1a2a3a4a")).into(),
                (&hex!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")).into(),
                (&hex!("0000000100000001000000010000000100000001000000010000000100000001")).into(),
            ]
        }};
    }

    // Header stuff
    pub fn sample_header() -> Header {
        let parent_hash: H256 =
            (&hex!("0102010201020102010201020102010201020102010201020102010201020102")).into();
        let timestamp: u128 = 7_094_730;
        let content_root: H256 =
            (&hex!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")).into();

        let mut header = Header::new(parent_hash, timestamp, 0, content_root);
        header.hash = Some(header.hash());
        header
    }
}
