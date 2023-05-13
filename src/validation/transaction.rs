use crate::transaction::Account;
use crate::transaction::Transaction;
use ed25519_dalek::PublicKey;
use ed25519_dalek::Signature;

use std::convert::TryFrom;

/// Checks that input and output value is not 0
pub fn check_non_zero(transaction: &Transaction) -> bool {
    !(transaction.value == 0)
}

/// Checks if input_sum >= output_sum
pub fn check_sufficient_input(transaction: &Transaction, user: Account) -> bool {
    let input_sum: u64 = user.balance;
    let output_sum: u64 = transaction.value;
    input_sum >= output_sum
}

pub fn check_signature_batch(transactions: &[Transaction]) -> bool {
    let mut raw_messages: Vec<Vec<u8>> = vec![];
    let mut messages: Vec<&[u8]> = vec![];
    let mut signatures: Vec<Signature> = vec![];
    let mut public_keys: Vec<PublicKey> = vec![];

    for (_idx, tx) in transactions.iter().enumerate() {
        let mut new_tx = tx.clone();
        new_tx.authorization = None;
        let raw = bincode::serialize(&new_tx).unwrap();
        raw_messages.push(raw);
    }

    for (idx, tx) in transactions.iter().enumerate() {
        for a in &tx.authorization {
            public_keys.push(PublicKey::from_bytes(&a.pubkey).unwrap());
            signatures.push(Signature::try_from(&a.signature[..]).unwrap());
            messages.push(&raw_messages[idx]);
        }
    }

    // TODO: tune the batch size
    match ed25519_dalek::verify_batch(&messages, &signatures, &public_keys) {
        Ok(()) => true,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::hash::H256,
        transaction::{self, Account, Transaction},
        wallet::{self, Wallet},
    };
    use rand::Rng;

    pub fn generate_random_hash() -> H256 {
        let mut rng = rand::thread_rng();
        let random_bytes: Vec<u8> = (0..32).map(|_| rng.gen_range(0, 255) as u8).collect();
        let mut raw_bytes = [0; 32];
        raw_bytes.copy_from_slice(&random_bytes);
        (&raw_bytes).into()
    }
    #[test]
    fn test_check_signature() {
        let my_acc = Account {
            address: generate_random_hash(),
            balance: 100,
        };
        let your_acc = Account {
            address: generate_random_hash(),
            balance: 100,
        };
        let mut mywallet = Wallet::new(my_acc);
        mywallet.generate_keypair();

        let mut yourwallet = Wallet::new(your_acc);
        yourwallet.generate_keypair();
        let tx = mywallet
            .create_transaction(yourwallet.address(), 10)
            .unwrap();
        let res = check_signature_batch(&[tx]);
        assert!(res);
    }
}
