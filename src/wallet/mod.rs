extern crate ed25519_dalek;
extern crate rand;
use crate::crypto::hash::{Hashable, H256};
use crate::transaction::{Account, Address, Authorization, Transaction};
use ed25519_dalek::Keypair;
use ed25519_dalek::{SignatureError, Signer};
use rand::rngs::OsRng;
use std::fmt;
use std::time::SystemTime;

pub const KEYPAIR_CF: &str = "KEYPAIR"; // &Address to &KeyPairPKCS8

pub type Result<T> = std::result::Result<T, WalletError>;

/// A data structure to maintain key pairs and their coins, and to generate transactions.
#[derive(Debug)]
pub struct Wallet {
    /// The underlying RocksDB handle.
    account: Account,
    /// Keep key pair (in pkcs8 bytes) in memory for performance, it's duplicated in database as well.
    keypair: Option<Keypair>,
}

#[derive(Debug)]
pub enum WalletError {
    InsufficientBalance,
    MissingKeyPair,
    SignError(String),
}

impl From<SignatureError> for WalletError {
    fn from(value: SignatureError) -> Self {
        WalletError::SignError(value.to_string())
    }
}

impl fmt::Display for WalletError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WalletError::InsufficientBalance => write!(f, "insufficient balance"),
            WalletError::MissingKeyPair => write!(f, "missing key pair for the requested address"),
            WalletError::SignError(x) => write!(f, "{}", &x),
        }
    }
}

impl Wallet {
    pub fn new(account: Account) -> Self {
        Self {
            account,
            keypair: None,
        }
    }

    pub fn get_keypair(&self) -> Vec<u8> {
        self.keypair.as_ref().unwrap().to_bytes().to_vec()
    }
    pub fn address(&self) -> H256 {
        self.account.address
    }

    pub fn number_of_coins(&self) -> u64 {
        self.account.balance
    }

    /// 返回是否存在keypair
    pub fn has(&self) -> bool {
        self.keypair.is_some()
    }
    /// Generate a new key pair

    pub fn generate_keypair(&mut self) {
        let mut csprng = OsRng {};
        let keypair: Keypair = Keypair::generate(&mut csprng);
        self.keypair = Some(keypair);
    }

    pub fn load_keypair(&mut self, keypair: Keypair) {
        self.keypair = Some(keypair);
    }

    /// Create a transaction using the wallet coins
    pub fn create_transaction(&mut self, recipient: Address, value: u64) -> Result<Transaction> {
        if self.account.balance < value {
            // we don't have enough money in wallet
            return Err(WalletError::InsufficientBalance);
        }

        self.account.balance = self.account.balance - value;

        let mut raw_transaction = Transaction {
            hash: None,
            input_account: self.account.address,
            output_acccount: recipient,
            value,
            authorization: None,
            time_stamp: SystemTime::now(),
        };
        raw_transaction.hash = Some(raw_transaction.hash());

        let raw = bincode::serialize(&raw_transaction).unwrap();
        let kp = self.keypair.as_mut().unwrap();
        let aut = Authorization {
            pubkey: kp.public.to_bytes().to_vec(),
            signature: kp.try_sign(&raw)?.to_bytes().to_vec(),
        };
        raw_transaction.authorization = Some(aut);
        Ok(raw_transaction)
    }
}

#[cfg(test)]
pub mod tests {}
