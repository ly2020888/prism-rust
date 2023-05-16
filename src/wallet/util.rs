use super::Wallet;
use crate::{crypto::hash::H256, transaction::Account};
use base64::engine::general_purpose;
use base64::Engine;
use ed25519_dalek::Keypair;
use rand::distributions::{Alphanumeric, Distribution};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::process;
use tracing::{error, info};

const ROOT: &str = "./wallets/";

fn generate_random_hash() -> String {
    let mut rng = rand::thread_rng();
    Alphanumeric
        .sample_iter(&mut rng)
        .take(48)
        .map(char::from)
        .collect::<String>()
        .to_uppercase()
}

fn create_wallets(fund_addrs: Vec<String>, value: u64) -> Vec<Wallet> {
    let mut addrs = vec![];
    for addr in fund_addrs {
        // info!("读取地址：{}", addr);
        let decoded = match general_purpose::STANDARD.decode(&addr.trim()) {
            Ok(d) => d,
            Err(e) => {
                error!("Error decoding address {}: {}", &addr.trim(), e);
                process::exit(1);
            }
        };
        let addr_bytes: [u8; 32] = (&decoded[0..32]).try_into().unwrap();
        let hash: H256 = addr_bytes.into();
        addrs.push(hash);
    }
    let mut wallets: Vec<Wallet> = vec![];

    for addr in addrs {
        let mut wallet = Wallet::new(Account {
            address: addr,
            balance: value,
        });

        // create wallet key pair if there is none
        if !wallet.has() {
            wallet.generate_keypair();
        }
        wallets.push(wallet);
    }

    wallets
}

fn load_keypair(wallet: &mut Vec<Wallet>, wallet_keys: Vec<String>) {
    for (idx, key_path) in wallet_keys.iter().enumerate() {
        let content = match std::fs::read(&key_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Error loading key pair at {}: {}", &key_path, &e);
                process::exit(1);
            }
        };

        let keypair = Keypair::from_bytes(&content);
        match keypair {
            Ok(a) => {
                wallet[idx].load_keypair(a);
                info!("Loaded key pair for address {}", &wallet[idx].address())
            }
            Err(e) => {
                error!("Error loading key pair into wallet: {}", &e);
                process::exit(1);
            }
        }
    }
}

fn save_wallets_list(wallets: &Vec<Wallet>) {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(ROOT.to_string() + "total.txt")
        .unwrap();
    for wal in wallets {
        let save_path = format!("{}", wal.address())[0..8].to_owned() + ".txt";
        let total = format!("{}\n", wal.address()).to_string();

        let keypair = wal.get_keypair();
        fs::write(ROOT.to_string() + &save_path, keypair).unwrap();

        file.write(total.as_bytes()).unwrap();
    }
}

fn load_fund_addrs() -> Vec<String> {
    let content = fs::read_to_string(ROOT.to_string() + "total.txt").unwrap();
    let content = content
        .split("\n")
        .filter(|s| s.len() > 0)
        .map(|s| s.to_owned())
        .collect::<Vec<String>>();
    content
}

/// n指定了需要随机生成的账户数，生成在./wallets/total.txt
pub fn save_wallets(n: usize, fund: u64) {
    let mut random_fund_addr = vec![];
    let mut paths = vec![];
    for _ in 0..n {
        let addr = generate_random_hash();
        paths.push(format!("{}", addr)[0..8].to_owned() + ".txt");
        random_fund_addr.push(addr);
    }
    let wallets: Vec<Wallet> = create_wallets(random_fund_addr, fund);
    save_wallets_list(&wallets);
}

/// 读取wallet目录下的total.txt的账户所有秘钥
pub fn load_wallets(fund: u64) -> Vec<Wallet> {
    let addrs = load_fund_addrs();
    let mut wallets: Vec<Wallet> = create_wallets(addrs.clone(), fund);
    let mut paths = vec![];
    for addr in addrs {
        let path = ROOT.to_owned() + &format!("{}", addr)[0..8].to_owned() + ".txt";
        paths.push(path);
    }

    load_keypair(&mut wallets, paths);
    wallets
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_save_wallets() {
        save_wallets(5, 100);
    }

    #[test]
    fn test_load_wallets() {
        let test = load_wallets(100);
        println!("{:?}", test)
    }
}
