use crate::transaction::{Account, Address, Transaction};
use bincode::{deserialize, serialize};
use ed25519_dalek::Keypair;
use rocksdb::*;

const KEYPAIR_LEVEL: &str = "KEYPAIR_LEVEL";
const ACCOUNT_LEVEL: &str = "ACCOUNT_LEVEL";

pub struct BalanceDatabase {
    pub db: rocksdb::DB, // coin id to output
}

impl BalanceDatabase {
    /// Open the database at the given path, and create a new one if one is missing.
    fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let mut cfs = vec![];

        macro_rules! add_cf {
            ($cf:expr) => {{
                let cf_option = Options::default();
                let cf = ColumnFamilyDescriptor::new($cf, cf_option);
                cfs.push(cf);
            }};
            ($cf:expr, $merge_op:expr) => {{
                let mut cf_option = Options::default();
                cf_option.set_merge_operator("mo", $merge_op, $merge_op);
                let cf = ColumnFamilyDescriptor::new($cf, cf_option);
                cfs.push(cf);
            }};
        }

        add_cf!(KEYPAIR_LEVEL);
        add_cf!(ACCOUNT_LEVEL);

        let mut opts = Options::default();
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        opts.set_allow_concurrent_memtable_write(false);
        let memtable_opts = MemtableFactory::HashSkipList {
            bucket_count: 1 << 20,
            height: 8,
            branching_factor: 4,
        };
        opts.set_memtable_factory(memtable_opts);
        opts.optimize_for_point_lookup(512);
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(16);

        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        Ok(Self { db })
    }

    /// Create a new database at the given path, and initialize the content.
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, rocksdb::Error> {
        DB::destroy(&Options::default(), &path)?;
        let db = Self::open(&path)?;

        Ok(db)
    }

    pub fn snapshot(&self) -> Result<Vec<u8>, rocksdb::Error> {
        let mut iter_opt = rocksdb::ReadOptions::default();
        iter_opt.set_prefix_same_as_start(false);
        iter_opt.set_total_order_seek(true);
        let iter = self.db.iterator_opt(rocksdb::IteratorMode::Start, iter_opt);
        let mut inited = false;
        let mut checksum: Vec<u8> = vec![];
        for i in iter {
            let (k, _v) = i?;
            if !inited {
                checksum = vec![0; k.as_ref().len()];
                inited = true;
            }
            checksum = checksum
                .iter()
                .zip(k.as_ref())
                .map(|(&c, &k)| c ^ k)
                .collect();
        }
        Ok(checksum)
    }

    pub fn execute_transactions(
        &self,
        transactions: &Vec<Transaction>,
    ) -> Result<(), BalanceError> {
        // check whether the inputs used in this transaction are all unspent, and whether the value
        // field in inputs are correct, and whether all owners have signed the transaction

        for t in transactions {
            let mut input_account: Account = self.get_account(&t.input_account)?;
            let mut output_acccount: Account = self.get_account(&t.output_acccount)?;

            let signed_account: Address = ring::digest::digest(
                &ring::digest::SHA256,
                &t.authorization.as_ref().unwrap().pubkey,
            )
            .into();

            if signed_account != input_account.address {
                return Ok(());
            }
            if input_account.balance >= t.value {
                input_account.balance = input_account.balance - t.value;
                output_acccount.balance = output_acccount.balance + t.value;
                self.insert_account(&input_account)?;
                self.insert_account(&output_acccount)?;
            }
        }

        // write the transaction as a batch
        // TODO: we don't write to wal here, so should the program crash, the db will be in
        // an inconsistent state. The solution here is to manually flush the memtable to
        // the disk at certain time, and manually log the state (e.g. voter tips, etc.)
        Ok(())
    }

    pub fn flush(&self) -> Result<(), rocksdb::Error> {
        let mut flush_opt = rocksdb::FlushOptions::default();
        flush_opt.set_wait(true);
        self.db.flush_opt(&flush_opt)
    }

    pub fn get_account(&self, address: &Address) -> Result<Account, BalanceError> {
        let account_level = self.db.cf_handle(ACCOUNT_LEVEL).unwrap();
        match self.db.get_pinned_cf(account_level, address)? {
            Some(d) => {
                let val: Account = deserialize(&d)?;
                return Ok(val);
            }
            None => return Err(BalanceError::NotFoundAccount),
        };
    }

    pub fn insert_account(&self, account: &Account) -> Result<(), BalanceError> {
        let account_level = self.db.cf_handle(ACCOUNT_LEVEL).unwrap();
        self.db.put_cf(
            account_level,
            serialize(&account.address)?,
            serialize(account)?,
        )?;
        Ok(())
    }

    // keypair
    pub fn get_keypair(&self, address: &Address) -> Result<Keypair, BalanceError> {
        let keypair_level = self.db.cf_handle(KEYPAIR_LEVEL).unwrap();
        match self.db.get_pinned_cf(keypair_level, address)? {
            Some(d) => {
                let val: Vec<u8> = deserialize(&d)?;
                let keypair = Keypair::from_bytes(&val).unwrap();
                return Ok(keypair);
            }
            None => return Err(BalanceError::NotFoundKeypair),
        };
    }

    pub fn insert_keypair(&self, address: &Address, keypair: &Keypair) -> Result<(), BalanceError> {
        let keypair_level = self.db.cf_handle(KEYPAIR_LEVEL).unwrap();
        self.db
            .put_cf(keypair_level, serialize(&address)?, keypair.to_bytes())?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum BalanceError {
    NotFoundAccount,
    NotFoundKeypair,
    RocksdbErr(String),
    BincodeErr(String),
}

impl From<rocksdb::Error> for BalanceError {
    fn from(value: rocksdb::Error) -> Self {
        BalanceError::RocksdbErr(value.to_string())
    }
}
impl From<bincode::Error> for BalanceError {
    fn from(value: bincode::Error) -> Self {
        BalanceError::BincodeErr(value.to_string())
    }
}
