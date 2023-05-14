use crate::transaction::{Account, Address, Transaction};
use bincode::{deserialize, serialize};
use rocksdb::*;

pub struct BalanceDatabase {
    pub db: rocksdb::DB, // coin id to output
}

impl BalanceDatabase {
    /// Open the database at the given path, and create a new one if one is missing.
    fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let cfs = vec![];
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
    ) -> Result<(), rocksdb::Error> {
        // use batch for the transaction
        let mut batch = rocksdb::WriteBatch::default();

        // check whether the inputs used in this transaction are all unspent, and whether the value
        // field in inputs are correct, and whether all owners have signed the transaction

        for t in transactions {
            let input_account: Address = t.input_account.clone();
            let output_acccount: Address = t.output_acccount.clone();
            let a_id_ser = serialize(&input_account).unwrap();
            let b_id_ser = serialize(&output_acccount).unwrap();
            let mut account_a: Account;
            let mut account_b: Account;
            match self.db.get_pinned(&a_id_ser)? {
                Some(d) => {
                    account_a = deserialize(&d).unwrap();
                }
                None => return Ok(()),
            }

            match self.db.get_pinned(&b_id_ser)? {
                Some(d) => {
                    account_b = deserialize(&d).unwrap();
                }
                None => return Ok(()),
            }

            let signed_account: Address = ring::digest::digest(
                &ring::digest::SHA256,
                &t.authorization.as_ref().unwrap().pubkey,
            )
            .into();

            if signed_account != input_account {
                return Ok(());
            }
            if account_a.balance >= t.value {
                account_a.balance = account_a.balance - t.value;
                account_b.balance = account_b.balance + t.value;
                batch.put(
                    serialize(&input_account).unwrap(),
                    serialize(&account_a).unwrap(),
                );
                batch.put(
                    serialize(&output_acccount).unwrap(),
                    serialize(&account_b).unwrap(),
                );
            }
        }

        // write the transaction as a batch
        // TODO: we don't write to wal here, so should the program crash, the db will be in
        // an inconsistent state. The solution here is to manually flush the memtable to
        // the disk at certain time, and manually log the state (e.g. voter tips, etc.)
        self.db.write_without_wal(batch)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), rocksdb::Error> {
        let mut flush_opt = rocksdb::FlushOptions::default();
        flush_opt.set_wait(true);
        self.db.flush_opt(&flush_opt)
    }
}
