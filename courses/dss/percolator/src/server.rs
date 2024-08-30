use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Display;
use std::fs::read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::u64;

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    ts: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        return Ok(TimestampResponse {
            ts: self.ts.fetch_add(1, Ordering::Relaxed),
        });
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Value(Vec<u8>),
    Lock(Vec<u8>, u64),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Timestamp(ts) => write!(f, "Timestamp({})", ts),
            Value::Value(v) => write!(f, "Value({})", String::from_utf8(v.clone()).unwrap()),
            Value::Lock(key, ts) => write!(
                f,
                "Lock({}, {})",
                String::from_utf8(key.clone()).unwrap(),
                ts
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

#[derive(Clone, Copy)]
pub enum Column {
    Write,
    Data,
    Lock,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Write => write!(f, "Write"),
            Column::Data => write!(f, "Data"),
            Column::Lock => write!(f, "Lock"),
        }
    }
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: &Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let ts_start = ts_start_inclusive.unwrap_or_default();
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);

        let col = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };

        // order by timestamp desc
        for (k, v) in col.iter().rev() {
            let ts = k.1;

            warn!(
                "KvTable [{}] try read key = {}, ts = {}",
                column,
                String::from_utf8(k.0.clone()).unwrap(),
                ts
            );

            if k.0 != *key {
                continue;
            }

            if !(ts >= ts_start && ts <= ts_end) {
                continue;
            }

            info!(
                "KvTable [{}] read key: ({}, {}), value: {}",
                column,
                String::from_utf8(k.0.clone()).unwrap(),
                ts,
                v
            );

            return Some((k, v));
        }

        None
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let col = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };

        info!(
            "KvTable [{}] insert key: ({}, {}), value: {}",
            column,
            String::from_utf8(key.clone()).unwrap(),
            ts,
            value
        );

        col.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let col = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };

        info!(
            "KvTable [{}] remove key: ({}, {})",
            column,
            String::from_utf8(key.clone()).unwrap(),
            commit_ts
        );

        col.remove(&(key, commit_ts));
    }

    #[inline]
    fn lookup_key_by_value(
        &self,
        column: Column,
        value: &Value,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<Key> {
        let col = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };

        let ts_start = ts_start_inclusive.unwrap_or_default();
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);

        info!(
            "KvTable [{}] lookup_key_by_value, value: {}, ts_range: ({}, {})",
            column, value, ts_start, ts_end,
        );

        for (k, v) in col.iter() {
            if !(k.1 >= ts_start && k.1 <= ts_end) {
                continue;
            }

            match (v, value) {
                (Value::Timestamp(ts), Value::Timestamp(target_ts)) => {
                    if *ts == *target_ts {
                        return Some(k.clone());
                    }
                }
                (Value::Value(data), Value::Value(target_data)) => {
                    if data == target_data {
                        return Some(k.clone());
                    }
                }
                (Value::Lock(primary_lock, ts), Value::Lock(target_lock, target_ts)) => {
                    if primary_lock == target_lock && ts == target_ts {
                        return Some(k.clone());
                    }
                }
                _ => {
                    error!("mismatch column type and value type");
                    return None;
                }
            }
        }

        None
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        loop {
            let mut kv_guard = self.data.lock().unwrap();

            if self
                .get_lock_range(&kv_guard, &req.key, None, Some(req.start_ts))
                .is_some()
            {
                // BackoffAndMaybeCleanupLock
                drop(kv_guard);
                self.back_off_maybe_clean_up_lock(req.start_ts, &req.key);
                continue;
            }

            let data = if let Some(latest_commit_ts) =
                self.get_write_range(&kv_guard, &req.key, None, Some(req.start_ts))
            {
                self.get_data(&kv_guard, &req.key, latest_commit_ts)
                    .unwrap_or_default()
            } else {
                Vec::default()
            };

            return Ok(GetResponse { value: data });
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let mut kv_guard: MutexGuard<'_, KvTable> = self.data.lock().unwrap();

        let (key, value) = match req.write {
            Some(w) => (w.key, w.value),
            None => {
                return Err(labrpc::Error::Other(String::from("no Write set provided")));
            }
        };

        let start_ts = req.start_ts;

        if self
            .get_write_range(&kv_guard, &key, Some(start_ts), None)
            .is_some()
        {
            info!(
                "current key exist Data record, key: {}",
                String::from_utf8(key.clone()).unwrap()
            );
            return Ok(PrewriteResponse { ok: false });
        }

        if self.get_lock_range(&kv_guard, &key, None, None).is_some() {
            info!(
                "current key exist Lock record, key: {}",
                String::from_utf8(key.clone()).unwrap()
            );
            return Ok(PrewriteResponse { ok: false });
        }

        self.set_lock(&mut kv_guard, key.clone(), start_ts, req.primary);
        self.set_data(&mut kv_guard, key, start_ts, value);

        Ok(PrewriteResponse { ok: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut kv_guard = self.data.lock().unwrap();

        if req.is_primary {
            match self.get_lock(&kv_guard, &req.key, req.start_ts) {
                Some((lk_primary, ts)) => {
                    if req.key != lk_primary {
                        todo!("check is pk")
                    }
                    self.remove_lock(&mut kv_guard, req.key.clone(), req.start_ts);
                    self.set_write(&mut kv_guard, req.key, req.commit_ts, req.start_ts);
                    return Ok(CommitResponse { ok: true });
                }
                None => return Ok(CommitResponse { ok: false }),
            }
        } else {
            // secondary lock can clear without result check
            self.remove_lock(&mut kv_guard, req.key.clone(), req.start_ts);
            self.set_write(&mut kv_guard, req.key, req.commit_ts, req.start_ts);
            return Ok(CommitResponse { ok: true });
        }
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: &Vec<u8>) {
        // Your code here.
        let mut kv_guard = self.data.lock().unwrap();

        if let Some((lk_primary, lk_start_ts)) =
            self.get_lock_range(&kv_guard, key, None, Some(start_ts))
        {
            /* TODO:
                primary lock exist, means previous transaction not committed
                should wait until TTL to clean primary lock
                pseudocode:
                if *key == lk_primary && !reach_ttl(){
                    return;
                }
            */

            // erase secondary lock and write commit
            match kv_guard.lookup_key_by_value(
                Column::Write,
                &Value::Timestamp(lk_start_ts),
                Some(lk_start_ts),
                None,
            ) {
                // primary commit success, exist write record
                Some((write_key, prev_commit_ts)) => {
                    self.remove_lock(&mut kv_guard, key.clone(), lk_start_ts);
                    self.set_write(&mut kv_guard, key.clone(), prev_commit_ts, lk_start_ts);
                }
                // commit failed, should clean both lock and data
                None => {
                    self.remove_lock(&mut kv_guard, key.clone(), lk_start_ts);
                    self.remove_data(&mut kv_guard, key.clone(), lk_start_ts);
                }
            }
        }
    }

    fn get_lock_range(
        &self,
        kv_guard: &MutexGuard<'_, KvTable>,
        key: &Vec<u8>,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(Vec<u8>, u64)> {
        kv_guard
            .read(key, Column::Lock, ts_start_inclusive, ts_end_inclusive)
            .map(|v| match v.1 {
                Value::Lock(is_primary, ts) => (is_primary.clone(), *ts),
                _ => panic!("invalid value type in Lock column"),
            })
    }

    fn get_lock(
        &self,
        kv_guard: &MutexGuard<'_, KvTable>,
        key: &Vec<u8>,
        ts: u64,
    ) -> Option<(Vec<u8>, u64)> {
        self.get_lock_range(kv_guard, key, Some(ts), Some(ts))
    }

    fn get_write_range(
        &self,
        kv_guard: &MutexGuard<'_, KvTable>,
        key: &Vec<u8>,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<u64> {
        kv_guard
            .read(key, Column::Write, ts_start_inclusive, ts_end_inclusive)
            .map(|v| match v.1 {
                Value::Timestamp(ts) => *ts,
                _ => panic!("invalid value type in Write column"),
            })
    }

    fn get_write(&self, kv_guard: &MutexGuard<'_, KvTable>, key: &Vec<u8>, ts: u64) -> Option<u64> {
        self.get_write_range(kv_guard, key, Some(ts), Some(ts))
    }

    fn get_data(
        &self,
        kv_guard: &MutexGuard<'_, KvTable>,
        key: &Vec<u8>,
        ts: u64,
    ) -> Option<Vec<u8>> {
        kv_guard
            .read(key, Column::Data, Some(ts), Some(ts))
            .map(|v| match v.1 {
                Value::Value(v) => v.clone(),
                _ => panic!("invalid value type in Data column"),
            })
    }

    fn set_lock(
        &self,
        kv_guard: &mut MutexGuard<'_, KvTable>,
        key: Vec<u8>,
        ts: u64,
        primary: Vec<u8>,
    ) {
        kv_guard.write(key, Column::Lock, ts, Value::Lock(primary, ts))
    }

    fn set_write(
        &self,
        kv_guard: &mut MutexGuard<'_, KvTable>,
        key: Vec<u8>,
        ts: u64,
        start_ts: u64,
    ) {
        kv_guard.write(key, Column::Write, ts, Value::Timestamp(start_ts))
    }

    fn set_data(
        &self,
        kv_guard: &mut MutexGuard<'_, KvTable>,
        key: Vec<u8>,
        ts: u64,
        value: Vec<u8>,
    ) {
        kv_guard.write(key, Column::Data, ts, Value::Value(value))
    }

    fn remove_lock(&self, kv_guard: &mut MutexGuard<'_, KvTable>, key: Vec<u8>, ts: u64) {
        kv_guard.erase(key, Column::Lock, ts)
    }

    fn remove_data(&self, kv_guard: &mut MutexGuard<'_, KvTable>, key: Vec<u8>, ts: u64) {
        kv_guard.erase(key, Column::Data, ts)
    }
}
