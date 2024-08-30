use std::{collections::HashMap, thread, time::Duration};

use futures::executor::block_on;
use labrpc::*;

use crate::{
    msg::{
        CommitRequest, CommitResponse, GetRequest, PrewriteRequest, PrewriteResponse,
        TimestampRequest, Write,
    },
    service::{TSOClient, TransactionClient},
};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: Option<u64>,
    writes: HashMap<Vec<u8>, Vec<u8>>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_client,
            txn_client,
            start_ts: None,
            writes: HashMap::default(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        for i in 0..RETRY_TIMES {
            match block_on(async { self.tso_client.get_timestamp(&TimestampRequest {}).await }) {
                Ok(resp) => return Ok(resp.ts),
                Err(_) => {
                    thread::sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << i)));
                }
            }
        }

        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        if let Some(ts) = self.start_ts {
            debug!("a transaction is in progress")
        }

        let current_ts = self.get_timestamp().unwrap();

        info!("transaction start, record start_ts = {}", current_ts);
        self.start_ts = Some(current_ts);
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        if self.start_ts.is_none() {
            todo!("return err")
        }

        let reply = block_on(async {
            self.txn_client
                .get(&GetRequest {
                    key,
                    start_ts: self.start_ts.unwrap(),
                })
                .await
        })?;

        Ok(reply.value)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.writes.insert(key, value);
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        let mut w_iter = self.writes.iter();
        let start_ts = self.start_ts.unwrap();

        let primary_write = w_iter.next().unwrap();

        let reply = block_on(async {
            self.txn_client
                .prewrite(&PrewriteRequest {
                    write: Some(Write {
                        key: primary_write.0.to_owned(),
                        value: primary_write.1.to_owned(),
                    }),
                    primary: primary_write.0.to_owned(),
                    start_ts,
                })
                .await
        })
        .or_else(|err| match err {
            Error::Other(_) => return Ok(PrewriteResponse { ok: false }),
            _ => return Err(err),
        })?;

        if !reply.ok {
            return Ok(false);
        }

        for w in w_iter {
            let reply = block_on(async {
                self.txn_client
                    .prewrite(&PrewriteRequest {
                        write: Some(Write {
                            key: w.0.to_owned(),
                            value: w.1.to_owned(),
                        }),
                        primary: primary_write.0.to_owned(),
                        start_ts,
                    })
                    .await
            })
            .or_else(|err| match err {
                Error::Other(_) => return Ok(PrewriteResponse { ok: false }),
                _ => return Err(err),
            })?;

            if !reply.ok {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;
        let reply = block_on(async {
            self.txn_client
                .commit(&&CommitRequest {
                    is_primary: true,
                    start_ts,
                    commit_ts,
                    key: primary_write.0.to_owned(),
                })
                .await
        })
        .or_else(|err| match err {
            Error::Other(hook) if hook == "reqhook" => Ok(CommitResponse { ok: false }),
            _ => return Err(err),
        })?;

        if !reply.ok {
            return Ok(false);
        }

        for (k, v) in self.writes.iter() {
            if k == primary_write.0 {
                continue;
            }

            let reply = block_on(async {
                self.txn_client
                    .commit(&&CommitRequest {
                        is_primary: false,
                        start_ts,
                        commit_ts,
                        key: k.to_owned(),
                    })
                    .await
            })
            .or_else(|err| match err {
                Error::Other(_) => return Ok(CommitResponse { ok: true }),
                _ => return Err(err),
            })?;

            if !reply.ok {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
