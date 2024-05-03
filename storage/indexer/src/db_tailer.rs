// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::schema::{
    event_by_key::EventByKeySchema, event_by_version::EventByVersionSchema,
    indexer_metadata::TailerMetadataSchema, transaction_by_account::TransactionByAccountSchema,
};
use aptos_config::config::index_db_tailer_config::IndexDBTailerConfig;
use aptos_schemadb::{SchemaBatch, DB};
use aptos_storage_interface::{DbReader, Result};
use aptos_types::{contract_event::ContractEvent, transaction::Version};
use std::sync::Arc;

pub struct DBTailer {
    pub last_version: Version,
    pub db: DB,
    pub main_db_reader: Arc<dyn DbReader>,
    batch_size: usize,
}

impl DBTailer {
    pub fn new(db: DB, db_reader: Arc<dyn DbReader>, config: &IndexDBTailerConfig) -> Self {
        let last_version = Self::initialize(&db);
        Self {
            last_version,
            db,
            main_db_reader: db_reader,
            batch_size: config.batch_size,
        }
    }

    fn initialize(db: &DB) -> Version {
        // read the latest key from the db
        let mut rev_iter_res = db
            .rev_iter::<TailerMetadataSchema>(Default::default())
            .expect("Cannot create db tailer metadata iterator");
        rev_iter_res
            .next()
            .map(|res| res.map_or(0, |(version, _)| version))
            .unwrap_or_default()
    }

    pub fn run(&self) -> Result<()> {
        loop {
            let db_iter = self
                .main_db_reader
                .get_db_backup_iter(self.last_version, self.batch_size)
                .expect("Cannot create db tailer iterator");
            let txn_by_account_batch = SchemaBatch::new();
            let event_by_key_batch = SchemaBatch::new();
            let event_by_version_batch: SchemaBatch = SchemaBatch::new();
            let mut version = self.last_version;
            db_iter.for_each(|res| {
                res.map(|(txn, events)| {
                    if let Some(txn) = txn.try_as_signed_user_txn() {
                        txn_by_account_batch
                            .put::<TransactionByAccountSchema>(
                                &(txn.sender(), txn.sequence_number()),
                                &version,
                            )
                            .expect("Failed to put txn to db tailer batch");

                        events.iter().enumerate().for_each(|(idx, event)| {
                            if let ContractEvent::V1(v1) = event {
                                event_by_key_batch
                                    .put::<EventByKeySchema>(
                                        &(*v1.key(), v1.sequence_number()),
                                        &(version, idx as u64),
                                    )
                                    .expect("Failed to event by key to db tailer batch");
                                event_by_version_batch
                                    .put::<EventByVersionSchema>(
                                        &(*v1.key(), version, v1.sequence_number()),
                                        &(idx as u64),
                                    )
                                    .expect("Failed to event by version to db tailer batch");
                            }
                        });
                    }
                    version += 1;
                })
                .expect("Failed to iterate db tailer iterator");
            });
            // write to index db

            // update the metadata

            // update the last version
        }
    }

    pub fn get_last_version(&self) -> Version {
        self.last_version
    }
}
