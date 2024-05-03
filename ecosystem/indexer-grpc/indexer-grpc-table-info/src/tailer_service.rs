// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_config::config::NodeConfig;
use aptos_db_indexer::{db_ops::open_db, db_tailer::DBTailer};
use aptos_indexer_grpc_utils::counters::{log_grpc_step, IndexerGrpcStep};
use aptos_storage_interface::DbReader;
use std::sync::Arc;

const SERVICE_TYPE: &str = "db_tailer_service";
const INDEX_ASYNC_DB_TAILER: &str = "index_async_db_tailer";

pub struct TailerService {
    pub db_tailer: Arc<DBTailer>,
}

impl TailerService {
    pub fn new(db_reader: Arc<dyn DbReader>, node_config: &NodeConfig) -> Self {
        let db_path = node_config
            .storage
            .get_dir_paths()
            .default_root_path()
            .join(INDEX_ASYNC_DB_TAILER);
        let rocksdb_config = node_config.storage.rocksdb_configs.index_db_config;
        let db = open_db(db_path, &rocksdb_config)
            .expect("Failed to open up indexer db tailer initially");

        let indexer_db_tailer =
            Arc::new(DBTailer::new(db, db_reader, &node_config.index_db_tailer));
        Self {
            db_tailer: indexer_db_tailer,
        }
    }

    pub async fn run(&mut self) {
        loop {
            let start_time: std::time::Instant = std::time::Instant::now();
            self.db_tailer
                .run()
                .expect("Failed to run indexer db tailer");
            let versions_processed = self.db_tailer.get_last_version();
            log_grpc_step(
                SERVICE_TYPE,
                IndexerGrpcStep::DBTailerProcessed,
                None,
                None,
                None,
                None,
                Some(start_time.elapsed().as_secs_f64()),
                None,
                Some(versions_processed as i64),
                None,
            );
        }
    }
}
