// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct IndexDBTailerConfig {
    pub enable: bool,
    pub batch_size: usize,
}

impl Default for IndexDBTailerConfig {
    fn default() -> Self {
        Self {
            enable: false,
            batch_size: 10_000,
        }
    }
}
