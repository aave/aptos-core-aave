// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_storage_interface::{db_ensure as ensure, AptosDbError, Result};

pub fn ensure_slice_len_eq(data: &[u8], len: usize) -> Result<()> {
    ensure!(
        data.len() == len,
        "Unexpected data len {}, expected {}.",
        data.len(),
        len,
    );
    Ok(())
}
