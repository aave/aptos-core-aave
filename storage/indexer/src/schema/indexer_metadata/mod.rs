// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! This module defines physical storage schema storing metadata for the internal indexer
//!

use super::TAILER_METADATA_CF_NAME;
use crate::{
    metadata::{MetadataKey, MetadataValue},
    schema::INDEXER_METADATA_CF_NAME,
    utils::ensure_slice_len_eq,
};
use anyhow::Result;
use aptos_schemadb::{
    define_schema,
    schema::{KeyCodec, ValueCodec},
};
use aptos_types::transaction::Version;

define_schema!(
    IndexerMetadataSchema,
    MetadataKey,
    MetadataValue,
    INDEXER_METADATA_CF_NAME
);

impl KeyCodec<IndexerMetadataSchema> for MetadataKey {
    fn encode_key(&self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(self)?)
    }

    fn decode_key(data: &[u8]) -> Result<Self> {
        Ok(bcs::from_bytes(data)?)
    }
}

impl ValueCodec<IndexerMetadataSchema> for MetadataValue {
    fn encode_value(&self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(self)?)
    }

    fn decode_value(data: &[u8]) -> Result<Self> {
        Ok(bcs::from_bytes(data)?)
    }
}

define_schema!(TailerMetadataSchema, Version, (), TAILER_METADATA_CF_NAME);

impl KeyCodec<TailerMetadataSchema> for Version {
    fn encode_key(&self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(self)?)
    }

    fn decode_key(data: &[u8]) -> Result<Self> {
        Ok(bcs::from_bytes(data)?)
    }
}

impl ValueCodec<TailerMetadataSchema> for () {
    fn encode_value(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn decode_value(data: &[u8]) -> Result<Self> {
        ensure_slice_len_eq(data, 0)?;
        Ok(())
    }
}

#[cfg(test)]
mod test;
