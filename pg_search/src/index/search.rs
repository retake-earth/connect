// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use super::reader::index::SearchIndexReader;
use super::writer::index::IndexError;
use crate::gucs;
use crate::index::blocking::BlockingDirectory;
use crate::index::channel::{ChannelDirectory, ChannelRequestHandler};
use crate::index::writer::index::SearchIndexWriter;
use crate::postgres::index::get_fields;
use crate::postgres::options::SearchIndexCreateOptions;
use crate::postgres::storage::block::METADATA_BLOCKNO;
use crate::postgres::storage::utils::BM25BufferCache;
use crate::query::SearchQueryInput;
use crate::schema::{
    SearchDocument, SearchField, SearchFieldConfig, SearchFieldName, SearchFieldType,
    SearchIndexSchema, SearchIndexSchemaError,
};
use anyhow::Result;
use once_cell::sync::Lazy;
use pgrx::{pg_sys, PgRelation};
use serde::Serialize;
use std::num::NonZeroUsize;
use tantivy::directory::DirectoryClone;
use tantivy::query::Query;
use tantivy::schema::Schema;
use tantivy::{query::QueryParser, Directory, Executor, Index, IndexSettings, ReloadPolicy};
use thiserror::Error;
use tokenizers::{create_normalizer_manager, create_tokenizer_manager};
use tracing::trace;
use url::quirks::search;

/// PostgreSQL operates in a process-per-client model, meaning every client connection
/// to PostgreSQL results in a new backend process being spawned on the PostgreSQL server.
pub static mut SEARCH_EXECUTOR: Lazy<Executor> = Lazy::new(Executor::single_thread);

pub enum WriterResources {
    CreateIndex,
    Statement,
    Vacuum,
}
pub type Parallelism = NonZeroUsize;
pub type MemoryBudget = usize;
pub type TargetSegmentCount = usize;
pub type DoMerging = bool;

impl WriterResources {
    pub fn resources(
        &self,
        index_options: &SearchIndexCreateOptions,
    ) -> (Parallelism, MemoryBudget, TargetSegmentCount, DoMerging) {
        match self {
            WriterResources::CreateIndex => (
                gucs::create_index_parallelism(),
                gucs::create_index_memory_budget(),
                index_options.target_segment_count(),
                true, // we always want a merge on CREATE INDEX
            ),
            WriterResources::Statement => (
                gucs::statement_parallelism(),
                gucs::statement_memory_budget(),
                index_options.target_segment_count(),
                index_options.merge_on_insert(), // user/index decides if we merge for INSERT/UPDATE statements
            ),
            WriterResources::Vacuum => (
                gucs::statement_parallelism(),
                gucs::statement_memory_budget(),
                index_options.target_segment_count(),
                true, // we always want a merge on (auto)VACUUM
            ),
        }
    }
}

// #[derive(Serialize)]
struct SearchIndex {
    schema: SearchIndexSchema,
    index_oid: pg_sys::Oid,
    underlying_index: Index,
    handler: ChannelRequestHandler,
}

impl SearchIndex {
    fn create_index(
        index_relation: &PgRelation,
        resources: WriterResources,
    ) -> Result<SearchIndexWriter> {
        let schema = make_schema(index_relation)?;
        let create_options = index_relation.rd_options as *mut SearchIndexCreateOptions;

        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..IndexSettings::default()
        };
        let search_index = Self::prepare_index(index_relation, schema, |directory, schema| {
            Index::create(directory, schema.schema.clone(), settings)
        })?;

        SearchIndexWriter::new(
            search_index.underlying_index,
            search_index.schema,
            search_index.handler,
            resources,
            unsafe { &*create_options },
        )
    }

    fn open_writer(
        index_relation: &PgRelation,
        resources: WriterResources,
    ) -> Result<SearchIndexWriter> {
        let schema = make_schema(index_relation)?;
        let create_options = index_relation.rd_options as *mut SearchIndexCreateOptions;

        let search_index = Self::prepare_index(index_relation, schema, |directory, _| {
            Index::open(directory)
        })?;

        SearchIndexWriter::new(
            search_index.underlying_index,
            search_index.schema,
            search_index.handler,
            resources,
            unsafe { &*create_options },
        )
    }

    fn prepare_index<F: FnOnce(Box<dyn Directory>, &SearchIndexSchema) -> tantivy::Result<Index>>(
        index_relation: &PgRelation,
        schema: SearchIndexSchema,
        opener: F,
    ) -> Result<Self, SearchIndexError>
    where
        F: Send + Sync,
    {
        let index_oid = index_relation.oid();
        let cache = unsafe { BM25BufferCache::open(index_oid) };
        let lock =
            unsafe { cache.get_buffer(METADATA_BLOCKNO, Some(pgrx::pg_sys::BUFFER_LOCK_SHARE)) };

        let (req_sender, req_receiver) = crossbeam::channel::bounded(1);
        let (resp_sender, resp_receiver) = crossbeam::channel::bounded(1);
        let tantivy_dir = BlockingDirectory::new(index_oid);
        let channel_dir = ChannelDirectory::new(req_sender, resp_receiver);
        let mut handler =
            ChannelRequestHandler::open(tantivy_dir, index_oid, resp_sender, req_receiver);

        let underlying_index = handler
            .wait_for(|| {
                let mut index = opener(channel_dir.box_clone(), &schema)?;
                SearchIndex::setup_tokenizers(&mut index, &schema);
                tantivy::Result::Ok(index)
            })
            .expect("scoped thread should not fail")?;

        unsafe { pg_sys::UnlockReleaseBuffer(lock) };

        Ok(SearchIndex {
            schema,
            underlying_index,
            index_oid,
            handler,
        })
    }

    pub fn perform<T, F: FnOnce(&Index) -> T>(&mut self, action: F) -> std::thread::Result<T>
    where
        F: Send + Sync,
        T: Send + Sync,
    {
        self.handler.wait_for(|| action(&self.underlying_index))
    }

    fn open_reader(index_relation: &PgRelation) -> Result<SearchIndexReader> {
        let directory = BlockingDirectory::new(index_relation.oid());
        let mut index = Index::open(directory)?;
        let schema = make_schema(index_relation)?;
        SearchIndex::setup_tokenizers(&mut index, &schema);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();

        Ok(SearchIndexReader::new(
            index_relation,
            index,
            searcher,
            reader,
            schema,
        ))
    }

    #[allow(static_mut_refs)]
    fn executor() -> &'static Executor {
        unsafe { &SEARCH_EXECUTOR }
    }

    fn setup_tokenizers(underlying_index: &mut Index, schema: &SearchIndexSchema) {
        let tokenizers = schema
            .fields
            .iter()
            .filter_map(|field| {
                let field_config = &field.config;
                let field_name: &str = field.name.as_ref();
                trace!(field_name, "attempting to create tokenizer");
                match field_config {
                    SearchFieldConfig::Text { tokenizer, .. }
                    | SearchFieldConfig::Json { tokenizer, .. } => Some(tokenizer),
                    _ => None,
                }
            })
            .collect();

        underlying_index.set_tokenizers(create_tokenizer_manager(tokenizers));
        underlying_index.set_fast_field_tokenizers(create_normalizer_manager());
    }

    fn key_field(&self) -> SearchField {
        self.schema.key_field()
    }

    fn key_field_name(&self) -> String {
        self.key_field().name.to_string()
    }
}

#[derive(Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum SearchIndexError {
    #[error(transparent)]
    SchemaError(#[from] SearchIndexSchemaError),

    #[error(transparent)]
    WriterIndexError(#[from] IndexError),

    #[error(transparent)]
    TantivyError(#[from] tantivy::error::TantivyError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),

    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
}

fn make_schema(index_relation: &PgRelation) -> Result<SearchIndexSchema> {
    if index_relation.rd_options.is_null() {
        panic!("must specify key field")
    }
    let (fields, key_field_index) = unsafe { get_fields(index_relation) };
    let schema = SearchIndexSchema::new(fields, key_field_index)?;
    Ok(schema)
}

/// Open a (non-channel-based) [`SearchIndexReader`] for the specified Postgres index relation
pub fn open_search_reader(index_relation: &PgRelation) -> Result<SearchIndexReader> {
    SearchIndex::open_reader(index_relation)
}

/// Open an existing index for writing
pub fn open_search_writer(
    index_relation: &PgRelation,
    resources: WriterResources,
) -> Result<SearchIndexWriter> {
    SearchIndex::open_writer(index_relation, resources)
}

/// Create a new, empty index for the specified Postgres index relation
pub fn create_new_index(
    index_relation: &PgRelation,
    resources: WriterResources,
) -> Result<SearchIndexWriter> {
    SearchIndex::create_index(index_relation, resources)
}
