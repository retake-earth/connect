use anyhow::{anyhow, Result};
use duckdb::arrow::array::RecordBatch;
use pgrx::*;
use std::collections::HashMap;
use supabase_wrappers::prelude::*;
use thiserror::Error;

use super::handler::FdwHandler;
use crate::duckdb::connection;
use crate::duckdb::csv::create_csv_view;
use crate::duckdb::delta::create_delta_view;
use crate::duckdb::parquet::create_parquet_view;
use crate::schema::cell::*;

const DEFAULT_SECRET: &str = "default_secret";

pub trait BaseFdw {
    // Getter methods
    fn get_current_batch(&self) -> Option<RecordBatch>;
    fn get_current_batch_index(&self) -> usize;
    fn get_scan_started(&self) -> bool;
    fn get_sql(&self) -> Option<String>;
    fn get_target_columns(&self) -> Vec<Column>;
    fn get_user_mapping_options(&self) -> HashMap<String, String>;

    // Setter methods
    fn set_current_batch(&mut self, batch: Option<RecordBatch>);
    fn set_current_batch_index(&mut self, idx: usize);
    fn set_scan_started(&mut self);
    fn set_sql(&mut self, statement: Option<String>);
    fn set_target_columns(&mut self, columns: &[Column]);

    async fn begin_scan_impl(
        &mut self,
        // TODO: Push down quals
        _quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let oid_u32: u32 = options
            .get(OPTS_TABLE_KEY)
            .ok_or_else(|| anyhow!("table oid not found"))?
            .parse()?;
        let table_oid = pg_sys::Oid::from(oid_u32);
        let pg_relation = unsafe { PgRelation::open(table_oid) };
        let schema_name = pg_relation.namespace();
        let table_name = pg_relation.name();

        // Cache target columns
        self.set_target_columns(columns);

        // Create DuckDB secret from user mapping options
        connection::create_secret(DEFAULT_SECRET, self.get_user_mapping_options())?;

        // Create DuckDB view
        if !connection::view_exists(table_name, schema_name)? {
            // Create schema if it does not exist
            connection::execute(
                format!("CREATE SCHEMA IF NOT EXISTS {schema_name}").as_str(),
                [],
            )?;

            let foreign_table = unsafe { pg_sys::GetForeignTable(pg_relation.oid()) };
            let foreign_server = unsafe { pg_sys::GetForeignServer((*foreign_table).serverid) };
            let table_options = unsafe { options_to_hashmap((*foreign_table).options)? };

            match FdwHandler::from(foreign_server) {
                FdwHandler::Csv => {
                    create_csv_view(table_name, schema_name, table_options)?;
                }
                FdwHandler::Delta => {
                    create_delta_view(table_name, schema_name, table_options)?;
                }
                FdwHandler::Parquet => {
                    create_parquet_view(table_name, schema_name, table_options)?;
                }
                _ => {
                    todo!()
                }
            }
        }

        // Ensure we are in the same DuckDB schema as the Postgres schema
        connection::execute(format!("SET SCHEMA '{schema_name}'").as_str(), [])?;

        // Construct SQL scan statement
        let targets = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        };

        let mut sql = format!("SELECT {targets} FROM {schema_name}.{table_name}");

        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| sort.deparse())
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" ORDER BY {}", order_by));
        }

        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" LIMIT {}", real_limit));
        }

        self.set_sql(Some(sql));
        Ok(())
    }

    async fn iter_scan_impl(&mut self, row: &mut Row) -> Result<Option<()>> {
        if !self.get_scan_started() {
            self.set_scan_started();
            let sql = self
                .get_sql()
                .ok_or_else(|| anyhow!("sql statement was not cached"))?;
            connection::create_arrow(sql.as_str())?;
        }

        if self.get_current_batch().is_none()
            || self.get_current_batch_index()
                >= self
                    .get_current_batch()
                    .as_ref()
                    .ok_or_else(|| anyhow!("current batch not found"))?
                    .num_rows()
        {
            self.set_current_batch_index(0);
            let next_batch = connection::get_next_batch()?;

            if next_batch.is_none() {
                return Ok(None);
            }

            self.set_current_batch(next_batch);
        }

        let current_batch_binding = self.get_current_batch();
        let current_batch = current_batch_binding
            .as_ref()
            .ok_or_else(|| anyhow!("current batch not found"))?;
        let current_batch_index = self.get_current_batch_index();

        for (column_index, target_column) in
            self.get_target_columns().clone().into_iter().enumerate()
        {
            let batch_column = current_batch.column(column_index);
            let cell = batch_column.get_cell(
                current_batch_index,
                target_column.type_oid,
                target_column.name.as_str(),
            )?;
            row.push(target_column.name.as_str(), cell);
        }

        self.set_current_batch_index(current_batch_index + 1);

        Ok(Some(()))
    }

    fn end_scan_impl(&mut self) {
        connection::clear_arrow();
    }

    fn explain_impl(&self) -> Result<Option<Vec<(String, String)>>> {
        let sql = self
            .get_sql()
            .ok_or_else(|| anyhow!("sql statement was not cached"))?;
        Ok(Some(vec![("DuckDB Scan".to_string(), sql)]))
    }
}

impl From<BaseFdwError> for pg_sys::panic::ErrorReport {
    fn from(value: BaseFdwError) -> Self {
        pg_sys::panic::ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{}", value), "")
    }
}

pub fn validate_options(opt_list: Vec<Option<String>>, valid_options: Vec<String>) -> Result<()> {
    for opt in opt_list
        .iter()
        .flatten()
        .map(|opt| opt.split('=').next().unwrap_or(""))
    {
        if !valid_options.contains(&opt.to_string()) {
            return Err(anyhow!(
                "invalid option: {}. valid options are: {}",
                opt,
                valid_options.join(", ")
            ));
        }
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum BaseFdwError {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    #[error(transparent)]
    Options(#[from] OptionsError),
}
