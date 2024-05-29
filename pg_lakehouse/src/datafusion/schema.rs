use async_std::sync::Mutex;
use async_trait::async_trait;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::exec_err;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use deltalake::DeltaTable;
use pgrx::*;
use std::any::Any;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;
use supabase_wrappers::prelude::*;

use crate::fdw::handler::*;
use crate::fdw::options::*;
use crate::schema::attribute::*;

use super::catalog::CatalogError;
use super::format::*;
use super::provider::*;

#[derive(Clone)]
pub struct LakehouseSchemaProvider {
    schema_name: String,
    #[allow(unused)]
    tables: Arc<Mutex<HashMap<pg_sys::Oid, Arc<dyn TableProvider + Send + Sync>>>>,
}

impl LakehouseSchemaProvider {
    pub fn new(schema_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[allow(unused)]
    async fn table_impl(
        &self,
        table_name: &str,
    ) -> Result<Arc<dyn TableProvider + Send + Sync>, CatalogError> {
        let pg_relation = unsafe {
            PgRelation::open_with_name(table_name).unwrap_or_else(|err| {
                panic!("{}", err);
            })
        };

        let table_options = pg_relation.table_options()?;
        let path = require_option(TableOption::Path.as_str(), &table_options)?;
        let extension = require_option(TableOption::Extension.as_str(), &table_options)?;
        let format = require_option_or(TableOption::Format.as_str(), &table_options, "");
        let mut tables = self.tables.lock().await;

        let table: Arc<dyn TableProvider + Send + Sync> = match tables.entry(pg_relation.oid()) {
            Occupied(entry) => entry.into_mut().to_owned(),
            Vacant(entry) => {
                let mut attribute_map: HashMap<usize, PgAttribute> = pg_relation
                    .tuple_desc()
                    .iter()
                    .enumerate()
                    .map(|(index, attribute)| {
                        (
                            index,
                            PgAttribute::new(attribute.name(), attribute.atttypid),
                        )
                    })
                    .collect();

                let provider = match TableFormat::from(format) {
                    TableFormat::None => create_listing_provider(path, extension).await?,
                    TableFormat::Delta => create_delta_provider(path, extension).await?,
                };

                for (index, field) in provider.schema().fields().iter().enumerate() {
                    if let Some(attribute) = attribute_map.remove(&index) {
                        can_convert_to_attribute(field, attribute)?;
                    }
                }

                entry.insert(provider).to_owned()
            }
        };

        let provider = match TableFormat::from(format) {
            TableFormat::Delta => {
                let mut delta_table = table
                    .as_any()
                    .downcast_ref::<DeltaTable>()
                    .ok_or(CatalogError::DowncastDeltaTable)?
                    .clone();
                delta_table.load().await?;
                Arc::new(delta_table) as Arc<dyn TableProvider + Send + Sync>
            }
            _ => table.clone(),
        };

        Ok(provider)
    }
}

#[async_trait]
impl SchemaProvider for LakehouseSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // This function never gets called anywhere, so it's safe to leave unimplemented
    fn table_names(&self) -> Vec<String> {
        todo!("table_names not implemented")
    }

    fn table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table_name: &'life1 str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<Arc<dyn TableProvider>>, DataFusionError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            let table = self
                .table_impl(table_name)
                .unwrap_or_else(|err| panic!("{}", err));


    }

    fn table_exist(&self, table_name: &str) -> bool {
        let pg_relation = match unsafe {
            PgRelation::open_with_name(format!("{}.{}", self.schema_name, table_name).as_str())
        } {
            Ok(relation) => relation,
            Err(_) => return false,
        };

        if !pg_relation.is_foreign_table() {
            return false;
        }

        let foreign_table = unsafe { pg_sys::GetForeignTable(pg_relation.oid()) };
        let foreign_server = unsafe { pg_sys::GetForeignServer((*foreign_table).serverid) };
        let fdw_handler = FdwHandler::from(foreign_server);

        fdw_handler != FdwHandler::Other
    }

    #[doc = r" Returns the owner of the Schema, default is None. This value is reported"]
    #[doc = r" as part of `information_tables.schemata"]
    fn owner_name(&self) -> Option<&str> {
        None
    }

    #[doc = r" If supported by the implementation, adds a new table named `name` to"]
    #[doc = r" this schema."]
    #[doc = r""]
    #[doc = r#" If a table of the same name was already registered, returns "Table"#]
    #[doc = r#" already exists" error."#]
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    #[doc = r" If supported by the implementation, removes the `name` table from this"]
    #[doc = r" schema and returns the previously registered [`TableProvider`], if any."]
    #[doc = r""]
    #[doc = r" If no `name` table exists, returns Ok(None)."]
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }
}
