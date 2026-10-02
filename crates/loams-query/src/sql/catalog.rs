//! The DataFusion catalog of one namespace (plan M1.2 Task 10 rule 1): the
//! catalog `"<ns>"` with one schema, `collections`, whose tables are the
//! namespace's collections and aliases. A wire front end may register more
//! schemas beside it (the PostgreSQL listener's `pg_catalog`, PG1); none can
//! replace `collections`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{PoisonError, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;

use crate::error::ServiceError;
use crate::exec::df_error;
use crate::sql::provider::CollectionProvider;
use crate::sql::{COLLECTIONS_SCHEMA, SqlScope};

/// The catalog of one namespace: [`COLLECTIONS_SCHEMA`], plus any schema a
/// wire front end registers.
#[derive(Debug)]
pub struct NamespaceCatalog {
    scope: SqlScope,
    schema: Arc<CollectionsSchema>,
    /// Schemas registered beside `collections`, such as `pg_catalog`.
    additional_schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl NamespaceCatalog {
    pub(crate) fn new(scope: SqlScope) -> Self {
        Self {
            schema: Arc::new(CollectionsSchema {
                scope: scope.clone(),
            }),
            scope,
            additional_schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Re-reads the namespace's names and collections into the catalog
    /// cache, so the search table functions plan against the current
    /// catalog.
    pub(crate) async fn refresh(&self) -> Result<(), ServiceError> {
        self.scope
            .service
            .catalog()
            .refresh(&self.scope.ns)
            .await
            .map_err(ServiceError::from)
    }
}

impl CatalogProvider for NamespaceCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut names = vec![COLLECTIONS_SCHEMA.to_string()];
        names.extend(
            self.additional_schemas
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .keys()
                .cloned(),
        );
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == COLLECTIONS_SCHEMA {
            return Some(self.schema.clone());
        }
        self.additional_schemas
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>, DataFusionError> {
        if name == COLLECTIONS_SCHEMA {
            return Err(DataFusionError::Configuration(
                "the collections schema cannot be replaced".to_string(),
            ));
        }
        Ok(self
            .additional_schemas
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_string(), schema))
    }
}

/// The `collections` schema of a namespace: a table per collection and per
/// alias, resolved when a statement plans.
#[derive(Debug)]
pub struct CollectionsSchema {
    scope: SqlScope,
}

#[async_trait]
impl SchemaProvider for CollectionsSchema {
    fn table_names(&self) -> Vec<String> {
        self.scope.service.catalog().names(&self.scope.ns)
    }

    /// A fresh `resolve_collection(Local, …)`: a collection created after
    /// the context is found.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self.scope.service.resolve(&self.scope.ns, name).await {
            Ok((ns_id, collection)) => Ok(Some(Arc::new(CollectionProvider::new(
                self.scope.clone(),
                ns_id,
                collection,
            )))),
            Err(ServiceError::NotFound { .. }) => Ok(None),
            Err(err) => Err(df_error(err)),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().iter().any(|table| table == name)
    }
}
