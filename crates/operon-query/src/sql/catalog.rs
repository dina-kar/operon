//! The DataFusion catalog of one namespace (plan M1.2 Task 10 rule 1): the
//! catalog `"<ns>"` with one schema, `collections`, whose tables are the
//! namespace's collections and aliases.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;

use crate::error::ServiceError;
use crate::exec::df_error;
use crate::sql::provider::CollectionProvider;
use crate::sql::{COLLECTIONS_SCHEMA, SqlScope};

/// The catalog of one namespace: one schema, [`COLLECTIONS_SCHEMA`].
#[derive(Debug)]
pub struct NamespaceCatalog {
    scope: SqlScope,
    schema: Arc<CollectionsSchema>,
}

impl NamespaceCatalog {
    pub(crate) fn new(scope: SqlScope) -> Self {
        Self {
            schema: Arc::new(CollectionsSchema {
                scope: scope.clone(),
            }),
            scope,
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
        vec![COLLECTIONS_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        (name == COLLECTIONS_SCHEMA).then(|| self.schema.clone() as Arc<dyn SchemaProvider>)
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
