//! The tools (plan M1.6 Task 7 rules 6–11; Task 8 adds `search`, `sql`
//! and `memory_write`). Inputs refuse unknown keys; no tool declares an
//! output schema (Ruling 8).

use operon_collection::{ConsistencyToken, Distance, FieldKind, PrimaryKey};
use operon_query::{CollectionInfo, Projection, ReadConsistency, SourceFilter};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Extensions};
use rmcp::{ErrorData, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::ToolError;
use crate::ids::DocId;
use crate::output::{cap_items, tool_result_json};
use crate::server::OperonMcp;

/// The header that selects the namespace (overview §6.9).
pub const NAMESPACE_HEADER: &str = "operon-namespace";

/// `list_collections` takes no arguments.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListCollectionsInput {}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListCollectionsOutput {
    pub collections: Vec<CollectionOut>,
    /// Trailing collections were dropped to fit the output cap.
    pub truncated: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct CollectionOut {
    pub name: String,
    pub fields: Vec<FieldOut>,
    pub vectors: Vec<VectorOut>,
    pub live_doc_count: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FieldOut {
    pub name: String,
    /// `text`, `keyword`, `i64`, `f64`, `bool`, `date`, `uuid` or `json`.
    pub kind: &'static str,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct VectorOut {
    pub name: String,
    pub dim: u32,
    /// `cosine`, `dot`, `euclid` or `manhattan`.
    pub distance: &'static str,
}

/// `get_documents`' arguments.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetDocumentsInput {
    /// The collection (or alias) to read.
    pub collection: String,
    /// The ids to fetch: numbers, strings, or {"uuid": "…"}.
    pub ids: Vec<DocId>,
    /// Source paths to return; all of the source when absent.
    pub select: Option<Vec<String>>,
    /// Read at least the writes of this consistency token.
    pub consistency_token: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GetDocumentsOutput {
    pub documents: Vec<DocumentOut>,
    pub truncated: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DocumentOut {
    pub id: DocId,
    pub found: bool,
    pub source: Option<Map<String, Value>>,
}

/// The tool result of `result` (rule 7).
fn respond<T: Serialize>(result: Result<T, ToolError>) -> Result<CallToolResult, ErrorData> {
    match result {
        Ok(output) => match serde_json::to_value(output) {
            Ok(value) => Ok(CallToolResult::structured(value)),
            Err(err) => Ok(ToolError::new("internal", err.to_string()).into_result()),
        },
        Err(err) => Ok(err.into_result()),
    }
}

fn field_kind(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text { .. } => "text",
        FieldKind::Keyword => "keyword",
        FieldKind::I64 => "i64",
        FieldKind::F64 => "f64",
        FieldKind::Bool => "bool",
        FieldKind::Date => "date",
        FieldKind::Uuid => "uuid",
        FieldKind::Json => "json",
    }
}

fn distance(distance: Distance) -> &'static str {
    match distance {
        Distance::Cosine => "cosine",
        Distance::Dot => "dot",
        Distance::Euclid => "euclid",
        Distance::Manhattan => "manhattan",
    }
}

fn collection_out(info: CollectionInfo) -> CollectionOut {
    CollectionOut {
        fields: info
            .schema
            .fields
            .iter()
            .map(|f| FieldOut {
                name: f.name.clone(),
                kind: field_kind(&f.kind),
            })
            .collect(),
        vectors: info
            .schema
            .vectors
            .iter()
            .map(|v| VectorOut {
                name: v.name.clone(),
                dim: v.dim,
                distance: distance(v.distance),
            })
            .collect(),
        name: info.name,
        live_doc_count: info.live_doc_count,
    }
}

/// Rule 10: an absent token reads `Strong`.
pub(crate) fn read_consistency(token: Option<&str>) -> Result<ReadConsistency, ToolError> {
    match token {
        None => Ok(ReadConsistency::Strong),
        Some(text) => text
            .parse::<ConsistencyToken>()
            .map(ReadConsistency::AtLeast)
            .map_err(|err| ToolError::invalid(format!("consistency_token: {err}"))),
    }
}

impl OperonMcp {
    /// Rule 6: the `Operon-Namespace` header, else the configured namespace.
    pub(crate) fn namespace(&self, extensions: &Extensions) -> Result<String, ToolError> {
        let header = extensions
            .get::<http::request::Parts>()
            .and_then(|parts| parts.headers.get(NAMESPACE_HEADER));
        let Some(value) = header else {
            return Ok(self.config.namespace.clone());
        };
        match std::str::from_utf8(value.as_bytes()) {
            Ok(ns) if (1..=255).contains(&ns.len()) => Ok(ns.to_string()),
            _ => Err(ToolError::invalid(
                "Operon-Namespace header must be valid UTF-8 of 1 to 255 bytes",
            )),
        }
    }

    async fn run_list_collections(
        &self,
        extensions: &Extensions,
    ) -> Result<ListCollectionsOutput, ToolError> {
        let ns = self.namespace(extensions)?;
        let mut infos = match self.collections.list_collections(&ns).await {
            Ok(infos) => infos,
            Err(operon_query::ServiceError::NotFound {
                kind: "namespace", ..
            }) => Vec::new(),
            Err(err) => return Err(err.into()),
        };
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        let mut collections: Vec<CollectionOut> = infos.into_iter().map(collection_out).collect();
        let truncated = cap_items(
            &mut collections,
            self.config.max_output_bytes,
            |collections| {
                tool_result_json(
                    serde_json::json!({ "collections": collections, "truncated": true }),
                )
            },
        );
        Ok(ListCollectionsOutput {
            collections,
            truncated,
        })
    }

    async fn run_get_documents(
        &self,
        input: GetDocumentsInput,
        extensions: &Extensions,
    ) -> Result<GetDocumentsOutput, ToolError> {
        let max = self.config.get_max_ids;
        if input.ids.is_empty() || input.ids.len() > max {
            return Err(ToolError::invalid(format!(
                "ids must hold 1 to {max} ids, got {}",
                input.ids.len()
            )));
        }
        let ns = self.namespace(extensions)?;
        let consistency = read_consistency(input.consistency_token.as_deref())?;
        let pks = input
            .ids
            .into_iter()
            .map(PrimaryKey::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let select = Projection {
            source: match input.select {
                Some(include) => SourceFilter::Paths {
                    include,
                    exclude: Vec::new(),
                },
                None => SourceFilter::All,
            },
            vectors: Vec::new(),
            fields: Vec::new(),
        };
        let docs = self
            .collections
            .get(&ns, &input.collection, &pks, &select, consistency)
            .await?;
        let mut documents: Vec<DocumentOut> = pks
            .iter()
            .zip(docs)
            .map(|(pk, doc)| DocumentOut {
                id: DocId::from(pk),
                found: doc.is_some(),
                source: doc.and_then(|doc| doc.source),
            })
            .collect();
        let truncated = cap_items(&mut documents, self.config.max_output_bytes, |docs| {
            tool_result_json(serde_json::json!({ "documents": docs, "truncated": true }))
        });
        Ok(GetDocumentsOutput {
            documents,
            truncated,
        })
    }
}

#[tool_router(vis = "pub(crate)")]
impl OperonMcp {
    #[tool(
        name = "list_collections",
        description = "List the namespace's collections with their fields, vectors and document counts.",
        annotations(
            title = "List collections",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn list_collections(
        &self,
        Parameters(_input): Parameters<ListCollectionsInput>,
        extensions: Extensions,
    ) -> Result<CallToolResult, ErrorData> {
        respond(self.run_list_collections(&extensions).await)
    }

    #[tool(
        name = "get_documents",
        description = "Fetch documents by id from a collection. Ids are numbers, strings, or {\"uuid\": \"…\"}. Missing ids come back with found: false.",
        annotations(
            title = "Get documents",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn get_documents(
        &self,
        Parameters(input): Parameters<GetDocumentsInput>,
        extensions: Extensions,
    ) -> Result<CallToolResult, ErrorData> {
        respond(self.run_get_documents(input, &extensions).await)
    }
}
