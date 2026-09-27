//! The tools (plan M1.6 Task 7 rules 6–11, Task 8 rules 1–4): `search`,
//! `sql`, `memory_write`, `list_collections` and `get_documents`. Inputs
//! refuse unknown keys; no tool declares an output schema (Ruling 8).

use std::collections::BTreeMap;
use std::time::SystemTime;

use operon_collection::{ConsistencyToken, Distance, DocOp, Document, FieldKind, PrimaryKey};
use operon_query::{
    AnnParams, BoolOperator, CollectionInfo, Fusion, MultiMatchKind, Projection, Query,
    ReadConsistency, Retriever, SearchRequest, ServiceError, SourceFilter, TrackTotalHits,
    WriteOptions,
};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Extensions};
use rmcp::{ErrorData, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::ToolError;
use crate::filter::translate_filter;
use crate::ids::DocId;
use crate::output::{cap_items, tool_result_json};
use crate::schema::{MEMORY_VECTOR, is_memory_collection, memory_schema, memory_vector};
use crate::server::OperonMcp;
use crate::sql::{SqlLimits, run_read_only};

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

/// `search`'s arguments (Task 8 rule 1).
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchInput {
    /// The collection (or alias) to search.
    pub collection: String,
    /// Full-text query (BM25) over `fields`.
    pub query: Option<String>,
    /// Text fields to search; every text field when absent.
    pub fields: Option<Vec<String>>,
    /// A query vector from the same model as the collection's vectors.
    pub vector: Option<Vec<f32>>,
    /// The vector field; needed only when the collection has several.
    pub vector_field: Option<String>,
    /// Field conditions that must all hold: a value, a list (any of),
    /// {"gt"|"gte"|"lt"|"lte": …}, {"exists": bool}, or null (missing).
    pub filter: Option<Map<String, Value>>,
    /// How many hits to return (default 10).
    pub limit: Option<usize>,
    /// Source paths to return; all of the source when absent.
    pub select: Option<Vec<String>>,
    /// Read at least the writes of this consistency token.
    pub consistency_token: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SearchOutput {
    pub hits: Vec<HitOut>,
    pub read_token: String,
    pub truncated: bool,
    /// What the search cost the server: the native response's block
    /// (M1.6 Task 10, D92).
    #[schemars(with = "Map<String, Value>")]
    pub performance: operon_query::perf::Performance,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct HitOut {
    pub id: DocId,
    pub score: f32,
    pub source: Option<Map<String, Value>>,
}

/// `sql`'s arguments (Task 8 rule 3).
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqlInput {
    /// One read-only statement (Apache DataFusion dialect).
    pub query: String,
    /// The most rows to return (default and ceiling: the server's cap).
    pub max_rows: Option<usize>,
}

/// `memory_write`'s arguments (Task 8 rule 4).
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryWriteInput {
    /// The memory's text.
    pub text: String,
    /// The memory collection (default `memories`).
    pub collection: Option<String>,
    /// The memory's id; writing again with the same id replaces it. A new
    /// ULID when absent.
    pub id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub author: Option<String>,
    pub metadata: Option<Map<String, Value>>,
    /// An embedding of `text`; its length fixes the collection's dimension.
    pub vector: Option<Vec<f32>>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MemoryWriteOutput {
    pub id: String,
    pub collection: String,
    pub created_collection: bool,
    pub consistency_token: String,
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

/// `select` as a projection: the listed source paths, else all of it; no
/// vectors and no fields.
fn projection(select: Option<Vec<String>>) -> Projection {
    Projection {
        source: match select {
            Some(include) => SourceFilter::Paths {
                include,
                exclude: Vec::new(),
            },
            None => SourceFilter::All,
        },
        vectors: Vec::new(),
        fields: Vec::new(),
    }
}

/// The text retriever's query over `fields` of `info` (rule 1.3).
fn text_query(
    info: &CollectionInfo,
    fields: Option<Vec<String>>,
    text: &str,
) -> Result<Query, ToolError> {
    let text_fields: Vec<&str> = info
        .schema
        .fields
        .iter()
        .filter(|f| matches!(f.kind, FieldKind::Text { .. }))
        .map(|f| f.name.as_str())
        .collect();
    let fields = match fields {
        Some(fields) if fields.is_empty() => {
            return Err(ToolError::invalid("fields must not be empty"));
        }
        Some(fields) => {
            if let Some(bad) = fields.iter().find(|f| !text_fields.contains(&f.as_str())) {
                return Err(ToolError::invalid(format!(
                    "`{bad}` is not a text field of collection `{}`",
                    info.name
                )));
            }
            fields
        }
        None if text_fields.is_empty() => {
            return Err(ToolError::invalid(format!(
                "collection `{}` has no text fields",
                info.name
            )));
        }
        None => text_fields.into_iter().map(str::to_string).collect(),
    };
    let text = text.to_string();
    Ok(match <[String; 1]>::try_from(fields) {
        Ok([field]) => Query::Match {
            field,
            text,
            operator: BoolOperator::Or,
            minimum_should_match: None,
            fuzziness: None,
            analyzer: None,
        },
        Err(fields) => Query::MultiMatch {
            fields: fields.into_iter().map(|f| (f, 1.0)).collect(),
            text,
            kind: MultiMatchKind::BestFields,
            operator: BoolOperator::Or,
            tie_breaker: None,
        },
    })
}

/// The vector field of `info` that `vector` searches (rule 1.4).
fn vector_field(
    info: &CollectionInfo,
    named: Option<String>,
    vector: &[f32],
) -> Result<String, ToolError> {
    let names = || {
        info.schema
            .vectors
            .iter()
            .map(|v| format!("`{}`", v.name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let spec = match named {
        Some(name) => info
            .schema
            .vectors
            .iter()
            .find(|v| v.name == name)
            .ok_or_else(|| {
                ToolError::invalid(format!(
                    "collection `{}` has no vector `{name}`; its vectors: [{}]",
                    info.name,
                    names()
                ))
            })?,
        None => match info.schema.vectors.as_slice() {
            [only] => only,
            _ => {
                return Err(ToolError::invalid(format!(
                    "collection `{}` has vectors [{}]: name one with vector_field",
                    info.name,
                    names()
                )));
            }
        },
    };
    if vector.len() != spec.dim as usize {
        return Err(ToolError::invalid(format!(
            "vector has {} dimensions; `{}` has {}",
            vector.len(),
            spec.name,
            spec.dim
        )));
    }
    Ok(spec.name.clone())
}

/// Rule 4.1: the bounds of a memory.
fn validate_memory(input: &MemoryWriteInput) -> Result<(), ToolError> {
    let bytes = |what: &str, value: &str, min: usize, max: usize| {
        if (min..=max).contains(&value.len()) {
            Ok(())
        } else {
            Err(ToolError::invalid(format!(
                "{what} must be {min} to {max} bytes, got {}",
                value.len()
            )))
        }
    };
    bytes("text", &input.text, 1, 1_048_576)?;
    if let Some(tags) = &input.tags {
        if tags.len() > 64 {
            return Err(ToolError::invalid(format!(
                "at most 64 tags, got {}",
                tags.len()
            )));
        }
        for tag in tags {
            bytes("a tag", tag, 1, 256)?;
        }
    }
    if let Some(id) = &input.id {
        bytes("id", id, 1, 512)?;
    }
    if let Some(author) = &input.author {
        bytes("author", author, 0, 256)?;
    }
    if let Some(vector) = &input.vector
        && !(1..=65_535).contains(&vector.len())
    {
        return Err(ToolError::invalid(format!(
            "vector must hold 1 to 65535 elements, got {}",
            vector.len()
        )));
    }
    Ok(())
}

/// Rule 4.5: the `embedding` of `info` must have `dim` dimensions.
fn check_dimension(info: &CollectionInfo, dim: usize) -> Result<bool, ToolError> {
    match info.schema.vectors.iter().find(|v| v.name == MEMORY_VECTOR) {
        None => Ok(false),
        Some(spec) if spec.dim as usize == dim => Ok(true),
        Some(spec) => Err(ToolError {
            field: Some(MEMORY_VECTOR.to_string()),
            ..ToolError::new(
                "schema_violation",
                format!(
                    "vector has {dim} dimensions; `{MEMORY_VECTOR}` of collection `{}` has {}",
                    info.name, spec.dim
                ),
            )
        }),
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
        let select = projection(input.select);
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

    async fn run_search(
        &self,
        input: SearchInput,
        extensions: &Extensions,
    ) -> Result<SearchOutput, ToolError> {
        // 1.1
        let text = input
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty());
        if text.is_none() && input.vector.is_none() {
            return Err(ToolError::invalid("search needs a query, a vector or both"));
        }
        let max = self.config.search_max_limit;
        let limit = input.limit.unwrap_or(10);
        if !(1..=max).contains(&limit) {
            return Err(ToolError::invalid(format!(
                "limit must be 1 to {max}, got {limit}"
            )));
        }
        let ns = self.namespace(extensions)?;
        let consistency = read_consistency(input.consistency_token.as_deref())?;
        let filter = match &input.filter {
            Some(filter) => translate_filter(filter)?,
            None => None,
        };
        // 1.2
        let info = self
            .collections
            .get_collection(&ns, &input.collection)
            .await?;
        // 1.3, 1.4
        let text_query = text
            .map(|text| text_query(&info, input.fields, text))
            .transpose()?;
        let vector = match input.vector {
            Some(vector) => {
                let field = vector_field(&info, input.vector_field, &vector)?;
                Some((field, vector))
            }
            None => None,
        };
        // 1.5
        let both = text_query.is_some() && vector.is_some();
        let k = if both {
            (4 * limit).clamp(50, 1_000)
        } else {
            limit
        };
        let mut retrievers = Vec::new();
        if let Some(query) = text_query {
            retrievers.push(Retriever::Text { query, k });
        }
        if let Some((field, query)) = vector {
            retrievers.push(Retriever::Vector {
                field,
                query,
                k,
                params: AnnParams::default(),
                filter: None,
            });
        }
        // 1.6
        let request = SearchRequest {
            consistency,
            retrievers,
            fusion: both.then_some(Fusion::Rrf { k: 60 }),
            filter,
            limit,
            select: projection(input.select),
            track_total_hits: TrackTotalHits::None,
            ..SearchRequest::new(input.collection)
        };
        let response = self.collections.search(&ns, request).await?;
        // 1.7
        let mut hits: Vec<HitOut> = response
            .hits
            .into_iter()
            .map(|hit| HitOut {
                id: DocId::from(&hit.pk),
                score: hit.score,
                source: hit.source,
            })
            .collect();
        let read_token = response.read_token.to_string();
        let performance = response.performance;
        let truncated = cap_items(&mut hits, self.config.max_output_bytes, |hits| {
            tool_result_json(serde_json::json!({
                "hits": hits,
                "read_token": read_token,
                "truncated": true,
                "performance": performance,
            }))
        });
        Ok(SearchOutput {
            hits,
            read_token,
            truncated,
            performance,
        })
    }

    async fn run_sql(
        &self,
        input: SqlInput,
        extensions: &Extensions,
    ) -> Result<crate::sql::SqlOutput, ToolError> {
        let cap = self.config.sql_max_rows;
        let max_rows = match input.max_rows {
            Some(0) => return Err(ToolError::invalid("max_rows must be at least 1")),
            Some(n) => n.min(cap),
            None => cap,
        };
        let ns = self.namespace(extensions)?;
        let ctx = self.collections.sql_context(&ns);
        let limits = SqlLimits {
            max_rows,
            timeout: self.config.sql_timeout,
            max_output_bytes: self.config.max_output_bytes,
        };
        run_read_only(&ctx, &input.query, &limits).await
    }

    async fn run_memory_write(
        &self,
        input: MemoryWriteInput,
        extensions: &Extensions,
    ) -> Result<MemoryWriteOutput, ToolError> {
        // 1.
        validate_memory(&input)?;
        // 2.
        let ns = self.namespace(extensions)?;
        let c = input
            .collection
            .clone()
            .unwrap_or_else(|| self.config.memory_collection.clone());
        self.collections.ensure_namespace(&ns).await?;
        let dim = input.vector.as_ref().map(Vec::len);
        // 3.
        let (mut info, created_collection) = match self.collections.get_collection(&ns, &c).await {
            Ok(info) => (info, false),
            Err(ServiceError::NotFound {
                kind: "collection", ..
            }) => {
                let schema = memory_schema(dim.map(|d| d as u32));
                match self
                    .collections
                    .create_collection_owned(&ns, &c, schema, None)
                    .await
                {
                    Ok((info, created)) => (info, created),
                    // A concurrent creator with another schema won.
                    Err(ServiceError::AlreadyExists(_)) => {
                        (self.collections.get_collection(&ns, &c).await?, false)
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            Err(err) => return Err(err.into()),
        };
        // 4.
        if !is_memory_collection(&info.schema) {
            return Err(ToolError::new(
                "not_a_memory_collection",
                format!("collection `{c}` has no text field `text`"),
            ));
        }
        // 5.
        if let Some(dim) = dim
            && !check_dimension(&info, dim)?
        {
            let added = self
                .collections
                .add_fields(
                    &ns,
                    &c,
                    Vec::new(),
                    vec![memory_vector(dim as u32)],
                    BTreeMap::new(),
                )
                .await;
            match added {
                Ok(_) => {}
                // A concurrent add of another dimension.
                Err(ServiceError::SchemaViolation { .. } | ServiceError::InvalidArgument(_)) => {
                    info = self.collections.get_collection(&ns, &c).await?;
                    if !check_dimension(&info, dim)? {
                        return Err(ToolError {
                            field: Some(MEMORY_VECTOR.to_string()),
                            ..ToolError::new(
                                "schema_violation",
                                format!("could not add `{MEMORY_VECTOR}` to collection `{c}`"),
                            )
                        });
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        // 6.
        let id = input
            .id
            .unwrap_or_else(|| ulid::Ulid::generate().to_string());
        let mut source = Map::new();
        source.insert("text".into(), Value::String(input.text));
        source.insert("tags".into(), Value::from(input.tags.unwrap_or_default()));
        if let Some(author) = input.author {
            source.insert("author".into(), Value::String(author));
        }
        source.insert(
            "metadata".into(),
            Value::Object(input.metadata.unwrap_or_default()),
        );
        source.insert(
            "created_at".into(),
            Value::String(humantime::format_rfc3339_millis(SystemTime::now()).to_string()),
        );
        let vectors = input
            .vector
            .map(|v| BTreeMap::from([(MEMORY_VECTOR.to_string(), v)]))
            .unwrap_or_default();
        let doc = Document {
            pk: PrimaryKey::Str(id.clone()),
            source,
            vectors,
            sparse_vectors: BTreeMap::new(),
        };
        let written = self
            .collections
            .write(&ns, &c, vec![DocOp::Upsert(doc)], WriteOptions::default())
            .await?;
        Ok(MemoryWriteOutput {
            id,
            collection: c,
            created_collection,
            consistency_token: written.token.to_string(),
        })
    }
}

#[tool_router(vis = "pub(crate)")]
impl OperonMcp {
    #[tool(
        name = "search",
        description = "Hybrid search over an Operon collection: BM25 full-text search on `query`, nearest-neighbour search on `vector`, or both fused with reciprocal rank fusion. Operon computes no embeddings: pass `vector` only if it comes from the same model as the collection's vectors. `filter` is an object of field conditions that must all hold: a value (equals), a list (any of), {\"gte\": …, \"lt\": …} (range), {\"exists\": true|false}, or null (missing).",
        annotations(
            title = "Search a collection",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn search(
        &self,
        Parameters(input): Parameters<SearchInput>,
        extensions: Extensions,
    ) -> Result<CallToolResult, ErrorData> {
        respond(self.run_search(input, &extensions).await)
    }

    #[tool(
        name = "sql",
        description = "Run a read-only SQL query (Apache DataFusion dialect) over the namespace's collections, which are tables named after the collections. Only queries are allowed (SELECT, WITH, VALUES, EXPLAIN, DESCRIBE); DDL, DML, COPY and SET are refused. At most `max_rows` rows are returned.",
        annotations(
            title = "Read-only SQL",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    pub async fn sql(
        &self,
        Parameters(input): Parameters<SqlInput>,
        extensions: Extensions,
    ) -> Result<CallToolResult, ErrorData> {
        respond(self.run_sql(input, &extensions).await)
    }

    #[tool(
        name = "memory_write",
        description = "Store a memory (text with optional tags, author, metadata and an embedding vector) as a document in a memory collection (default `memories`, created on first use). Returns the memory's id; writing again with the same id replaces it. Search sees it immediately.",
        annotations(
            title = "Write a memory",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    pub async fn memory_write(
        &self,
        Parameters(input): Parameters<MemoryWriteInput>,
        extensions: Extensions,
    ) -> Result<CallToolResult, ErrorData> {
        respond(self.run_memory_write(input, &extensions).await)
    }
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
