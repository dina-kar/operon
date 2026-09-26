//! The search table functions (plan M1.2 Task 10 rule 5; Rulings 8 and 23,
//! D56): `vector_search`, `text_search`, `hybrid_search`, `rrf` and the
//! reserved `rerank`, with Spice's names and positional argument order, and
//! the scalar retriever descriptors `rrf` takes as arguments.
//!
//! A nested `vector_search(…)` or `text_search(…)` in an argument of `rrf`
//! resolves to its [`RetrieverDescriptorUdf`], which `ExprSimplifier` folds
//! into a Utf8 literal `{"collection": c, "retriever": <Retriever JSON>}`; an
//! omitted `field` is `null` there and is resolved when `rrf` plans, against
//! the collection's schema. `rrf` also takes such descriptors as string
//! literals (the fallback form of rule 5).

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Float64Type, SchemaRef};
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult, plan_datafusion_err, plan_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TableType, Volatility,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use operon_collection::{CollectionSchema, FieldKind};
use operon_common::meta::Collection;
use serde_json::{Value, json};

use crate::error::ServiceError;
use crate::exec::{df_error, plan_properties};
use crate::hot;
use crate::ir::{
    AnnParams, BoolOperator, Fusion, Query, ReadConsistency, Retriever, SearchRequest,
};
use crate::sql::provider::{RowEncoder, SqlRow};
use crate::sql::{SQL_DEFAULT_K, SqlScope};
use crate::types::{Projection, SourceFilter};

/// The scalar forms registered next to the table functions: the retriever
/// descriptors, and `rrf`'s (so `rerank(rrf(…))` reaches `rerank`).
pub(crate) const DESCRIPTOR_FUNCTIONS: [&str; 3] = ["vector_search", "text_search", "rrf"];

/// Spice's RRF constant.
const DEFAULT_RRF_K: u32 = 60;

// ----- arguments -----

/// Argument `i` (0-based) of `function`, simplified to a literal.
fn constant(
    function: &str,
    i: usize,
    expr: &Expr,
    simplifier: &ExprSimplifier,
) -> DfResult<ScalarValue> {
    match simplifier.simplify(expr.clone())? {
        Expr::Literal(value, _) => Ok(value),
        _ => plan_err!("argument {} of {function} must be a constant", i + 1),
    }
}

fn simplifier() -> ExprSimplifier {
    ExprSimplifier::new(SimplifyContext::default())
}

/// Every argument of `function`, simplified to literals.
fn constants(function: &str, exprs: &[Expr]) -> DfResult<Vec<ScalarValue>> {
    let simplifier = simplifier();
    exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| constant(function, i, expr, &simplifier))
        .collect()
}

fn arity(function: &str, values: &[ScalarValue], min: usize, max: usize) -> DfResult<()> {
    if (min..=max).contains(&values.len()) {
        Ok(())
    } else {
        plan_err!(
            "{function} takes {min} to {max} arguments, got {}",
            values.len()
        )
    }
}

/// The arguments of one call, read by position (0-based `i`; messages are
/// 1-based).
struct Args<'a> {
    function: &'a str,
    values: &'a [ScalarValue],
}

impl Args<'_> {
    /// Argument `i`, `None` when absent or NULL.
    fn get(&self, i: usize) -> Option<&ScalarValue> {
        self.values.get(i).filter(|value| !value.is_null())
    }

    fn error<T>(&self, i: usize, what: &str) -> DfResult<T> {
        plan_err!("argument {} of {} must be {what}", i + 1, self.function)
    }

    fn text(&self, i: usize) -> DfResult<Option<String>> {
        match self.get(i) {
            None => Ok(None),
            Some(value) => match value.try_as_str() {
                Some(Some(text)) => Ok(Some(text.to_string())),
                _ => self.error(i, "a string"),
            },
        }
    }

    fn required_text(&self, i: usize) -> DfResult<String> {
        match self.text(i)? {
            Some(text) => Ok(text),
            None => self.error(i, "a string, not NULL"),
        }
    }

    /// A non-negative integer (a float with no fraction too, as Spice's
    /// `60.0`).
    fn whole(&self, i: usize) -> DfResult<Option<u64>> {
        let Some(value) = self.get(i) else {
            return Ok(None);
        };
        match whole_number(value) {
            Some(n) => Ok(Some(n)),
            None => self.error(i, "a non-negative whole number"),
        }
    }

    fn count(&self, i: usize) -> DfResult<Option<usize>> {
        match self.whole(i)? {
            None => Ok(None),
            Some(n) => match usize::try_from(n) {
                Ok(n) => Ok(Some(n)),
                Err(_) => self.error(i, "a smaller number"),
            },
        }
    }

    fn boolean(&self, i: usize) -> DfResult<Option<bool>> {
        match self.get(i) {
            None => Ok(None),
            Some(ScalarValue::Boolean(Some(b))) => Ok(Some(*b)),
            Some(_) => self.error(i, "a boolean"),
        }
    }

    /// A query vector: a list literal of numbers, or a JSON array string.
    fn vector(&self, i: usize) -> DfResult<Vec<f32>> {
        let bad = || self.error(i, "a list of numbers");
        let Some(value) = self.get(i) else {
            return bad();
        };
        if let Some(Some(text)) = value.try_as_str() {
            return match serde_json::from_str::<Vec<f64>>(text) {
                Ok(numbers) => Ok(numbers.into_iter().map(|x| x as f32).collect()),
                Err(_) => bad(),
            };
        }
        let values: ArrayRef = match value {
            ScalarValue::List(list) if list.len() == 1 && !list.is_null(0) => list.value(0),
            ScalarValue::LargeList(list) if list.len() == 1 && !list.is_null(0) => list.value(0),
            ScalarValue::FixedSizeList(list) if list.len() == 1 && !list.is_null(0) => {
                list.value(0)
            }
            _ => return bad(),
        };
        let numeric = values.data_type().is_numeric() || values.data_type() == &DataType::Null;
        if !numeric || values.null_count() > 0 {
            return bad();
        }
        let Ok(floats) = cast(&values, &DataType::Float64) else {
            return bad();
        };
        Ok(floats
            .as_primitive::<Float64Type>()
            .values()
            .iter()
            .map(|x| *x as f32)
            .collect())
    }

    /// An IR `Query` in its JSON form.
    fn filter(&self, i: usize) -> DfResult<Option<Query>> {
        match self.text(i)? {
            None => Ok(None),
            Some(text) => serde_json::from_str::<Query>(&text)
                .map(Some)
                .map_err(|err| {
                    plan_datafusion_err!(
                        "argument {} of {} is not a filter: {err}",
                        i + 1,
                        self.function
                    )
                }),
        }
    }
}

/// A non-negative integer literal, or a float literal with no fraction.
fn whole_number(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::Float16(Some(x)) => whole_float(f64::from(*x)),
        ScalarValue::Float32(Some(x)) => whole_float(f64::from(*x)),
        ScalarValue::Float64(Some(x)) => whole_float(*x),
        ScalarValue::Int8(Some(n)) => u64::try_from(*n).ok(),
        ScalarValue::Int16(Some(n)) => u64::try_from(*n).ok(),
        ScalarValue::Int32(Some(n)) => u64::try_from(*n).ok(),
        ScalarValue::Int64(Some(n)) => u64::try_from(*n).ok(),
        ScalarValue::UInt8(Some(n)) => Some(u64::from(*n)),
        ScalarValue::UInt16(Some(n)) => Some(u64::from(*n)),
        ScalarValue::UInt32(Some(n)) => Some(u64::from(*n)),
        ScalarValue::UInt64(Some(n)) => Some(*n),
        _ => None,
    }
}

fn whole_float(x: f64) -> Option<u64> {
    (x.is_finite() && x >= 0.0 && x.fract() == 0.0 && x <= u64::MAX as f64).then_some(x as u64)
}

fn is_number(value: &ScalarValue) -> bool {
    value.data_type().is_numeric()
}

// ----- calls -----

/// `vector_search(collection, query_vector [, field [, k [, filter_json [, exact]]]])`.
struct VectorCall {
    collection: String,
    query: Vec<f32>,
    field: Option<String>,
    k: usize,
    filter: Option<Query>,
    exact: bool,
}

impl VectorCall {
    fn parse(values: &[ScalarValue]) -> DfResult<Self> {
        arity("vector_search", values, 2, 6)?;
        let args = Args {
            function: "vector_search",
            values,
        };
        Ok(Self {
            collection: args.required_text(0)?,
            query: args.vector(1)?,
            field: args.text(2)?,
            k: args.count(3)?.unwrap_or(SQL_DEFAULT_K),
            filter: args.filter(4)?,
            exact: args.boolean(5)?.unwrap_or(false),
        })
    }

    fn retriever(&self, field: String) -> Retriever {
        Retriever::Vector {
            field,
            query: self.query.clone(),
            k: self.k,
            params: AnnParams {
                exact: self.exact,
                ..AnnParams::default()
            },
            filter: self.filter.clone(),
        }
    }
}

/// `text_search(collection, text [, field [, k [, filter_json]]])`.
struct TextCall {
    collection: String,
    text: String,
    field: Option<String>,
    k: usize,
    filter: Option<Query>,
}

/// An OR `match` of `text` on `field`.
fn match_query(field: String, text: String) -> Query {
    Query::Match {
        field,
        text,
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    }
}

impl TextCall {
    fn parse(values: &[ScalarValue]) -> DfResult<Self> {
        arity("text_search", values, 2, 5)?;
        let args = Args {
            function: "text_search",
            values,
        };
        Ok(Self {
            collection: args.required_text(0)?,
            text: args.required_text(1)?,
            field: args.text(2)?,
            k: args.count(3)?.unwrap_or(SQL_DEFAULT_K),
            filter: args.filter(4)?,
        })
    }

    /// The match, restricted by the filter without changing its scores.
    fn retriever(&self, field: String) -> Retriever {
        let matching = match_query(field, self.text.clone());
        let query = match &self.filter {
            None => matching,
            Some(filter) => Query::Bool {
                must: vec![matching],
                should: Vec::new(),
                must_not: Vec::new(),
                filter: vec![filter.clone()],
                minimum_should_match: None,
            },
        };
        Retriever::Text { query, k: self.k }
    }
}

/// The collection's only dense vector.
fn default_vector_field(collection: &str, schema: &CollectionSchema) -> DfResult<String> {
    match schema.vectors.as_slice() {
        [only] => Ok(only.name.clone()),
        all => plan_err!(
            "vector_search on {collection} needs a field: it has {} vectors",
            all.len()
        ),
    }
}

/// The collection's only Text field.
fn default_text_field(collection: &str, schema: &CollectionSchema) -> DfResult<String> {
    let texts: Vec<&str> = schema
        .fields
        .iter()
        .filter(|spec| matches!(spec.kind, FieldKind::Text { .. }))
        .map(|spec| spec.name.as_str())
        .collect();
    match texts.as_slice() {
        [only] => Ok(only.to_string()),
        all => plan_err!(
            "text_search on {collection} needs a field: it has {} text fields",
            all.len()
        ),
    }
}

/// Where a descriptor's retriever keeps its field.
const FIELD_POINTERS: [&str; 3] = [
    "/vector/field",
    "/text/query/match/field",
    "/text/query/bool/must/0/match/field",
];

/// `{"collection": c, "retriever": <Retriever JSON>}`, the field `null`
/// when the call omitted it.
fn descriptor(collection: &str, retriever: &Retriever, field_given: bool) -> DfResult<String> {
    let mut retriever = serde_json::to_value(retriever)
        .map_err(|err| DataFusionError::Internal(format!("encoding a retriever: {err}")))?;
    if !field_given {
        for pointer in FIELD_POINTERS {
            if let Some(field) = retriever.pointer_mut(pointer) {
                *field = Value::Null;
                break;
            }
        }
    }
    Ok(json!({"collection": collection, "retriever": retriever}).to_string())
}

/// The descriptor of a `vector_search` or `text_search` call.
fn call_descriptor(function: &str, values: &[ScalarValue]) -> DfResult<String> {
    match function {
        "vector_search" => {
            let call = VectorCall::parse(values)?;
            let field = call.field.clone().unwrap_or_default();
            descriptor(
                &call.collection,
                &call.retriever(field),
                call.field.is_some(),
            )
        }
        "text_search" => {
            let call = TextCall::parse(values)?;
            let field = call.field.clone().unwrap_or_default();
            descriptor(
                &call.collection,
                &call.retriever(field),
                call.field.is_some(),
            )
        }
        other => plan_err!("{other} is not a retriever"),
    }
}

// ----- the scalar descriptors -----

/// The scalar form of `vector_search`, `text_search` (and `rrf`), used as
/// arguments of `rrf` (and `rerank`): each returns a descriptor, Utf8 JSON
/// (rule 5). Evaluated anywhere else, a descriptor is just its JSON text.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RetrieverDescriptorUdf {
    name: &'static str,
    signature: Signature,
}

impl RetrieverDescriptorUdf {
    /// `name` is `vector_search`, `text_search` or `rrf`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    /// The descriptor of one row of arguments.
    fn evaluate(&self, values: &[ScalarValue]) -> DfResult<String> {
        if self.name != "rrf" {
            return call_descriptor(self.name, values);
        }
        // `{"rrf": [...]}`: descriptors inline, numbers as numbers.
        let args: Vec<Value> = values
            .iter()
            .map(|value| {
                if value.is_null() {
                    return Value::Null;
                }
                if let Some(Some(text)) = value.try_as_str() {
                    return serde_json::from_str::<Value>(text)
                        .ok()
                        .filter(Value::is_object)
                        .unwrap_or_else(|| Value::String(text.to_string()));
                }
                match whole_number(value) {
                    Some(n) => json!(n),
                    None => Value::String(value.to_string()),
                }
            })
            .collect();
        Ok(json!({ "rrf": args }).to_string())
    }
}

impl ScalarUDFImpl for RetrieverDescriptorUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let row = |r: usize| -> DfResult<Vec<ScalarValue>> {
            args.args
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Scalar(value) => Ok(value.clone()),
                    ColumnarValue::Array(array) => ScalarValue::try_from_array(array, r),
                })
                .collect()
        };
        if scalar {
            let text = self.evaluate(&row(0)?)?;
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))));
        }
        let mut out = StringBuilder::new();
        for r in 0..args.number_rows {
            out.append_value(self.evaluate(&row(r)?)?);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ----- the table functions -----

impl SqlScope {
    /// The collection named or aliased `name`, as the catalog cache knows it
    /// (table functions plan synchronously; `run_read_only` refreshes the
    /// cache first).
    fn planned(&self, name: &str) -> DfResult<Collection> {
        self.service
            .catalog()
            .collection(&self.ns, name)
            .ok_or_else(|| {
                df_error(ServiceError::NotFound {
                    kind: "collection",
                    name: name.to_string(),
                })
            })
    }
}

/// `vector_search(collection, query_vector [, field [, k [, filter_json [, exact]]]])`.
#[derive(Debug)]
pub struct VectorSearchFunction {
    scope: SqlScope,
}

impl VectorSearchFunction {
    pub(crate) fn new(scope: SqlScope) -> Self {
        Self { scope }
    }
}

impl TableFunctionImpl for VectorSearchFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let call = VectorCall::parse(&constants("vector_search", args.exprs())?)?;
        let collection = self.scope.planned(&call.collection)?;
        let field = match &call.field {
            Some(field) => field.clone(),
            None => default_vector_field(&call.collection, &collection.schema)?,
        };
        let mut request = SearchRequest::new(&call.collection);
        request.retrievers = vec![call.retriever(field)];
        request.limit = call.k;
        Ok(SearchProvider::new(self.scope.clone(), collection, request))
    }
}

/// `text_search(collection, text [, field [, k [, filter_json]]])`.
#[derive(Debug)]
pub struct TextSearchFunction {
    scope: SqlScope,
}

impl TextSearchFunction {
    pub(crate) fn new(scope: SqlScope) -> Self {
        Self { scope }
    }
}

impl TableFunctionImpl for TextSearchFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let call = TextCall::parse(&constants("text_search", args.exprs())?)?;
        let collection = self.scope.planned(&call.collection)?;
        let field = match &call.field {
            Some(field) => field.clone(),
            None => default_text_field(&call.collection, &collection.schema)?,
        };
        let mut request = SearchRequest::new(&call.collection);
        request.retrievers = vec![call.retriever(field)];
        request.limit = call.k;
        Ok(SearchProvider::new(self.scope.clone(), collection, request))
    }
}

/// `hybrid_search(collection, vector_field, query_vector, text_field, text,
/// k [, fusion ('rrf' | 'dbsf') [, rrf_k (60) [, filter_json]]])`: Operon's
/// two-field shorthand for `rrf(vector_search(…), text_search(…))`.
#[derive(Debug)]
pub struct HybridSearchFunction {
    scope: SqlScope,
}

impl HybridSearchFunction {
    pub(crate) fn new(scope: SqlScope) -> Self {
        Self { scope }
    }
}

impl TableFunctionImpl for HybridSearchFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let values = constants("hybrid_search", args.exprs())?;
        arity("hybrid_search", &values, 6, 9)?;
        let args = Args {
            function: "hybrid_search",
            values: &values,
        };
        let name = args.required_text(0)?;
        let vector_field = args.required_text(1)?;
        let query = args.vector(2)?;
        let text_field = args.required_text(3)?;
        let text = args.required_text(4)?;
        let Some(k) = args.count(5)? else {
            return args.error(5, "a non-negative whole number");
        };
        let rrf_k = match args.whole(7)? {
            None => DEFAULT_RRF_K,
            Some(n) => match u32::try_from(n) {
                Ok(n) => n,
                Err(_) => return args.error(7, "a smaller number"),
            },
        };
        let fusion = match args.text(6)?.map(|f| f.to_ascii_lowercase()).as_deref() {
            None | Some("rrf") => Fusion::Rrf { k: rrf_k },
            Some("dbsf") => Fusion::Dbsf,
            Some(_) => return args.error(6, "'rrf' or 'dbsf'"),
        };
        let filter = args.filter(8)?;
        let collection = self.scope.planned(&name)?;
        let mut request = SearchRequest::new(&name);
        request.retrievers = vec![
            Retriever::Vector {
                field: vector_field,
                query,
                k,
                params: AnnParams::default(),
                filter: None,
            },
            Retriever::Text {
                query: match_query(text_field, text),
                k,
            },
        ];
        request.fusion = Some(fusion);
        request.filter = filter;
        request.limit = k;
        Ok(SearchProvider::new(self.scope.clone(), collection, request))
    }
}

/// `rrf(retriever, retriever [, …] [, k [, limit]])` (Ruling 23): fuses
/// nested `vector_search`/`text_search` calls of one collection.
#[derive(Debug)]
pub struct RrfFunction {
    scope: SqlScope,
}

impl RrfFunction {
    pub(crate) fn new(scope: SqlScope) -> Self {
        Self { scope }
    }

    /// A descriptor's collection and retriever, its omitted field resolved.
    fn retriever(&self, i: usize, text: &str) -> DfResult<(Collection, String, Retriever)> {
        let not_a_retriever = || {
            plan_datafusion_err!(
                "argument {} of rrf must be a vector_search or text_search call",
                i + 1
            )
        };
        let Ok(Value::Object(mut object)) = serde_json::from_str::<Value>(text) else {
            return Err(not_a_retriever());
        };
        let (Some(Value::String(name)), Some(mut retriever)) =
            (object.remove("collection"), object.remove("retriever"))
        else {
            return Err(not_a_retriever());
        };
        let collection = self.scope.planned(&name)?;
        if let Some(field) = retriever.pointer_mut("/vector/field")
            && field.is_null()
        {
            *field = Value::String(default_vector_field(&name, &collection.schema)?);
        }
        for pointer in &FIELD_POINTERS[1..] {
            if let Some(field) = retriever.pointer_mut(pointer)
                && field.is_null()
            {
                *field = Value::String(default_text_field(&name, &collection.schema)?);
            }
        }
        let retriever: Retriever = serde_json::from_value(retriever).map_err(|err| {
            plan_datafusion_err!("argument {} of rrf is not a retriever: {err}", i + 1)
        })?;
        if !matches!(retriever, Retriever::Vector { .. } | Retriever::Text { .. }) {
            return Err(not_a_retriever());
        }
        Ok((collection, name, retriever))
    }
}

/// The retriever `k` for the default `limit`.
fn retriever_k(retriever: &Retriever) -> usize {
    match retriever {
        Retriever::Vector { k, .. }
        | Retriever::Text { k, .. }
        | Retriever::Fused { k, .. }
        | Retriever::Rescore { k, .. }
        | Retriever::Sparse { k, .. } => *k,
    }
}

impl TableFunctionImpl for RrfFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let simplifier = simplifier();
        let mut retrievers: Vec<(Collection, String, Retriever)> = Vec::new();
        // The trailing `k` and `limit` (NULL: the default).
        let mut numbers: Vec<ScalarValue> = Vec::new();
        for (i, expr) in args.exprs().iter().enumerate() {
            let value = match simplifier.simplify(expr.clone()) {
                Ok(Expr::Literal(value, _)) => value,
                // A nested call that did not fold: its own planning error.
                result => match expr {
                    Expr::ScalarFunction(ScalarFunction { func, args })
                        if matches!(func.name(), "vector_search" | "text_search") =>
                    {
                        let values = constants(func.name(), args)?;
                        ScalarValue::Utf8(Some(call_descriptor(func.name(), &values)?))
                    }
                    _ => {
                        result?;
                        return plan_err!("argument {} of rrf must be a constant", i + 1);
                    }
                },
            };
            if is_number(&value) || value.is_null() {
                numbers.push(value);
                continue;
            }
            if !numbers.is_empty() {
                return plan_err!("rrf takes its k and limit after its retrievers");
            }
            match value.try_as_str() {
                Some(Some(text)) => retrievers.push(self.retriever(i, text)?),
                _ => {
                    return plan_err!(
                        "argument {} of rrf must be a vector_search or text_search call",
                        i + 1
                    );
                }
            }
        }
        if retrievers.len() < 2 {
            return plan_err!(
                "rrf fuses at least two retrievers, got {}",
                retrievers.len()
            );
        }
        if numbers.len() > 2 {
            return plan_err!(
                "rrf takes at most k and limit after its retrievers, got {} numbers",
                numbers.len()
            );
        }
        let (first, first_name, _) = &retrievers[0];
        if let Some((_, other, _)) = retrievers.iter().find(|(c, _, _)| c.id != first.id) {
            return plan_err!(
                "rrf fuses retrievers of one collection, got {first_name} and {other}"
            );
        }
        let k = match numbers.first().filter(|value| !value.is_null()) {
            None => DEFAULT_RRF_K,
            Some(value) => match whole_number(value).and_then(|n| u32::try_from(n).ok()) {
                Some(k) => k,
                None => return plan_err!("rrf's k must be a whole number, got {value}"),
            },
        };
        let max_window = self.scope.service.config().search.limits.max_window;
        let limit = match numbers.get(1).filter(|value| !value.is_null()) {
            None => retrievers
                .iter()
                .map(|(_, _, retriever)| retriever_k(retriever))
                .max()
                .unwrap_or(0)
                .min(max_window),
            Some(value) => match whole_number(value).and_then(|n| usize::try_from(n).ok()) {
                Some(limit) => limit,
                None => return plan_err!("rrf's limit must be a whole number, got {value}"),
            },
        };
        let collection = first.clone();
        let mut request = SearchRequest::new(first_name.clone());
        request.retrievers = retrievers.into_iter().map(|(_, _, r)| r).collect();
        request.fusion = Some(Fusion::Rrf { k });
        request.limit = limit;
        Ok(SearchProvider::new(self.scope.clone(), collection, request))
    }
}

/// `rerank(…)`: reserved for M3 (D56); registered only to refuse.
#[derive(Debug)]
pub struct RerankFunction;

impl TableFunctionImpl for RerankFunction {
    fn call_with_args(&self, _args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        plan_err!("rerank arrives in M3 (D56); use rrf or hybrid_search")
    }
}

// ----- the result table -----

/// The hits of one search as a table: `_id`, `_score`, then the collection
/// columns of rule 2, in rank order.
#[derive(Debug)]
struct SearchProvider {
    scope: SqlScope,
    collection: Collection,
    request: SearchRequest,
    schema: SchemaRef,
}

impl SearchProvider {
    fn new(scope: SqlScope, collection: Collection, request: SearchRequest) -> Arc<Self> {
        let schema = RowEncoder::new(&collection.schema, true, None)
            .map(|encoder| encoder.output())
            .unwrap_or_else(|_| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        Arc::new(Self {
            scope,
            collection,
            request,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for SearchProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let encoder = RowEncoder::new(&self.collection.schema, true, projection)?;
        let mut request = self.request.clone();
        if let Some(limit) = limit {
            // Hits come in rank order, so the first `limit` serve any LIMIT.
            request.limit = request.limit.min(limit);
        }
        Ok(Arc::new(SearchExec {
            properties: plan_properties(encoder.output()),
            inner: Arc::new(SearchInner {
                scope: self.scope.clone(),
                request,
                encoder,
            }),
        }))
    }
}

#[derive(Debug)]
struct SearchInner {
    scope: SqlScope,
    request: SearchRequest,
    encoder: RowEncoder,
}

impl SearchInner {
    /// Runs the search (forwarded to the owner when the placement says so)
    /// and encodes its hits.
    async fn run(&self) -> Result<datafusion::arrow::array::RecordBatch, ServiceError> {
        let scope = &self.scope;
        let mut request = self.request.clone();
        request.consistency = scope.consistency.clone();
        request.select = Projection {
            source: if self.encoder.needs_source() {
                SourceFilter::All
            } else {
                SourceFilter::None
            },
            vectors: self.encoder.vector_names(),
            fields: Vec::new(),
        };
        let response = hot::scope(
            scope.hot.clone(),
            scope.service.search(&scope.ns, request.clone()),
        )
        .await?;
        let positioned = self.encoder.columns().iter().any(|column| {
            matches!(
                column,
                crate::sql::provider::Column::SeqNo | crate::sql::provider::Column::Partition
            )
        });
        // Hits carry no `seq_no`: read the positions of the same state.
        let mut positions: Vec<Option<(u64, u32)>> = vec![None; response.hits.len()];
        if positioned && !response.hits.is_empty() {
            let consistency = match &scope.consistency {
                pinned @ ReadConsistency::Pinned { .. } => pinned.clone(),
                _ => ReadConsistency::AtLeast(response.read_token.clone()),
            };
            let bare = Projection {
                source: SourceFilter::None,
                vectors: Vec::new(),
                fields: Vec::new(),
            };
            let pks: Vec<_> = response.hits.iter().map(|hit| hit.pk.clone()).collect();
            let chunk = scope.service.config().max_get_keys.max(1);
            let mut at = 0;
            for keys in pks.chunks(chunk) {
                let docs = hot::scope(
                    scope.hot.clone(),
                    scope.service.get(
                        &scope.ns,
                        &request.collection,
                        keys,
                        &bare,
                        consistency.clone(),
                    ),
                )
                .await?;
                for doc in docs {
                    positions[at] = doc.map(|doc| (doc.seq_no, doc.partition));
                    at += 1;
                }
            }
        }
        let rows: Vec<SqlRow> = response
            .hits
            .into_iter()
            .zip(positions)
            .map(|(hit, position)| SqlRow {
                pk: Some(hit.pk),
                score: Some(hit.score),
                source: hit.source,
                seq_no: position.map(|(seq_no, _)| seq_no),
                partition: position.map(|(_, partition)| partition),
                vectors: hit.vectors,
                sparse: hit.sparse_vectors,
            })
            .collect();
        self.encoder.encode(&rows)
    }
}

/// Runs one search at execution and emits its hits in rank order.
struct SearchExec {
    inner: Arc<SearchInner>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for SearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchExec")
            .field("collection", &self.inner.request.collection)
            .field("retrievers", &self.inner.request.retrievers.len())
            .field("limit", &self.inner.request.limit)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SearchExec: collection={}, retrievers={}, limit={}",
            self.inner.request.collection,
            self.inner.request.retrievers.len(),
            self.inner.request.limit
        )
    }
}

impl ExecutionPlan for SearchExec {
    fn name(&self) -> &str {
        "SearchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "SearchExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SearchExec has one partition, not {partition}"
            )));
        }
        let inner = self.inner.clone();
        let stream = futures::stream::once(async move { inner.run().await.map_err(df_error) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.encoder.output(),
            stream,
        )))
    }
}
