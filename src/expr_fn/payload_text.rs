use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

use super::common::literal_scalar;
use super::payload_access::array_string_value;
use crate::qdrant::QdrantPayloadAccess;

pub const PAYLOAD_TEXT_MATCH_FUNCTION_NAME: &str = "payload_text_match";
pub const PAYLOAD_TEXT_ANY_FUNCTION_NAME: &str = "payload_text_any";
pub const PAYLOAD_PHRASE_MATCH_FUNCTION_NAME: &str = "payload_phrase_match";
pub(crate) const PAYLOAD_TEXT_MATCH_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_text_match_access";
pub(crate) const PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME: &str = "__qdrant_payload_text_any_access";
pub(crate) const PAYLOAD_PHRASE_MATCH_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_phrase_match_access";

const PAYLOAD_TEXT_MATCH_ALIASES: &[&str] = &["qdrant_payload_text_match"];
const PAYLOAD_TEXT_ANY_ALIASES: &[&str] = &["qdrant_payload_text_any"];
const PAYLOAD_PHRASE_MATCH_ALIASES: &[&str] = &["qdrant_payload_phrase_match"];

pub(crate) fn is_payload_text_match_function_name(name: &str) -> bool {
    name == PAYLOAD_TEXT_MATCH_FUNCTION_NAME || PAYLOAD_TEXT_MATCH_ALIASES.contains(&name)
}

pub(crate) fn is_payload_text_any_function_name(name: &str) -> bool {
    name == PAYLOAD_TEXT_ANY_FUNCTION_NAME || PAYLOAD_TEXT_ANY_ALIASES.contains(&name)
}

pub(crate) fn is_payload_phrase_match_function_name(name: &str) -> bool {
    name == PAYLOAD_PHRASE_MATCH_FUNCTION_NAME || PAYLOAD_PHRASE_MATCH_ALIASES.contains(&name)
}

#[must_use]
pub fn qdrant_payload_text_match(accessor: Expr, query: Expr) -> Expr {
    qdrant_payload_text_match_udf().call(vec![accessor, query])
}

#[must_use]
pub fn qdrant_payload_text_any(accessor: Expr, terms: Expr) -> Expr {
    qdrant_payload_text_any_udf().call(vec![accessor, terms])
}

#[must_use]
pub fn qdrant_payload_phrase_match(accessor: Expr, phrase: Expr) -> Expr {
    qdrant_payload_phrase_match_udf().call(vec![accessor, phrase])
}

pub(crate) fn qdrant_payload_text_match_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadTextPredicateUdf::text_match()))
        .clone()
}

pub(crate) fn qdrant_payload_text_any_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadTextPredicateUdf::text_any())).clone()
}

pub(crate) fn qdrant_payload_phrase_match_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadTextPredicateUdf::phrase_match()))
        .clone()
}

pub(crate) fn qdrant_payload_text_match_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadTextPredicateAccessUdf::new(
            PayloadTextPredicateKind::TextMatch,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_payload_text_any_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadTextPredicateAccessUdf::new(
            PayloadTextPredicateKind::TextAny,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_payload_phrase_match_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadTextPredicateAccessUdf::new(
            PayloadTextPredicateKind::PhraseMatch,
        ))
    })
    .clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PayloadTextPredicateKind {
    TextMatch,
    TextAny,
    PhraseMatch,
}

impl PayloadTextPredicateKind {
    fn function_name(self) -> &'static str {
        match self {
            Self::TextMatch => PAYLOAD_TEXT_MATCH_FUNCTION_NAME,
            Self::TextAny => PAYLOAD_TEXT_ANY_FUNCTION_NAME,
            Self::PhraseMatch => PAYLOAD_PHRASE_MATCH_FUNCTION_NAME,
        }
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::TextMatch => PAYLOAD_TEXT_MATCH_ALIASES,
            Self::TextAny => PAYLOAD_TEXT_ANY_ALIASES,
            Self::PhraseMatch => PAYLOAD_PHRASE_MATCH_ALIASES,
        }
    }

    fn internal_function_name(self) -> &'static str {
        match self {
            Self::TextMatch => PAYLOAD_TEXT_MATCH_ACCESS_FUNCTION_NAME,
            Self::TextAny => PAYLOAD_TEXT_ANY_ACCESS_FUNCTION_NAME,
            Self::PhraseMatch => PAYLOAD_PHRASE_MATCH_ACCESS_FUNCTION_NAME,
        }
    }

    fn query_label(self) -> &'static str {
        match self {
            Self::TextMatch => "query",
            Self::TextAny => "terms",
            Self::PhraseMatch => "phrase",
        }
    }

    fn pushdown_message(self) -> &'static str {
        match self {
            Self::TextMatch => {
                "payload_text_match requires exact qdrant text-index filter pushdown"
            }
            Self::TextAny => "payload_text_any requires exact qdrant text-index filter pushdown",
            Self::PhraseMatch => {
                "payload_phrase_match requires exact qdrant phrase-match filter pushdown"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadTextPredicateUdf {
    kind:      PayloadTextPredicateKind,
    aliases:   Vec<String>,
    signature: Signature,
}

impl PayloadTextPredicateUdf {
    fn new(kind: PayloadTextPredicateKind) -> Self {
        Self {
            kind,
            aliases: kind.aliases().iter().map(|alias| (*alias).to_owned()).collect(),
            signature: Signature::any(2, Volatility::Immutable)
                .with_parameter_names(vec!["accessor", kind.query_label()])
                .expect("payload text predicate signature should accept two named parameters"),
        }
    }

    fn text_match() -> Self { Self::new(PayloadTextPredicateKind::TextMatch) }

    fn text_any() -> Self { Self::new(PayloadTextPredicateKind::TextAny) }

    fn phrase_match() -> Self { Self::new(PayloadTextPredicateKind::PhraseMatch) }
}

impl ScalarUDFImpl for PayloadTextPredicateUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.kind.function_name() }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, false)))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [accessor, query] = args.as_slice() else {
            return plan_err!(
                "{} requires a qdrant payload accessor and a string {}",
                self.name(),
                self.kind.query_label()
            );
        };
        let Some(access) = QdrantPayloadAccess::from_logical_expr(accessor) else {
            return plan_err!("{} accessor must be a qdrant payload path", self.name());
        };
        let (payload, path) = access.into_parts();
        let expr = match self.kind {
            PayloadTextPredicateKind::TextMatch => qdrant_payload_text_match_access_udf(),
            PayloadTextPredicateKind::TextAny => qdrant_payload_text_any_access_udf(),
            PayloadTextPredicateKind::PhraseMatch => qdrant_payload_phrase_match_access_udf(),
        }
        .call(vec![
            payload,
            lit(path),
            payload_text_query_expr(self.kind, query, self.name())?,
        ]);
        Ok(ExprSimplifyResult::Simplified(expr))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{}", self.kind.pushdown_message())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadTextPredicateAccessUdf {
    kind:      PayloadTextPredicateKind,
    signature: Signature,
}

impl PayloadTextPredicateAccessUdf {
    fn new(kind: PayloadTextPredicateKind) -> Self {
        Self { kind, signature: Signature::any(3, Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for PayloadTextPredicateAccessUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.kind.internal_function_name() }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!(
                "{} requires a payload JSON value, payload path, and a string {}",
                self.kind.function_name(),
                self.kind.query_label()
            );
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let payloads = &arrays[0];
        let paths = &arrays[1];
        let queries = &arrays[2];
        if payloads.is_empty() {
            return Ok(ColumnarValue::Array(datafusion::arrow::array::new_null_array(
                &DataType::Boolean,
                0,
            )));
        }
        let _payload = array_string_value(payloads, 0, "payload")?;
        let _path = array_string_value(paths, 0, "payload path")?;
        let _query = array_string_value(queries, 0, self.kind.query_label())?;
        exec_err!("{}", self.kind.pushdown_message())
    }
}

fn payload_text_query_expr(
    kind: PayloadTextPredicateKind,
    expr: &Expr,
    function_name: &str,
) -> Result<Expr> {
    match kind {
        PayloadTextPredicateKind::TextAny => Ok(lit(payload_text_any_query_string(
            &literal_scalar(expr, function_name, kind.query_label(), "a string or array literal")?,
            function_name,
            kind.query_label(),
        )?)),
        PayloadTextPredicateKind::TextMatch | PayloadTextPredicateKind::PhraseMatch => {
            Ok(expr.clone())
        }
    }
}

pub(crate) fn payload_text_any_query_string(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<String> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Ok(value.clone()),
        ScalarValue::List(array) => {
            if array.is_empty() {
                return Ok(String::new());
            }
            list_text_any_terms(array.value(0).as_ref(), function_name, argument)
        }
        ScalarValue::LargeList(array) => {
            if array.is_empty() {
                return Ok(String::new());
            }
            list_text_any_terms(array.value(0).as_ref(), function_name, argument)
        }
        _ => plan_err!("{function_name} requires {argument} to be a string or array literal"),
    }
}

fn list_text_any_terms(array: &dyn Array, function_name: &str, argument: &str) -> Result<String> {
    let mut terms = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        let value = ScalarValue::try_from_array(array, index)?;
        let Some(term) = value.try_as_str().flatten() else {
            return plan_err!(
                "{function_name} requires {argument} to be a string or array literal"
            );
        };
        terms.push(term.to_owned());
    }
    Ok(terms.join(" "))
}
