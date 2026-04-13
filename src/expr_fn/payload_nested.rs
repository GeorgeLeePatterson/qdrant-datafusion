use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub const PAYLOAD_NESTED_MATCH_FUNCTION_NAME: &str = "payload_nested_match";

const PAYLOAD_NESTED_MATCH_ALIASES: &[&str] = &["qdrant_payload_nested_match"];

pub(crate) fn is_payload_nested_match_function_name(name: &str) -> bool {
    name == PAYLOAD_NESTED_MATCH_FUNCTION_NAME || PAYLOAD_NESTED_MATCH_ALIASES.contains(&name)
}

#[must_use]
pub fn qdrant_payload_nested_match(accessor: Expr, predicate: Expr) -> Expr {
    qdrant_payload_nested_match_udf().call(vec![accessor, predicate])
}

pub(crate) fn qdrant_payload_nested_match_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadNestedMatchUdf::new())).clone()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadNestedMatchUdf {
    aliases:   Vec<String>,
    signature: Signature,
}

impl PayloadNestedMatchUdf {
    fn new() -> Self {
        Self {
            aliases:   PAYLOAD_NESTED_MATCH_ALIASES
                .iter()
                .map(|alias| (*alias).to_owned())
                .collect(),
            signature: Signature::any(2, Volatility::Immutable)
                .with_parameter_names(vec!["accessor", "predicate"])
                .expect("payload nested signature should accept named parameters"),
        }
    }
}

impl ScalarUDFImpl for PayloadNestedMatchUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { PAYLOAD_NESTED_MATCH_FUNCTION_NAME }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("payload_nested_match requires exact qdrant nested filter pushdown")
    }
}
