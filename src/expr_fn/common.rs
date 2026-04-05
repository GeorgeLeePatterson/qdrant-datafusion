use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

pub(crate) fn function_args<'a>(
    expr: &'a Expr,
    name: &str,
    aliases: &[&str],
) -> Option<&'a [Expr]> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    (function.name() == name || aliases.iter().any(|alias| function.name() == *alias))
        .then_some(function.args.as_slice())
}

pub(crate) fn column_name(expr: &Expr, function_name: &str) -> Result<String> {
    let expr = expr.clone().unalias_nested().data;
    let Expr::Column(column) = expr else {
        return plan_err!("{function_name} requires a column reference");
    };
    Ok(column.name)
}

fn is_array_constructor_function_name(name: &str) -> bool { matches!(name, "make_array" | "array") }

pub(crate) fn literal_scalar(
    expr: &Expr,
    function_name: &str,
    argument: &str,
    expectation: &str,
) -> Result<ScalarValue> {
    match expr.clone().unalias_nested().data {
        Expr::Alias(alias) => literal_scalar(&alias.expr, function_name, argument, expectation),
        Expr::Cast(cast) => literal_scalar(&cast.expr, function_name, argument, expectation),
        Expr::TryCast(cast) => literal_scalar(&cast.expr, function_name, argument, expectation),
        Expr::Literal(value, _) => Ok(value),
        Expr::ScalarFunction(function) if is_array_constructor_function_name(function.name()) => {
            let values = function
                .args
                .iter()
                .map(|arg| literal_scalar(arg, function_name, argument, expectation))
                .collect::<Result<Vec<_>>>()?;
            let item_type = values
                .iter()
                .find(|value| !value.is_null())
                .map_or(DataType::Null, ScalarValue::data_type);
            Ok(ScalarValue::List(ScalarValue::new_list_nullable(&values, &item_type)))
        }
        _ => plan_err!("{function_name} requires {argument} to be {expectation}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NonExecutableScoreUdf {
    name:      &'static str,
    aliases:   Vec<String>,
    signature: Signature,
}

impl NonExecutableScoreUdf {
    pub(crate) fn new(name: &'static str, aliases: &[&str]) -> Self {
        Self::new_with_signature(name, aliases, Signature::variadic_any(Volatility::Immutable))
    }

    pub(crate) fn new_nullary_or_variadic(name: &'static str, aliases: &[&str]) -> Self {
        Self::new_with_signature(
            name,
            aliases,
            Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
        )
    }

    pub(crate) fn new_with_signature(
        name: &'static str,
        aliases: &[&str],
        signature: Signature,
    ) -> Self {
        Self { name, aliases: aliases.iter().map(|alias| (*alias).to_owned()).collect(), signature }
    }
}

impl ScalarUDFImpl for NonExecutableScoreUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.name }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float32) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float32, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} requires qdrant operator pushdown", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NonExecutableMarkerUdf {
    name:        &'static str,
    aliases:     Vec<String>,
    signature:   Signature,
    return_type: DataType,
}

impl NonExecutableMarkerUdf {
    pub(crate) fn new(name: &'static str, aliases: &[&str], return_type: DataType) -> Self {
        Self {
            name,
            aliases: aliases.iter().map(|alias| (*alias).to_owned()).collect(),
            signature: Signature::variadic_any(Volatility::Immutable),
            return_type,
        }
    }
}

impl ScalarUDFImpl for NonExecutableMarkerUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.name }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), self.return_type.clone(), false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} requires qdrant operator pushdown", self.name)
    }
}
