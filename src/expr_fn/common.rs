use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NonExecutableScoreUdf {
    name: &'static str,
    aliases: Vec<String>,
    signature: Signature,
}

impl NonExecutableScoreUdf {
    pub(crate) fn new(name: &'static str, aliases: &[&str]) -> Self {
        Self {
            name,
            aliases: aliases.iter().map(|alias| (*alias).to_owned()).collect(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NonExecutableScoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }

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
    name: &'static str,
    aliases: Vec<String>,
    signature: Signature,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

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
