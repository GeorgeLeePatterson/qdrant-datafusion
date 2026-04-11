use std::sync::OnceLock;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, plan_err};
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::prelude::lit;

use super::common::{NonExecutableMarkerUdf, function_args};

pub const FORMULA_SCORE_FUNCTION_NAME: &str = "qdrant_formula_score";
pub const PAYLOAD_NUM_FUNCTION_NAME: &str = "qdrant_payload_num";
pub const PAYLOAD_DATETIME_FUNCTION_NAME: &str = "qdrant_payload_datetime";
pub const DATETIME_VALUE_FUNCTION_NAME: &str = "qdrant_datetime_value";
pub const CONDITION_FUNCTION_NAME: &str = "qdrant_condition";
pub const GEO_DISTANCE_FUNCTION_NAME: &str = "qdrant_geo_distance";
pub const EXP_DECAY_FUNCTION_NAME: &str = "qdrant_exp_decay";
pub const GAUSS_DECAY_FUNCTION_NAME: &str = "qdrant_gauss_decay";
pub const LIN_DECAY_FUNCTION_NAME: &str = "qdrant_lin_decay";

const FORMULA_ALIASES: &[&str] = &["formula_score"];
const PAYLOAD_NUM_ALIASES: &[&str] = &["payload_num"];
const PAYLOAD_DATETIME_ALIASES: &[&str] = &["payload_datetime"];
const DATETIME_VALUE_ALIASES: &[&str] = &["datetime_value"];
const CONDITION_ALIASES: &[&str] = &["condition"];
const GEO_DISTANCE_ALIASES: &[&str] = &["geo_distance"];
const EXP_DECAY_ALIASES: &[&str] = &["exp_decay"];
const GAUSS_DECAY_ALIASES: &[&str] = &["gauss_decay"];
const LIN_DECAY_ALIASES: &[&str] = &["lin_decay"];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QdrantFormulaNumericDefault {
    Integer(i64),
    Float(f64),
}

impl QdrantFormulaNumericDefault {
    fn into_expr(self) -> Expr {
        match self {
            Self::Integer(value) => lit(value),
            Self::Float(value) => lit(value),
        }
    }
}

impl From<i8> for QdrantFormulaNumericDefault {
    fn from(value: i8) -> Self { Self::Integer(i64::from(value)) }
}

impl From<i16> for QdrantFormulaNumericDefault {
    fn from(value: i16) -> Self { Self::Integer(i64::from(value)) }
}

impl From<i32> for QdrantFormulaNumericDefault {
    fn from(value: i32) -> Self { Self::Integer(i64::from(value)) }
}

impl From<i64> for QdrantFormulaNumericDefault {
    fn from(value: i64) -> Self { Self::Integer(value) }
}

impl From<u8> for QdrantFormulaNumericDefault {
    fn from(value: u8) -> Self { Self::Integer(i64::from(value)) }
}

impl From<u16> for QdrantFormulaNumericDefault {
    fn from(value: u16) -> Self { Self::Integer(i64::from(value)) }
}

impl From<u32> for QdrantFormulaNumericDefault {
    fn from(value: u32) -> Self { Self::Integer(i64::from(value)) }
}

impl From<f32> for QdrantFormulaNumericDefault {
    fn from(value: f32) -> Self { Self::Float(f64::from(value)) }
}

impl From<f64> for QdrantFormulaNumericDefault {
    fn from(value: f64) -> Self { Self::Float(value) }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QdrantPayloadNum {
    path:    String,
    default: Option<QdrantFormulaNumericDefault>,
}

impl QdrantPayloadNum {
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self { Self { path: path.into(), default: None } }

    #[must_use]
    pub fn with_default(mut self, default: impl Into<QdrantFormulaNumericDefault>) -> Self {
        self.default = Some(default.into());
        self
    }
}

impl From<&str> for QdrantPayloadNum {
    fn from(path: &str) -> Self { Self::new(path) }
}

impl From<String> for QdrantPayloadNum {
    fn from(path: String) -> Self { Self::new(path) }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantPayloadDatetime {
    path:    String,
    default: Option<String>,
}

impl QdrantPayloadDatetime {
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self { Self { path: path.into(), default: None } }

    #[must_use]
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }
}

impl From<&str> for QdrantPayloadDatetime {
    fn from(path: &str) -> Self { Self::new(path) }
}

impl From<String> for QdrantPayloadDatetime {
    fn from(path: String) -> Self { Self::new(path) }
}

#[derive(Debug, Clone)]
pub enum QdrantDecay {
    Scale { scale: f32 },
    Target { target: Expr, scale: f32, midpoint: Option<f32> },
}

impl QdrantDecay {
    #[must_use]
    pub fn new(scale: f32) -> Self { Self::Scale { scale } }

    #[must_use]
    pub fn towards(target: Expr, scale: f32) -> Self {
        Self::Target { target, scale, midpoint: None }
    }

    #[must_use]
    pub fn towards_with_midpoint(target: Expr, scale: f32, midpoint: f32) -> Self {
        Self::Target { target, scale, midpoint: Some(midpoint) }
    }

    fn into_args(self, x: Expr) -> Vec<Expr> {
        match self {
            Self::Scale { scale } => vec![x, lit(scale)],
            Self::Target { target, scale, midpoint } => {
                let mut args = vec![x, target, lit(scale)];
                if let Some(midpoint) = midpoint {
                    args.push(lit(midpoint));
                }
                args
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FormulaCall {
    pub(crate) formula: Expr,
}

impl FormulaCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, FORMULA_SCORE_FUNCTION_NAME, FORMULA_ALIASES) else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!("{FORMULA_SCORE_FUNCTION_NAME} requires a single formula expression");
        }
        Ok(Some(Self { formula: args[0].clone() }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PayloadNumCall {
    pub(crate) path:    Expr,
    pub(crate) default: Option<Expr>,
}

impl PayloadNumCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, PAYLOAD_NUM_FUNCTION_NAME, PAYLOAD_NUM_ALIASES) else {
            return Ok(None);
        };
        match args {
            [path] => Ok(Some(Self { path: path.clone(), default: None })),
            [path, default] => {
                Ok(Some(Self { path: path.clone(), default: Some(default.clone()) }))
            }
            _ => plan_err!(
                "{PAYLOAD_NUM_FUNCTION_NAME} requires a payload path and an optional numeric \
                 default"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PayloadDatetimeCall {
    pub(crate) path:    Expr,
    pub(crate) default: Option<Expr>,
}

impl PayloadDatetimeCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) =
            function_args(expr, PAYLOAD_DATETIME_FUNCTION_NAME, PAYLOAD_DATETIME_ALIASES)
        else {
            return Ok(None);
        };
        match args {
            [path] => Ok(Some(Self { path: path.clone(), default: None })),
            [path, default] => {
                Ok(Some(Self { path: path.clone(), default: Some(default.clone()) }))
            }
            _ => plan_err!(
                "{PAYLOAD_DATETIME_FUNCTION_NAME} requires a payload path and an optional \
                 datetime default"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DatetimeValueCall {
    pub(crate) value: Expr,
}

impl DatetimeValueCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, DATETIME_VALUE_FUNCTION_NAME, DATETIME_VALUE_ALIASES)
        else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!(
                "{DATETIME_VALUE_FUNCTION_NAME} requires exactly one datetime literal"
            );
        }
        Ok(Some(Self { value: args[0].clone() }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConditionCall {
    pub(crate) predicate: Expr,
}

impl ConditionCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, CONDITION_FUNCTION_NAME, CONDITION_ALIASES) else {
            return Ok(None);
        };
        if args.len() != 1 {
            return plan_err!(
                "{CONDITION_FUNCTION_NAME} requires exactly one predicate expression"
            );
        }
        Ok(Some(Self { predicate: args[0].clone() }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GeoDistanceCall {
    pub(crate) path: Expr,
    pub(crate) lon:  Expr,
    pub(crate) lat:  Expr,
}

impl GeoDistanceCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Some(args) = function_args(expr, GEO_DISTANCE_FUNCTION_NAME, GEO_DISTANCE_ALIASES)
        else {
            return Ok(None);
        };
        let [path, lon, lat] = args else {
            return plan_err!(
                "{GEO_DISTANCE_FUNCTION_NAME} requires a payload path, longitude, and latitude"
            );
        };
        Ok(Some(Self { path: path.clone(), lon: lon.clone(), lat: lat.clone() }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecayKind {
    Exp,
    Gauss,
    Lin,
}

impl DecayKind {
    pub(crate) fn function_name(self) -> &'static str {
        match self {
            Self::Exp => EXP_DECAY_FUNCTION_NAME,
            Self::Gauss => GAUSS_DECAY_FUNCTION_NAME,
            Self::Lin => LIN_DECAY_FUNCTION_NAME,
        }
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Exp => EXP_DECAY_ALIASES,
            Self::Gauss => GAUSS_DECAY_ALIASES,
            Self::Lin => LIN_DECAY_ALIASES,
        }
    }

    fn iter() -> impl Iterator<Item = Self> { [Self::Exp, Self::Gauss, Self::Lin].into_iter() }
}

#[derive(Debug, Clone)]
pub(crate) struct DecayCall {
    pub(crate) kind:     DecayKind,
    pub(crate) x:        Expr,
    pub(crate) target:   Option<Expr>,
    pub(crate) scale:    Expr,
    pub(crate) midpoint: Option<Expr>,
}

impl DecayCall {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        for kind in DecayKind::iter() {
            let Some(args) = function_args(expr, kind.function_name(), kind.aliases()) else {
                continue;
            };
            return match args {
                [x, scale] => Ok(Some(Self {
                    kind,
                    x: x.clone(),
                    target: None,
                    scale: scale.clone(),
                    midpoint: None,
                })),
                [x, target, scale] => Ok(Some(Self {
                    kind,
                    x: x.clone(),
                    target: Some(target.clone()),
                    scale: scale.clone(),
                    midpoint: None,
                })),
                [x, target, scale, midpoint] => Ok(Some(Self {
                    kind,
                    x: x.clone(),
                    target: Some(target.clone()),
                    scale: scale.clone(),
                    midpoint: Some(midpoint.clone()),
                })),
                _ => plan_err!(
                    "{} requires (x, scale), (x, target, scale), or (x, target, scale, midpoint)",
                    kind.function_name()
                ),
            };
        }
        Ok(None)
    }
}

#[must_use]
pub fn qdrant_formula_score(formula: Expr) -> Expr {
    qdrant_formula_score_udf().call(vec![formula])
}

#[must_use]
pub fn qdrant_payload_num(path: impl Into<QdrantPayloadNum>) -> Expr {
    let path = path.into();
    let mut args = vec![lit(path.path)];
    if let Some(default) = path.default {
        args.push(default.into_expr());
    }
    qdrant_payload_num_udf().call(args)
}

#[must_use]
pub fn qdrant_payload_datetime(path: impl Into<QdrantPayloadDatetime>) -> Expr {
    let path = path.into();
    let mut args = vec![lit(path.path)];
    if let Some(default) = path.default {
        args.push(lit(default));
    }
    qdrant_payload_datetime_udf().call(args)
}

#[must_use]
pub fn qdrant_datetime_value(value: impl Into<String>) -> Expr {
    qdrant_datetime_value_udf().call(vec![lit(value.into())])
}

#[must_use]
pub fn qdrant_condition(predicate: Expr) -> Expr { qdrant_condition_udf().call(vec![predicate]) }

#[must_use]
pub fn qdrant_geo_distance(path: impl Into<String>, lon: f64, lat: f64) -> Expr {
    qdrant_geo_distance_udf().call(vec![lit(path.into()), lit(lon), lit(lat)])
}

#[must_use]
pub fn qdrant_exp_decay(x: Expr, decay: QdrantDecay) -> Expr {
    qdrant_exp_decay_udf().call(decay.into_args(x))
}

#[must_use]
pub fn qdrant_gauss_decay(x: Expr, decay: QdrantDecay) -> Expr {
    qdrant_gauss_decay_udf().call(decay.into_args(x))
}

#[must_use]
pub fn qdrant_lin_decay(x: Expr, decay: QdrantDecay) -> Expr {
    qdrant_lin_decay_udf().call(decay.into_args(x))
}

pub(crate) fn qdrant_formula_score_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            FORMULA_SCORE_FUNCTION_NAME,
            FORMULA_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_payload_num_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            PAYLOAD_NUM_FUNCTION_NAME,
            PAYLOAD_NUM_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_payload_datetime_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            PAYLOAD_DATETIME_FUNCTION_NAME,
            PAYLOAD_DATETIME_ALIASES,
            DataType::Utf8,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_datetime_value_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            DATETIME_VALUE_FUNCTION_NAME,
            DATETIME_VALUE_ALIASES,
            DataType::Utf8,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_condition_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            CONDITION_FUNCTION_NAME,
            CONDITION_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_geo_distance_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            GEO_DISTANCE_FUNCTION_NAME,
            GEO_DISTANCE_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_exp_decay_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            EXP_DECAY_FUNCTION_NAME,
            EXP_DECAY_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_gauss_decay_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            GAUSS_DECAY_FUNCTION_NAME,
            GAUSS_DECAY_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}

pub(crate) fn qdrant_lin_decay_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(NonExecutableMarkerUdf::new(
            LIN_DECAY_FUNCTION_NAME,
            LIN_DECAY_ALIASES,
            DataType::Float32,
        ))
    })
    .clone()
}
