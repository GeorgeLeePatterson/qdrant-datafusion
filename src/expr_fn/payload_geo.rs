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
use super::payload_access::payload_json_value;
use crate::qdrant::QdrantPayloadAccess;

pub const PAYLOAD_GEO_DISTANCE_FUNCTION_NAME: &str = "payload_geo_distance";
pub const PAYLOAD_GEO_WITHIN_BBOX_FUNCTION_NAME: &str = "payload_geo_within_bbox";
pub const PAYLOAD_GEO_WITHIN_POLYGON_FUNCTION_NAME: &str = "payload_geo_within_polygon";
pub(crate) const PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_geo_distance_access";
pub(crate) const PAYLOAD_GEO_WITHIN_BBOX_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_geo_within_bbox_access";
pub(crate) const PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_geo_within_polygon_access";

const PAYLOAD_GEO_DISTANCE_ALIASES: &[&str] = &["qdrant_payload_geo_distance"];
const PAYLOAD_GEO_WITHIN_BBOX_ALIASES: &[&str] = &["qdrant_payload_geo_within_bbox"];
const PAYLOAD_GEO_WITHIN_POLYGON_ALIASES: &[&str] = &["qdrant_payload_geo_within_polygon"];
const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

pub(crate) fn is_payload_geo_distance_function_name(name: &str) -> bool {
    name == PAYLOAD_GEO_DISTANCE_FUNCTION_NAME || PAYLOAD_GEO_DISTANCE_ALIASES.contains(&name)
}

pub(crate) fn is_payload_geo_within_bbox_function_name(name: &str) -> bool {
    name == PAYLOAD_GEO_WITHIN_BBOX_FUNCTION_NAME || PAYLOAD_GEO_WITHIN_BBOX_ALIASES.contains(&name)
}

pub(crate) fn is_payload_geo_within_polygon_function_name(name: &str) -> bool {
    name == PAYLOAD_GEO_WITHIN_POLYGON_FUNCTION_NAME
        || PAYLOAD_GEO_WITHIN_POLYGON_ALIASES.contains(&name)
}

#[must_use]
pub fn qdrant_payload_geo_distance(accessor: Expr, lon: Expr, lat: Expr) -> Expr {
    qdrant_payload_geo_distance_udf().call(vec![accessor, lon, lat])
}

#[must_use]
pub fn qdrant_payload_geo_within_bbox(
    accessor: Expr,
    lon1: Expr,
    lat1: Expr,
    lon2: Expr,
    lat2: Expr,
) -> Expr {
    qdrant_payload_geo_within_bbox_udf().call(vec![accessor, lon1, lat1, lon2, lat2])
}

#[must_use]
pub fn qdrant_payload_geo_within_polygon(accessor: Expr, points: Expr) -> Expr {
    qdrant_payload_geo_within_polygon_udf().call(vec![accessor, points])
}

pub(crate) fn qdrant_payload_geo_distance_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadGeoDistanceUdf::new())).clone()
}

pub(crate) fn qdrant_payload_geo_within_bbox_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadGeoPredicateUdf::within_bbox()))
        .clone()
}

pub(crate) fn qdrant_payload_geo_within_polygon_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadGeoPredicateUdf::within_polygon()))
        .clone()
}

pub(crate) fn qdrant_payload_geo_distance_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadGeoDistanceAccessUdf::new())).clone()
}

pub(crate) fn qdrant_payload_geo_within_bbox_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadGeoPredicateAccessUdf::new(PayloadGeoPredicateKind::Bbox))
    })
    .clone()
}

pub(crate) fn qdrant_payload_geo_within_polygon_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| {
        ScalarUDF::new_from_impl(PayloadGeoPredicateAccessUdf::new(
            PayloadGeoPredicateKind::Polygon,
        ))
    })
    .clone()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadGeoDistanceUdf {
    aliases:   Vec<String>,
    signature: Signature,
}

impl PayloadGeoDistanceUdf {
    fn new() -> Self {
        Self {
            aliases:   PAYLOAD_GEO_DISTANCE_ALIASES
                .iter()
                .map(|alias| (*alias).to_owned())
                .collect(),
            signature: Signature::any(3, Volatility::Immutable)
                .with_parameter_names(vec!["accessor", "longitude", "latitude"])
                .expect("payload geo distance signature should accept named parameters"),
        }
    }
}

impl ScalarUDFImpl for PayloadGeoDistanceUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { PAYLOAD_GEO_DISTANCE_FUNCTION_NAME }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, true)))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [accessor, lon, lat] = args.as_slice() else {
            return plan_err!(
                "{} requires a qdrant payload accessor, longitude, and latitude",
                self.name()
            );
        };
        let Some(access) = QdrantPayloadAccess::from_logical_expr(accessor) else {
            return plan_err!("{} accessor must be a qdrant payload path", self.name());
        };
        let (payload, path) = access.into_parts();
        Ok(ExprSimplifyResult::Simplified(qdrant_payload_geo_distance_access_udf().call(vec![
            payload,
            lit(path),
            lon.clone(),
            lat.clone(),
        ])))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} requires qdrant payload geo-distance simplification", self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PayloadGeoPredicateKind {
    Bbox,
    Polygon,
}

impl PayloadGeoPredicateKind {
    fn function_name(self) -> &'static str {
        match self {
            Self::Bbox => PAYLOAD_GEO_WITHIN_BBOX_FUNCTION_NAME,
            Self::Polygon => PAYLOAD_GEO_WITHIN_POLYGON_FUNCTION_NAME,
        }
    }

    fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Bbox => PAYLOAD_GEO_WITHIN_BBOX_ALIASES,
            Self::Polygon => PAYLOAD_GEO_WITHIN_POLYGON_ALIASES,
        }
    }

    fn internal_function_name(self) -> &'static str {
        match self {
            Self::Bbox => PAYLOAD_GEO_WITHIN_BBOX_ACCESS_FUNCTION_NAME,
            Self::Polygon => PAYLOAD_GEO_WITHIN_POLYGON_ACCESS_FUNCTION_NAME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadGeoPredicateUdf {
    kind:      PayloadGeoPredicateKind,
    aliases:   Vec<String>,
    signature: Signature,
}

impl PayloadGeoPredicateUdf {
    fn new(kind: PayloadGeoPredicateKind) -> Self {
        let signature = match kind {
            PayloadGeoPredicateKind::Bbox => Signature::any(5, Volatility::Immutable)
                .with_parameter_names(vec![
                    "accessor",
                    "longitude_1",
                    "latitude_1",
                    "longitude_2",
                    "latitude_2",
                ])
                .expect("payload geo bbox signature should accept named parameters"),
            PayloadGeoPredicateKind::Polygon => Signature::any(2, Volatility::Immutable)
                .with_parameter_names(vec!["accessor", "points"])
                .expect("payload geo polygon signature should accept named parameters"),
        };
        Self {
            kind,
            aliases: kind.aliases().iter().map(|alias| (*alias).to_owned()).collect(),
            signature,
        }
    }

    fn within_bbox() -> Self { Self::new(PayloadGeoPredicateKind::Bbox) }

    fn within_polygon() -> Self { Self::new(PayloadGeoPredicateKind::Polygon) }
}

impl ScalarUDFImpl for PayloadGeoPredicateUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.kind.function_name() }

    fn aliases(&self) -> &[String] { &self.aliases }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let Some(accessor) = args.first() else {
            return plan_err!("{} requires a qdrant payload accessor", self.name());
        };
        let Some(access) = QdrantPayloadAccess::from_logical_expr(accessor) else {
            return plan_err!("{} accessor must be a qdrant payload path", self.name());
        };
        let (payload, path) = access.into_parts();
        let expr = match self.kind {
            PayloadGeoPredicateKind::Bbox => {
                let [_, lon1, lat1, lon2, lat2] = args.as_slice() else {
                    return plan_err!(
                        "{} requires a qdrant payload accessor and two geo corners",
                        self.name()
                    );
                };
                qdrant_payload_geo_within_bbox_access_udf().call(vec![
                    payload,
                    lit(path),
                    lon1.clone(),
                    lat1.clone(),
                    lon2.clone(),
                    lat2.clone(),
                ])
            }
            PayloadGeoPredicateKind::Polygon => {
                let [_, points] = args.as_slice() else {
                    return plan_err!(
                        "{} requires a qdrant payload accessor and a polygon point array",
                        self.name()
                    );
                };
                qdrant_payload_geo_within_polygon_access_udf().call(vec![
                    payload,
                    lit(path),
                    lit(canonical_geo_polygon_scalar(
                        &literal_scalar(
                            points,
                            self.name(),
                            "points",
                            "an array literal of [lon, lat] pairs",
                        )?,
                        self.name(),
                        "points",
                    )?),
                ])
            }
        };
        Ok(ExprSimplifyResult::Simplified(expr))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} requires qdrant payload geo simplification", self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadGeoDistanceAccessUdf {
    signature: Signature,
}

impl PayloadGeoDistanceAccessUdf {
    fn new() -> Self { Self { signature: Signature::any(4, Volatility::Immutable) } }
}

impl ScalarUDFImpl for PayloadGeoDistanceAccessUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 4 {
            return exec_err!(
                "{} requires a payload JSON value, payload path, longitude, and latitude",
                self.name()
            );
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let payloads = &arrays[0];
        let paths = &arrays[1];
        let longitudes = &arrays[2];
        let latitudes = &arrays[3];
        let mut values = Vec::with_capacity(payloads.len());
        for index in 0..payloads.len() {
            values.push(payload_geo_distance_scalar(
                string_scalar_from_array(payloads, index, "payload")?.as_deref(),
                string_scalar_from_array(paths, index, "payload path")?.as_deref(),
                numeric_scalar_from_array(longitudes, index, "longitude")?,
                numeric_scalar_from_array(latitudes, index, "latitude")?,
            )?);
        }
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PayloadGeoPredicateAccessUdf {
    kind:      PayloadGeoPredicateKind,
    signature: Signature,
}

impl PayloadGeoPredicateAccessUdf {
    fn new(kind: PayloadGeoPredicateKind) -> Self {
        Self { kind, signature: Signature::variadic_any(Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for PayloadGeoPredicateAccessUdf {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { self.kind.internal_function_name() }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> { Ok(DataType::Boolean) }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs<'_>,
    ) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        match self.kind {
            PayloadGeoPredicateKind::Bbox => {
                if arrays.len() != 6 {
                    return exec_err!(
                        "{} requires a payload JSON value, payload path, and two geo corners",
                        self.kind.function_name()
                    );
                }
                let payloads = &arrays[0];
                let paths = &arrays[1];
                let lons1 = &arrays[2];
                let lats1 = &arrays[3];
                let lons2 = &arrays[4];
                let lats2 = &arrays[5];
                let mut values = Vec::with_capacity(payloads.len());
                for index in 0..payloads.len() {
                    values.push(payload_geo_within_bbox_scalar(
                        string_scalar_from_array(payloads, index, "payload")?.as_deref(),
                        string_scalar_from_array(paths, index, "payload path")?.as_deref(),
                        numeric_scalar_from_array(lons1, index, "longitude_1")?,
                        numeric_scalar_from_array(lats1, index, "latitude_1")?,
                        numeric_scalar_from_array(lons2, index, "longitude_2")?,
                        numeric_scalar_from_array(lats2, index, "latitude_2")?,
                    )?);
                }
                Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
            }
            PayloadGeoPredicateKind::Polygon => {
                if arrays.len() != 3 {
                    return exec_err!(
                        "{} requires a payload JSON value, payload path, and polygon points",
                        self.kind.function_name()
                    );
                }
                let payloads = &arrays[0];
                let paths = &arrays[1];
                let polygons = &arrays[2];
                let mut values = Vec::with_capacity(payloads.len());
                for index in 0..payloads.len() {
                    let polygon = canonical_geo_polygon(
                        &ScalarValue::try_from_array(polygons, index)?,
                        self.kind.function_name(),
                        "points",
                    )?;
                    values.push(payload_geo_within_polygon_scalar(
                        string_scalar_from_array(payloads, index, "payload")?.as_deref(),
                        string_scalar_from_array(paths, index, "payload path")?.as_deref(),
                        &polygon,
                    )?);
                }
                Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
            }
        }
    }
}

fn payload_geo_distance_scalar(
    payload: Option<&str>,
    path: Option<&str>,
    lon: Option<f64>,
    lat: Option<f64>,
) -> Result<ScalarValue> {
    let Some(payload) = payload else {
        return Ok(ScalarValue::Float64(None));
    };
    let Some(path) = path else {
        return Ok(ScalarValue::Float64(None));
    };
    let Some(lon) = lon else {
        return Ok(ScalarValue::Float64(None));
    };
    let Some(lat) = lat else {
        return Ok(ScalarValue::Float64(None));
    };
    let Some(value) =
        payload_json_value(Some(payload), Some(path), PAYLOAD_GEO_DISTANCE_FUNCTION_NAME)?
    else {
        return Ok(ScalarValue::Float64(None));
    };
    let Some((point_lon, point_lat)) = geo_point_value(&value, path)? else {
        return Ok(ScalarValue::Float64(None));
    };
    Ok(ScalarValue::Float64(Some(haversine_distance_meters(point_lon, point_lat, lon, lat))))
}

fn payload_geo_within_bbox_scalar(
    payload: Option<&str>,
    path: Option<&str>,
    lon1: Option<f64>,
    lat1: Option<f64>,
    lon2: Option<f64>,
    lat2: Option<f64>,
) -> Result<ScalarValue> {
    let Some(payload) = payload else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(path) = path else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(lon1) = lon1 else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(lat1) = lat1 else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(lon2) = lon2 else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(lat2) = lat2 else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(value) =
        payload_json_value(Some(payload), Some(path), PAYLOAD_GEO_WITHIN_BBOX_FUNCTION_NAME)?
    else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some((point_lon, point_lat)) = geo_point_value(&value, path)? else {
        return Ok(ScalarValue::Boolean(None));
    };
    let (west, south, east, north) = normalized_bbox(lon1, lat1, lon2, lat2)?;
    Ok(ScalarValue::Boolean(Some(
        point_lon >= west && point_lon <= east && point_lat >= south && point_lat <= north,
    )))
}

fn payload_geo_within_polygon_scalar(
    payload: Option<&str>,
    path: Option<&str>,
    polygon: &[(f64, f64)],
) -> Result<ScalarValue> {
    let Some(payload) = payload else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(path) = path else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some(value) =
        payload_json_value(Some(payload), Some(path), PAYLOAD_GEO_WITHIN_POLYGON_FUNCTION_NAME)?
    else {
        return Ok(ScalarValue::Boolean(None));
    };
    let Some((point_lon, point_lat)) = geo_point_value(&value, path)? else {
        return Ok(ScalarValue::Boolean(None));
    };
    Ok(ScalarValue::Boolean(Some(point_in_polygon((point_lon, point_lat), polygon))))
}

fn geo_point_value(value: &serde_json::Value, path: &str) -> Result<Option<(f64, f64)>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Object(object) => {
            let Some(lon) = object.get("lon") else {
                return exec_err!(
                    "payload_geo_distance expected '{}' to contain a geo object with 'lon' and \
                     'lat' fields",
                    path
                );
            };
            let Some(lat) = object.get("lat") else {
                return exec_err!(
                    "payload_geo_distance expected '{}' to contain a geo object with 'lon' and \
                     'lat' fields",
                    path
                );
            };
            let Some(lon) = lon.as_f64() else {
                return exec_err!(
                    "payload_geo_distance expected '{}' geo longitude to be numeric",
                    path
                );
            };
            let Some(lat) = lat.as_f64() else {
                return exec_err!(
                    "payload_geo_distance expected '{}' geo latitude to be numeric",
                    path
                );
            };
            Ok(Some((lon, lat)))
        }
        _ => exec_err!("payload_geo_distance expected '{}' to resolve to a geo object", path),
    }
}

fn normalized_bbox(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> Result<(f64, f64, f64, f64)> {
    if !lon1.is_finite() || !lat1.is_finite() || !lon2.is_finite() || !lat2.is_finite() {
        return exec_err!("payload_geo_within_bbox requires finite coordinates");
    }
    Ok((lon1.min(lon2), lat1.min(lat2), lon1.max(lon2), lat1.max(lat2)))
}

pub(crate) fn canonical_geo_polygon_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<ScalarValue> {
    let polygon = canonical_geo_polygon(value, function_name, argument)?;
    let point_values = polygon
        .iter()
        .map(|(lon, lat)| {
            ScalarValue::List(ScalarValue::new_list_nullable(
                &[ScalarValue::Float64(Some(*lon)), ScalarValue::Float64(Some(*lat))],
                &DataType::Float64,
            ))
        })
        .collect::<Vec<_>>();
    Ok(ScalarValue::List(ScalarValue::new_list_nullable(
        &point_values,
        &DataType::new_list(DataType::Float64, true),
    )))
}

pub(crate) fn canonical_geo_polygon(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<Vec<(f64, f64)>> {
    let points = match value {
        ScalarValue::List(array) => {
            if array.is_empty() {
                vec![]
            } else {
                geo_polygon_points(array.value(0).as_ref(), function_name, argument)?
            }
        }
        ScalarValue::LargeList(array) => {
            if array.is_empty() {
                vec![]
            } else {
                geo_polygon_points(array.value(0).as_ref(), function_name, argument)?
            }
        }
        _ => {
            return plan_err!(
                "{function_name} requires {argument} to be an array literal of [lon, lat] pairs"
            );
        }
    };
    if points.len() < 3 {
        return plan_err!(
            "{function_name} requires {argument} to contain at least three [lon, lat] points"
        );
    }
    let mut closed = points;
    if closed.first() != closed.last()
        && let Some(first) = closed.first().copied()
    {
        closed.push(first);
    }
    Ok(closed)
}

fn geo_polygon_points(
    array: &dyn Array,
    function_name: &str,
    argument: &str,
) -> Result<Vec<(f64, f64)>> {
    (0..array.len())
        .map(|index| {
            let value = ScalarValue::try_from_array(array, index)?;
            match value {
                ScalarValue::List(point) => {
                    parse_geo_polygon_point(point.value(0).as_ref(), function_name, argument)
                }
                ScalarValue::LargeList(point) => {
                    parse_geo_polygon_point(point.value(0).as_ref(), function_name, argument)
                }
                _ => plan_err!(
                    "{function_name} requires {argument} to be an array literal of [lon, lat] \
                     pairs"
                ),
            }
        })
        .collect()
}

fn parse_geo_polygon_point(
    array: &dyn Array,
    function_name: &str,
    argument: &str,
) -> Result<(f64, f64)> {
    if array.len() != 2 {
        return plan_err!("{function_name} requires each {argument} point to contain [lon, lat]");
    }
    let lon = numeric_scalar_from_scalar(
        &ScalarValue::try_from_array(array, 0)?,
        function_name,
        "longitude",
    )?;
    let lat = numeric_scalar_from_scalar(
        &ScalarValue::try_from_array(array, 1)?,
        function_name,
        "latitude",
    )?;
    Ok((lon, lat))
}

fn numeric_scalar_from_scalar(
    value: &ScalarValue,
    function_name: &str,
    argument: &str,
) -> Result<f64> {
    #[expect(clippy::cast_precision_loss)]
    match value {
        ScalarValue::Float64(Some(value)) => Ok(*value),
        ScalarValue::Float32(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::Int8(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::Int16(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::Int64(Some(value)) => Ok(*value as f64),
        ScalarValue::UInt8(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Ok(f64::from(*value)),
        ScalarValue::UInt64(Some(value)) => Ok(*value as f64),
        _ => exec_err!("{} requires {argument} to be numeric", function_name),
    }
}

fn point_in_polygon(point: (f64, f64), polygon: &[(f64, f64)]) -> bool {
    if polygon.len() < 4 {
        return false;
    }
    let (x, y) = point;
    let mut inside = false;
    for edge in polygon.windows(2) {
        let start = edge[0];
        let end = edge[1];
        if point_on_segment(point, start, end) {
            return true;
        }
        let intersects = ((start.1 > y) != (end.1 > y))
            && (x < (end.0 - start.0) * (y - start.1) / (end.1 - start.1) + start.0);
        if intersects {
            inside = !inside;
        }
    }
    inside
}

fn point_on_segment(point: (f64, f64), start: (f64, f64), end: (f64, f64)) -> bool {
    let (x, y) = point;
    let cross = (y - start.1) * (end.0 - start.0) - (x - start.0) * (end.1 - start.1);
    if cross.abs() > 1e-9 {
        return false;
    }
    let dot = (x - start.0) * (end.0 - start.0) + (y - start.1) * (end.1 - start.1);
    if dot < 0.0 {
        return false;
    }
    let squared_len = (end.0 - start.0).powi(2) + (end.1 - start.1).powi(2);
    dot <= squared_len
}

fn string_scalar_from_array(
    array: &datafusion::arrow::array::ArrayRef,
    index: usize,
    argument: &str,
) -> Result<Option<String>> {
    let value = ScalarValue::try_from_array(array, index)?;
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => Ok(value),
        ScalarValue::Null => Ok(None),
        _ => exec_err!("payload_geo_distance requires {argument} to be Utf8"),
    }
}

#[expect(clippy::cast_precision_loss)]
fn numeric_scalar_from_array(
    array: &datafusion::arrow::array::ArrayRef,
    index: usize,
    argument: &str,
) -> Result<Option<f64>> {
    let value = ScalarValue::try_from_array(array, index)?;
    match value {
        ScalarValue::Float64(value) => Ok(value),
        ScalarValue::Float32(value) => Ok(value.map(f64::from)),
        ScalarValue::Int8(value) => Ok(value.map(f64::from)),
        ScalarValue::Int16(value) => Ok(value.map(f64::from)),
        ScalarValue::Int32(value) => Ok(value.map(f64::from)),
        ScalarValue::Int64(value) => Ok(value.map(|value| value as f64)),
        ScalarValue::UInt8(value) => Ok(value.map(f64::from)),
        ScalarValue::UInt16(value) => Ok(value.map(f64::from)),
        ScalarValue::UInt32(value) => Ok(value.map(f64::from)),
        ScalarValue::UInt64(value) => Ok(value.map(|value| value as f64)),
        ScalarValue::Null => Ok(None),
        _ => exec_err!("payload_geo_distance requires {argument} to be numeric"),
    }
}

fn haversine_distance_meters(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_METERS * c
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;

    use super::{
        PAYLOAD_GEO_WITHIN_POLYGON_FUNCTION_NAME, canonical_geo_polygon,
        payload_geo_distance_scalar, payload_geo_within_bbox_scalar,
        payload_geo_within_polygon_scalar,
    };

    #[test]
    fn payload_geo_distance_returns_zero_for_same_point() {
        let value = payload_geo_distance_scalar(
            Some(r#"{"location":{"lon":0.0,"lat":0.0}}"#),
            Some("location"),
            Some(0.0),
            Some(0.0),
        )
        .expect("distance");
        assert_eq!(value, ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn payload_geo_distance_returns_null_for_missing_path() {
        let value = payload_geo_distance_scalar(
            Some(r#"{"other":{"lon":0.0,"lat":0.0}}"#),
            Some("location"),
            Some(0.0),
            Some(0.0),
        )
        .expect("distance");
        assert_eq!(value, ScalarValue::Float64(None));
    }

    #[test]
    fn payload_geo_within_bbox_matches_points_inside_box() {
        let value = payload_geo_within_bbox_scalar(
            Some(r#"{"location":{"lon":0.0,"lat":1.0}}"#),
            Some("location"),
            Some(-1.0),
            Some(-1.0),
            Some(1.0),
            Some(1.5),
        )
        .expect("bbox");
        assert_eq!(value, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn payload_geo_within_polygon_auto_closes_ring() {
        let polygon = canonical_geo_polygon(
            &ScalarValue::List(ScalarValue::new_list_nullable(
                &[
                    ScalarValue::List(ScalarValue::new_list_nullable(
                        &[ScalarValue::Float64(Some(-1.0)), ScalarValue::Float64(Some(-1.0))],
                        &DataType::Float64,
                    )),
                    ScalarValue::List(ScalarValue::new_list_nullable(
                        &[ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(-1.0))],
                        &DataType::Float64,
                    )),
                    ScalarValue::List(ScalarValue::new_list_nullable(
                        &[ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(1.5))],
                        &DataType::Float64,
                    )),
                    ScalarValue::List(ScalarValue::new_list_nullable(
                        &[ScalarValue::Float64(Some(-1.0)), ScalarValue::Float64(Some(1.5))],
                        &DataType::Float64,
                    )),
                ],
                &DataType::new_list(DataType::Float64, true),
            )),
            PAYLOAD_GEO_WITHIN_POLYGON_FUNCTION_NAME,
            "points",
        )
        .expect("polygon");
        let value = payload_geo_within_polygon_scalar(
            Some(r#"{"location":{"lon":0.0,"lat":1.0}}"#),
            Some("location"),
            &polygon,
        )
        .expect("polygon match");
        assert_eq!(value, ScalarValue::Boolean(Some(true)));
    }
}
