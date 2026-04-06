use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::lit;

use super::payload_access::payload_json_value;
use crate::qdrant::QdrantPayloadAccess;

pub const PAYLOAD_GEO_DISTANCE_FUNCTION_NAME: &str = "payload_geo_distance";
pub(crate) const PAYLOAD_GEO_DISTANCE_ACCESS_FUNCTION_NAME: &str =
    "__qdrant_payload_geo_distance_access";

const PAYLOAD_GEO_DISTANCE_ALIASES: &[&str] = &["qdrant_payload_geo_distance"];
const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

pub(crate) fn is_payload_geo_distance_function_name(name: &str) -> bool {
    name == PAYLOAD_GEO_DISTANCE_FUNCTION_NAME || PAYLOAD_GEO_DISTANCE_ALIASES.contains(&name)
}

#[must_use]
pub fn qdrant_payload_geo_distance(accessor: Expr, lon: Expr, lat: Expr) -> Expr {
    qdrant_payload_geo_distance_udf().call(vec![accessor, lon, lat])
}

pub(crate) fn qdrant_payload_geo_distance_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadGeoDistanceUdf::new())).clone()
}

pub(crate) fn qdrant_payload_geo_distance_access_udf() -> ScalarUDF {
    static UDF: OnceLock<ScalarUDF> = OnceLock::new();
    UDF.get_or_init(|| ScalarUDF::new_from_impl(PayloadGeoDistanceAccessUdf::new())).clone()
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
    use datafusion::common::ScalarValue;

    use super::payload_geo_distance_scalar;

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
}
