use qdrant_client::qdrant::{Condition, DatetimeRange, Filter, Range};

use super::{QdrantFilterExpr, QdrantFilterValue, QdrantPayloadPath, QdrantPredicate};
use crate::qdrant::QdrantPayloadField;

impl QdrantPredicate {
    pub(super) fn from_disjunction(exprs: &[QdrantFilterExpr]) -> Option<Self> {
        match exprs {
            [QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(_)), ..] => exprs
                .iter()
                .map(|expr| match expr {
                    QdrantFilterExpr::Predicate(QdrantPredicate::IdIn(ids)) if ids.len() == 1 => {
                        Some(ids[0].clone())
                    }
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(QdrantPredicate::IdIn),
            [QdrantFilterExpr::Predicate(QdrantPredicate::PayloadEq { field, .. }), ..] => exprs
                .iter()
                .map(|expr| match expr {
                    QdrantFilterExpr::Predicate(QdrantPredicate::PayloadEq {
                        field: next_field,
                        value,
                    }) if next_field == field => Some(value.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(|values| QdrantPredicate::PayloadIn { field: field.clone(), values }),
            _ => None,
        }
    }
}

impl QdrantPayloadPath {
    pub(super) fn eq_condition(&self, value: &QdrantFilterValue) -> Condition {
        match value {
            QdrantFilterValue::String(value) => Condition::matches(self.key(), value.clone()),
            QdrantFilterValue::Integer(value) => Condition::matches(self.key(), *value),
            QdrantFilterValue::Bool(value) => Condition::matches(self.key(), *value),
            QdrantFilterValue::Float(value) => Condition::range(
                self.key(),
                Range { gte: Some(*value), lte: Some(*value), ..Default::default() },
            ),
            QdrantFilterValue::Datetime(value) => Condition::datetime_range(
                self.key(),
                DatetimeRange { gte: Some(*value), lte: Some(*value), ..Default::default() },
            ),
        }
    }

    pub(super) fn in_condition(&self, values: &[QdrantFilterValue]) -> Condition {
        match values {
            [] => unreachable!("empty IN list is not admitted"),
            [QdrantFilterValue::String(_), ..] => Condition::matches(
                self.key(),
                values
                    .iter()
                    .map(|value| match value {
                        QdrantFilterValue::String(value) => value.clone(),
                        _ => unreachable!("validated homogeneous IN list"),
                    })
                    .collect::<Vec<_>>(),
            ),
            [QdrantFilterValue::Integer(_), ..] => Condition::matches(
                self.key(),
                values
                    .iter()
                    .map(|value| match value {
                        QdrantFilterValue::Integer(value) => *value,
                        _ => unreachable!("validated homogeneous IN list"),
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => Filter::should(values.iter().map(|value| self.eq_condition(value))).into(),
        }
    }

    pub(super) fn range_condition(
        &self,
        lower: Option<&(QdrantFilterValue, bool)>,
        upper: Option<&(QdrantFilterValue, bool)>,
    ) -> Condition {
        if let Some(QdrantFilterValue::Datetime(_)) = lower.or(upper).map(|(value, _)| value) {
            let mut range = DatetimeRange::default();
            if let Some((QdrantFilterValue::Datetime(value), inclusive)) = lower {
                if *inclusive {
                    range.gte = Some(*value);
                } else {
                    range.gt = Some(*value);
                }
            }
            if let Some((QdrantFilterValue::Datetime(value), inclusive)) = upper {
                if *inclusive {
                    range.lte = Some(*value);
                } else {
                    range.lt = Some(*value);
                }
            }
            return Condition::datetime_range(self.key(), range);
        }
        let mut range = Range::default();
        if let Some((value, inclusive)) = lower {
            let Some(value) = value.range_bound() else {
                unreachable!("validated range lower bound");
            };
            if *inclusive {
                range.gte = Some(value);
            } else {
                range.gt = Some(value);
            }
        }
        if let Some((value, inclusive)) = upper {
            let Some(value) = value.range_bound() else {
                unreachable!("validated range upper bound");
            };
            if *inclusive {
                range.lte = Some(value);
            } else {
                range.lt = Some(value);
            }
        }
        Condition::range(self.key(), range)
    }
}

impl QdrantPayloadField {
    pub(super) fn into_range_predicate(
        self,
        field: QdrantPayloadPath,
        lower: Option<(QdrantFilterValue, bool)>,
        upper: Option<(QdrantFilterValue, bool)>,
    ) -> Option<QdrantPredicate> {
        match self {
            QdrantPayloadField::Integer { range: true, .. }
            | QdrantPayloadField::Float
            | QdrantPayloadField::Datetime => {
                Some(QdrantPredicate::PayloadRange { field, lower, upper })
            }
            _ => None,
        }
    }
}

impl QdrantFilterValue {
    #[expect(clippy::cast_precision_loss)]
    fn integer_to_f64(value: i64) -> f64 {
        value as f64
    }

    fn range_bound(&self) -> Option<f64> {
        match self {
            QdrantFilterValue::Integer(value) => Some(Self::integer_to_f64(*value)),
            QdrantFilterValue::Float(value) => Some(*value),
            _ => None,
        }
    }
}
