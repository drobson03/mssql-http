use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tiberius::{
    time::chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc},
    ColumnData, FromSql, Query,
};

use crate::DatabasePool;

#[derive(Deserialize)]
pub struct Execute {
    pub query: String,
    pub parameters: Vec<Value>,
}

#[derive(Serialize, Debug)]
pub struct DatabaseResult {
    pub error: Option<String>,
    pub rows_affected: Option<u64>,
    pub rows: Option<Vec<Vec<Value>>>,
}

pub async fn execute(
    State(pool): State<DatabasePool>,
    Json(payload): Json<Execute>,
) -> impl IntoResponse {
    let conn = pool.get().await;

    if conn.is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DatabaseResult {
                error: Some(format!("{:?}", conn.err())),
                rows_affected: None,
                rows: None,
            }),
        );
    }

    let mut conn = conn.unwrap();

    let query_str = payload.query.as_str();

    let mut query = Query::new(query_str);

    let parameters = &payload.parameters;

    for parameter in parameters {
        query.bind(parameter.as_str());
    }

    let mut results = DatabaseResult {
        error: None,
        rows_affected: None,
        rows: None,
    };

    if query_str.trim().to_lowercase().starts_with("select") {
        let mut query_results = query
            .query(&mut conn)
            .await
            .unwrap()
            .into_results()
            .await
            .unwrap();

        match query_results.pop() {
            Some(rows) => {
                let mut columns_set = false;
                let mut result_rows = Vec::with_capacity(rows.len());

                for row in rows.into_iter() {
                    if !columns_set {
                        columns_set = true;
                    }

                    let mut values: Vec<Value> = Vec::with_capacity(row.len());

                    for val in row.into_iter() {
                        match val {
                            ColumnData::U8(val) => values.push(val.into()),
                            ColumnData::I16(val) => values.push(val.into()),
                            ColumnData::I32(val) => values.push(val.into()),
                            ColumnData::I64(val) => values.push(val.into()),
                            ColumnData::F32(val) => values.push(val.into()),
                            ColumnData::F64(val) => values.push(val.into()),
                            ColumnData::Bit(val) => values.push(val.into()),
                            ColumnData::String(val) => values.push(val.into()),
                            dt @ ColumnData::DateTime(_) => {
                                let dt = NaiveDateTime::from_sql(&dt)
                                    .unwrap()
                                    .map(|dt| DateTime::<Utc>::from_utc(dt, Utc));
                                values.push(dt.unwrap().to_rfc3339().into());
                            }
                            dt @ ColumnData::SmallDateTime(_) => {
                                let dt = NaiveDateTime::from_sql(&dt)
                                    .unwrap()
                                    .map(|dt| DateTime::<Utc>::from_utc(dt, Utc));
                                values.push(dt.unwrap().to_rfc3339().into());
                            }
                            dt @ ColumnData::Time(_) => values.push(
                                NaiveTime::from_sql(&dt)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                                    .into(),
                            ),
                            dt @ ColumnData::Date(_) => values.push(
                                NaiveDate::from_sql(&dt)
                                    .unwrap()
                                    .unwrap()
                                    .to_string()
                                    .into(),
                            ),
                            dt @ ColumnData::DateTime2(_) => {
                                let dt = NaiveDateTime::from_sql(&dt)
                                    .unwrap()
                                    .map(|dt| DateTime::<Utc>::from_utc(dt, Utc));
                                values.push(dt.unwrap().to_rfc3339().into());
                            }
                            _ => values.push(Value::Null),
                        }
                    }

                    result_rows.push(values);
                }

                results.rows = Some(result_rows);
            }
            None => (),
        }
    } else {
        let query_result = query.execute(&mut conn).await.unwrap();

        results.rows_affected = Some(query_result.rows_affected()[0]);
    }

    (StatusCode::OK, Json(results))
}
