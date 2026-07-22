use rusqlite::{Connection, Params, Transaction};

use maqistor_engine::StoreError;

pub(crate) const ROWS_PER_STATEMENT: usize = 64;

pub(crate) fn values_placeholders(rows: usize, cols: usize) -> String {
    debug_assert!(rows > 0 && cols > 0);
    (0..rows)
        .map(|row| {
            let start = row * cols;
            let inner = (1..=cols)
                .map(|col| format!("?{}", start + col))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({inner})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn insert_sql(table: &str, columns: &[&str], rows: usize) -> String {
    debug_assert!(!columns.is_empty());
    let cols = columns.join(", ");
    let values = values_placeholders(rows, columns.len());
    format!("INSERT INTO {table} ({cols}) VALUES {values}")
}

pub(crate) fn update_from_values_sql(
    table: &str,
    alias: &str,
    set_sql: &str,
    value_columns: &[&str],
    rows: usize,
    where_sql: &str,
    returning: Option<&str>,
) -> String {
    debug_assert!(!value_columns.is_empty());
    let values = values_placeholders(rows, value_columns.len());
    let cols = value_columns.join(", ");
    let mut sql = format!(
        "WITH {alias}({cols}) AS (VALUES {values}) \
         UPDATE {table} SET {set_sql} FROM {alias} WHERE {where_sql}"
    );
    if let Some(returning) = returning {
        sql.push_str(" RETURNING ");
        sql.push_str(returning);
    }
    sql
}

pub(crate) fn execute_cached(
    conn: &Connection,
    sql: &str,
    params: impl Params,
) -> Result<usize, StoreError> {
    conn.prepare_cached(sql)
        .map_err(|err| StoreError::Internal(err.to_string()))?
        .execute(params)
        .map_err(|err| StoreError::Internal(err.to_string()))
}

pub(crate) fn execute_cached_tx(
    tx: &Transaction<'_>,
    sql: &str,
    params: impl Params,
) -> Result<usize, StoreError> {
    tx.prepare_cached(sql)
        .map_err(|err| StoreError::Internal(err.to_string()))?
        .execute(params)
        .map_err(|err| StoreError::Internal(err.to_string()))
}

pub(crate) fn query_pairs_cached_tx<T, F>(
    tx: &Transaction<'_>,
    sql: &str,
    params: impl Params,
    map: F,
) -> Result<Vec<T>, StoreError>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    let mut stmt = tx
        .prepare_cached(sql)
        .map_err(|err| StoreError::Internal(err.to_string()))?;
    let rows = stmt
        .query_map(params, map)
        .map_err(|err| StoreError::Internal(err.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| StoreError::Internal(err.to_string()))?;
    Ok(rows)
}
