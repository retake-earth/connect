// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Tests for the paradedb.tokenize function

mod fixtures;

use fixtures::*;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::PgConnection;
use std::collections::HashSet;

#[rstest]
fn reltuples_are_set(mut conn: PgConnection) {
    "CREATE TABLE reltuptest AS SELECT md5(x::text), x FROM generate_series(1, 1024) x;"
        .execute(&mut conn);

    let (reltuples,) = "SELECT reltuples FROM pg_class WHERE oid = 'reltuptest'::regclass::oid"
        .fetch_one::<(f32,)>(&mut conn);
    assert_eq!(reltuples, -1.0);

    "CALL paradedb.create_bm25(index_name => 'idxreltuptest', table_name => 'reltuptest', key_field => 'x', text_fields => paradedb.field('md5'))".execute(&mut conn);
    let (reltuples,) = "SELECT reltuples FROM pg_class WHERE oid = 'reltuptest'::regclass::oid"
        .fetch_one::<(f32,)>(&mut conn);
    assert_eq!(reltuples, 1024.0);
}

#[rstest]
fn direct_or_queries(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    for query in &[
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ 'description:keyboard OR category:electronics'",
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ 'description:keyboard' OR id @@@ 'category:electronics'",
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ paradedb.term('description', 'keyboard') OR id @@@ paradedb.term('category', 'electronics')",
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ paradedb.term('description', 'keyboard') OR id @@@ 'category:electronics'",
    ] {
        let columns: SimpleProductsTableVec = query.fetch_collect(&mut conn);

        assert_eq!(
            columns.description.iter().cloned().collect::<HashSet<_>>(),
            concat!(
            "Plastic Keyboard,Ergonomic metal keyboard,Innovative wireless earbuds,",
            "Fast charging power bank,Bluetooth-enabled speaker"
            )
                .split(',')
                .map(|s| s.to_string())
                .collect::<HashSet<_>>()
        );

        assert_eq!(
            columns.category.iter().cloned().collect::<HashSet<_>>(),
            "Electronics,Electronics,Electronics,Electronics,Electronics"
                .split(',')
                .map(|s| s.to_string())
                .collect::<HashSet<_>>()
        );
    }
}

#[rstest]
fn direct_and_queries(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    for query in &[
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ 'description:keyboard AND category:electronics'",
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ 'description:keyboard' AND id @@@ 'category:electronics'",
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ paradedb.term('description', 'keyboard') AND id @@@ paradedb.term('category', 'electronics')",
        "SELECT * FROM paradedb.bm25_search WHERE id @@@ paradedb.term('description', 'keyboard') AND id @@@ 'category:electronics'",
    ] {
        let columns: SimpleProductsTableVec = query.fetch_collect(&mut conn);

        assert_eq!(
            columns.description.iter().cloned().collect::<HashSet<_>>(),
            ["Plastic Keyboard","Ergonomic metal keyboard"].iter().map(|s| s.to_string())
                .collect::<HashSet<_>>()
        );

        assert_eq!(
            columns.category.iter().cloned().collect::<HashSet<_>>(),
            ["Electronics"].iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>()
        );
    }
}

#[rstest]
fn direct_sql_mix(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (description, ) = "SELECT description FROM paradedb.bm25_search WHERE id @@@ 'description:keyboard' AND id = 2".fetch_one::<(String,)>(&mut conn);

    assert_eq!(description, "Plastic Keyboard");
}

#[rstest]
fn explain_row_estimate(mut conn: PgConnection) {
    use serde_json::Number;
    use serde_json::Value;

    SimpleProductsTable::setup().execute(&mut conn);

    let (plan, ) = "EXPLAIN (FORMAT JSON) SELECT * FROM paradedb.bm25_search WHERE id @@@ 'description:keyboard' AND id = 2".fetch_one::<(Value,)>(&mut conn);
    let plan = plan
        .get(0)
        .unwrap()
        .as_object()
        .unwrap()
        .get("Plan")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(plan.get("Plan Rows"), Some(&Value::Number(Number::from(2))));
}
