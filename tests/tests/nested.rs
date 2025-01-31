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
mod fixtures;

use anyhow::Result;
use fixtures::*;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
async fn nested_single_level_simple(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with a single level of nested JSON data
    r#"
    CREATE TABLE nested_table_single (
        id SERIAL PRIMARY KEY,
        nested_data JSONB
    );
    "#
    .execute(&mut conn);

    // std::thread::sleep(std::time::Duration::from_secs(10));

    // Insert some documents
    r#"
    INSERT INTO nested_table_single (nested_data) VALUES
    ('{"obj1": {"name": "blue", "count": 10}}'),
    ('{"obj1": {"name": "red",  "count": 4}}'),
    ('{"obj1": {"name": "blue", "count": 3}}');
    "#
    .execute(&mut conn);

    // Create a bm25 index that marks nested_data as a nested field
    r#"
    CREATE INDEX nested_table_single_idx ON nested_table_single
    USING bm25 (id, nested_data)
    WITH (
        key_field = 'id',
        json_fields = '{"nested_data": {"nested": {"obj1": {}}}}'
    );
    "#
    .execute(&mut conn);

    // Query for documents where nested_data.obj1.name = "blue" and nested_data.obj1.count > 5
    let rows: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_single
        WHERE nested_table_single @@@ paradedb.nested(
            path => 'nested_data',
            query => paradedb.boolean(
                must => ARRAY[
                    paradedb.term(field => 'nested_data.obj1.name', value => 'blue'),
                    paradedb.term(field => 'nested_data.obj1.count', value => 10)
                ]
            )
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Only the first row matches both conditions
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1,));

    Ok(())
}

#[rstest]
async fn nested_single_level_score_modes(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with a single level of nested JSON data
    r#"
    CREATE TABLE nested_table_score (
        id SERIAL PRIMARY KEY,
        nested_data JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data
    r#"
    INSERT INTO nested_table_score (nested_data) VALUES
    ('{"obj1": [{"name": "powell", "value": 3}, {"name": "powell", "value": 7}]}'),
    ('{"obj1": [{"name": "powell", "value": 10}, {"name": "powell", "value": 2}]}');
    "#
    .execute(&mut conn);

    std::thread::sleep(std::time::Duration::from_secs(10));

    // Create index
    r#"
    CREATE INDEX nested_table_score_idx ON nested_table_score
    USING bm25 (id, nested_data)
    WITH (
        key_field = 'id',
        json_fields = '{"nested_data": {"nested": ["nested_data.obj1"]}}'
    );
    "#
    .execute(&mut conn);

    let rows_avg: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_score
        WHERE nested_table_score @@@ paradedb.nested(
            path => 'nested_data.obj1',
            query => paradedb.term(field => 'nested_data.obj1.name', value => 'powell')
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Both documents should match
    assert_eq!(rows_avg.len(), 2);
    assert_eq!(rows_avg[0], (1,));
    assert_eq!(rows_avg[1], (2,));

    Ok(())
}

#[rstest]
async fn nested_multi_level(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with multi-level nested JSON data
    r#"
    CREATE TABLE nested_table_multi (
        id SERIAL PRIMARY KEY,
        nested_data JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data with deeper nesting
    r#"
    INSERT INTO nested_table_multi (nested_data) VALUES
    (
        '{
            "driver": {
                "last_name": "McQueen",
                "vehicle": [
                    {"make": "Powell Motors", "model": "Canyonero"},
                    {"make": "Miller-Meteor", "model": "Ecto-1"}
                ]
            }
        }'
    ),
    (
        '{
            "driver": {
                "last_name": "Hudson",
                "vehicle": [
                    {"make": "Mifune", "model": "Mach Five"},
                    {"make": "Miller-Meteor", "model": "Ecto-1"}
                ]
            }
        }'
    );
    "#
    .execute(&mut conn);

    // Create index with nested fields
    r#"
    CREATE INDEX nested_table_multi_idx ON nested_table_multi
    USING bm25 (id, nested_data)
    WITH (
        key_field = 'id',
        nested_fields = '["nested_data.driver", "nested_data.driver.vehicle"]'
    );
    "#
    .execute(&mut conn);

    // Query for multi-level nested data: driver.vehicle.make=Powell Motors AND driver.vehicle.model=Canyonero
    let rows: Vec<(i32,)> =  r#"
        SELECT id FROM nested_table_multi
        WHERE nested_table_multi @@@ paradedb.nested(
            path => 'nested_data.driver',
            query => paradedb.nested(
                path => 'nested_data.driver.vehicle',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.term(field => 'nested_data.driver.vehicle.make', value => 'Powell Motors'),
                        paradedb.term(field => 'nested_data.driver.vehicle.model', value => 'Canyonero')
                    ]
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1,));

    Ok(())
}

#[rstest]
async fn nested_must_not_clause(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table to test must_not clauses within nested data
    r#"
    CREATE TABLE nested_table_must_not (
        id SERIAL PRIMARY KEY,
        comments JSONB
    );
    "#
    .execute(&mut conn);

    // Insert sample docs
    r#"
    INSERT INTO nested_table_must_not (comments) VALUES
    ('{"comments": [{"author": "kimchy"}]}'),
    ('{"comments": [{"author": "kimchy"}, {"author": "nik9000"}]}'),
    ('{"comments": [{"author": "nik9000"}]}');
    "#
    .execute(&mut conn);

    // Create index
    r#"
    CREATE INDEX nested_table_must_not_idx ON nested_table_must_not
    USING bm25 (id, comments)
    WITH (
        key_field = 'id',
        nested_fields = '["comments"]'
    );
    "#
    .execute(&mut conn);

    // This query returns docs where the nested subdocument does NOT have "author=nik9000"
    // but it will still return any doc that has at least one nested subdocument matching the must_not.
    let rows_inner: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_must_not
        WHERE nested_table_must_not @@@ paradedb.nested(
            path => 'comments',
            query => paradedb.boolean(
                must_not => ARRAY[
                    paradedb.term(field => 'comments.author', value => 'nik9000')
                ]
            )
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Document 1 and 2 both match the inner must_not for at least one subdocument
    assert_eq!(rows_inner.len(), 2);
    assert_eq!(rows_inner[0], (1,));
    assert_eq!(rows_inner[1], (2,));

    // Now place the must_not at the outer level to exclude any doc that has ANY subdoc with author=nik9000
    let rows_outer: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_must_not
        WHERE paradedb.boolean(
            must_not => ARRAY[
                paradedb.nested(
                    path => 'comments',
                    query => paradedb.term(field => 'comments.author', value => 'nik9000')
                )
            ]
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Only doc 1 is returned, because doc 2 and doc 3 contain 'nik9000' in at least one subdocument
    assert_eq!(rows_outer.len(), 1);
    assert_eq!(rows_outer[0], (1,));

    Ok(())
}

#[rstest]
async fn nested_ignore_unmapped_path(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table with some nested data
    r#"
    CREATE TABLE nested_table_unmapped (
        id SERIAL PRIMARY KEY,
        data JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data
    r#"
    INSERT INTO nested_table_unmapped (data) VALUES
    ('{"obj1": {"name": "apple"}}'),
    ('{"obj1": {"name": "banana"}}');
    "#
    .execute(&mut conn);

    // Create index for the existing path
    r#"
    CREATE INDEX nested_table_unmapped_idx ON nested_table_unmapped
    USING bm25 (id, data)
    WITH (
        key_field = 'id',
        nested_fields = '["data.obj1"]'
    );
    "#
    .execute(&mut conn);

    // Query with ignore_unmapped => true, referencing an unmapped path "data.obj2"
    let rows_ignore_unmapped: Vec<(i32,)> = r#"
        SELECT id FROM nested_table_unmapped
        WHERE nested_table_unmapped @@@ paradedb.nested(
            path => 'data.obj2',
            query => paradedb.term(field => 'data.obj2.whatever', value => 'none'),
            ignore_unmapped => true
        )
        ORDER BY id
        "#
    .fetch(&mut conn);

    // Should return nothing, but not error out
    assert_eq!(rows_ignore_unmapped.len(), 0);

    // Query with ignore_unmapped => false should produce an error
    // We'll confirm we get an error containing 'unmapped path' or similar
    let err = r#"
        SELECT id FROM nested_table_unmapped
        WHERE nested_table_unmapped @@@ paradedb.nested(
            path => 'data.obj2',
            query => paradedb.term(field => 'data.obj2.whatever', value => 'none'),
            ignore_unmapped => false
        )"#
    .fetch_result::<(i32,)>(&mut conn);

    assert!(err.err().unwrap().to_string().contains("unmapped path"));

    Ok(())
}

#[rstest]
async fn nested_cart_products_simple_case(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // This table simulates a shopping cart scenario with nested fields.
    // cart -> products -> model_attributes
    // We want to confirm that cart queries (like "blue & L") only match when both terms
    // appear in the same nested product object.

    r#"
    CREATE TABLE nested_shopping_carts (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    // Insert a few sample carts
    // 1) cart_id=100, has two products:
    //    product_id=13 => (color=blue, size=L)
    //    product_id=15 => (color=red, size=M)
    // 2) cart_id=200, has one product:
    //    product_id=20 => (color=blue, size=M)
    // 3) cart_id=300, has two products:
    //    product_id=30 => (color=blue, size=L)
    //    product_id=31 => (color=blue, size=M)
    // We'll then run queries to ensure that only those with color=blue & size=L
    // match each cart properly.

    r#"
    INSERT INTO nested_shopping_carts (cart) VALUES
    (
        '{
            "cart_id": 100,
            "products": [
                {
                    "product_id": 13,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 15,
                    "model_attributes": {
                        "color": "red",
                        "size": "M"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 200,
            "products": [
                {
                    "product_id": 20,
                    "model_attributes": {
                        "color": "blue",
                        "size": "M"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 300,
            "products": [
                {
                    "product_id": 30,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 31,
                    "model_attributes": {
                        "color": "blue",
                        "size": "M"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    // Create a bm25 index that designates products and their model_attributes as nested
    r#"
    CREATE INDEX nested_shopping_carts_idx ON nested_shopping_carts
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        nested_fields = '["cart.products","cart.products.model_attributes"]'
    );
    "#
    .execute(&mut conn);

    // We want to find all carts that have (color=blue AND size=L) in the SAME product.
    // For that, we will use a multi-level nested query:
    // "path=cart.products => nested(
    //      path=cart.products.model_attributes => boolean(must=[term(color=blue), term(size=L)])
    // )"
    let rows: Vec<(i32,)> =  r#"
        SELECT id FROM nested_shopping_carts
        WHERE nested_shopping_carts @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.nested(
                path => 'cart.products.model_attributes',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                        paradedb.term(field => 'cart.products.model_attributes.size', value => 'L')
                    ]
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    // Carts #1 (id=1) and #3 (id=3) match color=blue & size=L in at least one product.
    // Cart #2 (id=2) has blue color but only size=M, so it should NOT match.
    assert_eq!(rows, vec![(1,), (3,)]);

    Ok(())
}

#[rstest]
async fn nested_cart_products_score_modes(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Same table & data structure, but let's add more data to test different score modes
    r#"
    CREATE TABLE nested_shopping_carts_scores (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    // Insert data with multiple products having partial matches, so we can test whether
    // the different scoring modes produce different match sets or the same set with different scores.
    r#"
    INSERT INTO nested_shopping_carts_scores (cart) VALUES
    (
        '{
            "cart_id": 400,
            "products": [
                {
                    "product_id": 40,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 41,
                    "model_attributes": {
                        "color": "blue",
                        "size": "M"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 500,
            "products": [
                {
                    "product_id": 42,
                    "model_attributes": {
                        "color": "blue",
                        "size": "M"
                    }
                },
                {
                    "product_id": 44,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 600,
            "products": [
                {
                    "product_id": 48,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 49,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX nested_shopping_carts_scores_idx ON nested_shopping_carts_scores
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        nested_fields = '["cart.products","cart.products.model_attributes"]'
    );
    "#
    .execute(&mut conn);

    // We'll run the same logical query, but with different score_mode settings.
    // For demonstration, only 'avg' and 'sum' or 'none' might behave differently,
    // but let's test them just to confirm the index doesn't blow up.
    // We'll ensure that all three docs match because they have at least one product with color=blue,
    // but the final scoring might differ. (In real usage we'd check actual scores, but
    // here we're just testing that the search doesn't fail and the correct docs are returned.)

    let rows_avg: Vec<(i32,)> = r#"
        SELECT id FROM nested_shopping_carts_scores
        WHERE nested_shopping_carts_scores @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
            score_mode => 'avg'
        )
        ORDER BY id
        "#.fetch(&mut conn);
    // Expect all (1,2,3) in ascending order by ID in this new table, which is 1.. for the table rows
    assert_eq!(rows_avg.len(), 3);

    let rows_sum: Vec<(i32,)> = r#"
        SELECT id FROM nested_shopping_carts_scores
        WHERE nested_shopping_carts_scores @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
            score_mode => 'sum'
        )
        ORDER BY id
        "#.fetch(&mut conn);
    assert_eq!(rows_sum.len(), 3);

    let rows_none: Vec<(i32,)> = r#"
        SELECT id FROM nested_shopping_carts_scores
        WHERE nested_shopping_carts_scores @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
            score_mode => 'none'
        )
        ORDER BY id
        "#.fetch(&mut conn);
    assert_eq!(rows_none.len(), 3);

    Ok(())
}

#[rstest]
async fn nested_cart_products_partial_mismatch(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Create a table for partial mismatch checks, where some product lines
    // have only color, others have only size, etc. We want to confirm that
    // the entire cart is only returned if the single nested product matches
    // the entire search condition.

    r#"
    CREATE TABLE nested_carts_partial_mismatch (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    r#"
    INSERT INTO nested_carts_partial_mismatch (cart) VALUES
    (
        '{
            "cart_id": 700,
            "products": [
                {
                    "product_id": 70,
                    "model_attributes": {
                        "color": "blue"
                    }
                },
                {
                    "product_id": 71,
                    "model_attributes": {
                        "size": "L"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 800,
            "products": [
                {
                    "product_id": 72,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 900,
            "products": [
                {
                    "product_id": 73,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX nested_carts_partial_mismatch_idx ON nested_carts_partial_mismatch
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        nested_fields = '["cart.products","cart.products.model_attributes"]'
    );
    "#
    .execute(&mut conn);

    // Now let's query for color=blue AND size=L in the same product.
    // Only cart_id=800 truly has a single product with both color=blue and size=L.
    // Cart_id=700 has color=blue in product_id=70, size=L in product_id=71, but
    // that's not the same product.
    // Cart_id=900 is color=red size=L, so mismatch as well.

    let rows: Vec<(i32,)> = r#"
        SELECT id FROM nested_carts_partial_mismatch
        WHERE nested_carts_partial_mismatch @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.nested(
                path => 'cart.products.model_attributes',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                        paradedb.term(field => 'cart.products.model_attributes.size', value => 'L')
                    ]
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    // Only row #2 matches.
    assert_eq!(rows, vec![(2,)]);

    Ok(())
}

#[rstest]
async fn nested_cart_products_mix_and_match(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    // Test scenario where a cart has multiple nested products,
    // some of which match color=blue & size=L, and others don't match at all.
    // This confirms we only need at least one matching product for the parent cart to appear.

    r#"
    CREATE TABLE nested_carts_mix_and_match (
        id SERIAL PRIMARY KEY,
        cart JSONB
    );
    "#
    .execute(&mut conn);

    r#"
    INSERT INTO nested_carts_mix_and_match (cart) VALUES
    (
        '{
            "cart_id": 1000,
            "products": [
                {
                    "product_id": 100,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                },
                {
                    "product_id": 101,
                    "model_attributes": {
                        "color": "blue",
                        "size": "L"
                    }
                },
                {
                    "product_id": 102,
                    "model_attributes": {
                        "color": "green",
                        "size": "S"
                    }
                }
            ]
        }'
    ),
    (
        '{
            "cart_id": 1100,
            "products": [
                {
                    "product_id": 110,
                    "model_attributes": {
                        "color": "red",
                        "size": "L"
                    }
                },
                {
                    "product_id": 111,
                    "model_attributes": {
                        "color": "red",
                        "size": "M"
                    }
                }
            ]
        }'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX nested_carts_mix_and_match_idx ON nested_carts_mix_and_match
    USING bm25 (id, cart)
    WITH (
        key_field = 'id',
        nested_fields = '["cart.products","cart.products.model_attributes"]'
    );
    "#
    .execute(&mut conn);

    // Now let's run a nested query for color=blue AND size=L.
    // The first cart (id=1) has a product that meets that criterion (product_id=101).
    // The second cart (id=2) doesn't, so it shouldn't show up.

    let rows: Vec<(i32,)> = r#"
        SELECT id FROM nested_carts_mix_and_match
        WHERE nested_carts_mix_and_match @@@ paradedb.nested(
            path => 'cart.products',
            query => paradedb.nested(
                path => 'cart.products.model_attributes',
                query => paradedb.boolean(
                    must => ARRAY[
                        paradedb.term(field => 'cart.products.model_attributes.color', value => 'blue'),
                        paradedb.term(field => 'cart.products.model_attributes.size', value => 'L')
                    ]
                )
            )
        )
        ORDER BY id
        "#.fetch(&mut conn);

    // Only row #1 matches
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}
