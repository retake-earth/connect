// Copyright (c) 2023-2025 Retake, Inc.
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

use approx::assert_relative_eq;
use fixtures::*;
use num_traits::ToPrimitive;
use pgvector::Vector;
use rstest::*;
use sqlx::types::BigDecimal;
use sqlx::PgConnection;
use std::str::FromStr;

#[rstest]
fn quickstart(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    )
    "#
    .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    LIMIT 3;
    "#
    .fetch(&mut conn);
    assert_eq!(
        rows,
        vec![
            ("Ergonomic metal keyboard".into(), 4, "Electronics".into()),
            ("Plastic Keyboard".into(), 4, "Electronics".into()),
            ("Sleek running shoes".into(), 5, "Footwear".into())
        ]
    );

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata, weight_range) 
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' OR category @@@ 'footwear' AND rating @@@ '>2'
    ORDER BY description
    LIMIT 5"#
        .fetch(&mut conn);
    assert_eq!(rows[0].0, "Comfortable slippers".to_string());
    assert_eq!(rows[1].0, "Generic shoes".to_string());
    assert_eq!(rows[2].0, "Sleek running shoes".to_string());
    assert_eq!(rows[3].0, "Sturdy hiking boots".to_string());
    assert_eq!(rows[4].0, "White jogging shoes".to_string());

    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE description @@@ 'shoes' OR category @@@ 'footwear' AND rating @@@ '>2'
    ORDER BY score DESC, description
    LIMIT 5"#
        .fetch(&mut conn);
    assert_eq!(rows[0].0, "Generic shoes".to_string());
    assert_eq!(rows[1].0, "Sleek running shoes".to_string());
    assert_eq!(rows[2].0, "White jogging shoes".to_string());
    assert_eq!(rows[3].0, "Comfortable slippers".to_string());
    // The BM25 score here is a tie, so the order is arbitrary
    assert!(rows[4].0 == *"Sturdy hiking boots" || rows[4].0 == *"Winter woolen socks");
    assert_eq!(rows[0].3, 5.8135376);
    assert_eq!(rows[1].3, 5.4211845);
    assert_eq!(rows[2].3, 5.4211845);
    assert_eq!(rows[3].3, 2.9362776);
    assert_eq!(rows[4].3, 2.9362776);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ '"white shoes"~1'
    LIMIT 5"#
        .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "White jogging shoes");

    r#"
    CALL paradedb.create_bm25_test_table(
        schema_name => 'public',
        table_name => 'orders',
        table_type => 'Orders'
    );

    ALTER TABLE orders
    ADD CONSTRAINT foreign_key_product_id
    FOREIGN KEY (product_id)
    REFERENCES mock_items(id);

    CREATE INDEX orders_idx ON orders
    USING bm25 (order_id, customer_name) 
    WITH (key_field='order_id');
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, i32, i32, BigDecimal, String)> = r#"
    SELECT * FROM orders LIMIT 3"#
        .fetch(&mut conn);
    assert_eq!(
        rows,
        vec![
            (
                1,
                1,
                3,
                BigDecimal::from_str("99.99").unwrap(),
                "John Doe".into()
            ),
            (
                2,
                2,
                1,
                BigDecimal::from_str("49.99").unwrap(),
                "Jane Smith".into()
            ),
            (
                3,
                3,
                5,
                BigDecimal::from_str("249.95").unwrap(),
                "Alice Johnson".into()
            ),
        ]
    );

    let rows: Vec<(i32, String, String)> = r#"
    SELECT o.order_id, o.customer_name, m.description
    FROM orders o
    JOIN mock_items m ON o.product_id = m.id
    WHERE o.customer_name @@@ 'Johnson' AND m.description @@@ 'shoes'
    ORDER BY order_id
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(
        rows,
        vec![
            (3, "Alice Johnson".into(), "Sleek running shoes".into()),
            (6, "Alice Johnson".into(), "White jogging shoes".into()),
            (36, "Alice Johnson".into(), "White jogging shoes".into()),
        ]
    );

    r#"
    CREATE EXTENSION vector;
    ALTER TABLE mock_items ADD COLUMN embedding vector(3);
    "#
    .execute(&mut conn);
    r#"
    UPDATE mock_items m
    SET embedding = ('[' ||
        ((m.id + 1) % 10 + 1)::integer || ',' ||
        ((m.id + 2) % 10 + 1)::integer || ',' ||
        ((m.id + 3) % 10 + 1)::integer || ']')::vector;
    "#
    .execute(&mut conn);
    let rows: Vec<(String, i32, String, Vector)> = r#"
    SELECT description, rating, category, embedding
    FROM mock_items LIMIT 3;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "Ergonomic metal keyboard");
    assert_eq!(rows[1].0, "Plastic Keyboard");
    assert_eq!(rows[2].0, "Sleek running shoes");
    assert_eq!(rows[0].3, Vector::from(vec![3.0, 4.0, 5.0]));
    assert_eq!(rows[1].3, Vector::from(vec![4.0, 5.0, 6.0]));
    assert_eq!(rows[2].3, Vector::from(vec![5.0, 6.0, 7.0]));

    r#"
    CREATE INDEX on mock_items
    USING hnsw (embedding vector_cosine_ops);
    "#
    .execute(&mut conn);
    let rows: Vec<(String, String, i32, Vector)> = r#"
    SELECT description, category, rating, embedding
    FROM mock_items
    ORDER BY embedding <=> '[1,2,3]', description
    LIMIT 3;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "Artistic ceramic vase");
    assert_eq!(rows[1].0, "Designer wall paintings");
    assert_eq!(rows[2].0, "Handcrafted wooden frame");
    assert_eq!(rows[0].3, Vector::from(vec![1.0, 2.0, 3.0]));
    assert_eq!(rows[1].3, Vector::from(vec![1.0, 2.0, 3.0]));
    assert_eq!(rows[2].3, Vector::from(vec![1.0, 2.0, 3.0]));
}

#[rstest]
fn full_text_search(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, String, i32)> = r#"
    SELECT id, description, rating
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating @@@ '>3'
    ORDER BY id
    "#
    .fetch(&mut conn);
    assert_eq!(rows[0], (3, "Sleek running shoes".into(), 5));
    assert_eq!(rows[1], (5, "Generic shoes".into(), 4));

    // Basic term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Multiple terms
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'keyboard' OR category @@@ 'toy'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    // Not term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ '(shoes running -white)'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    // Basic phrase
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ '"plastic keyboard"'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Slop operator
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ '"ergonomic keyboard"~1'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Phrase prefix
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ '"plastic keyb"*'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Basic filtering
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating > 2
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating @@@ '>2'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Numeric filter
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating @@@ '4'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating @@@ '>=4'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    // Datetime filter
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND created_at @@@ '"2023-04-20T16:38:02Z"'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Boolean filter
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND in_stock @@@ 'true'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    // Range filter
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating @@@ '[1 TO 4]'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND created_at @@@ '[2020-01-31T00:00:00Z TO 2024-01-31T00:00:00Z]'
    "#.fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ '[book TO camera]'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 8);

    // Set filter
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND rating @@@ 'IN [2 3 4]'
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    // Pagination
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes'
    LIMIT 1 OFFSET 2
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // BM25 scoring
    let rows: Vec<(i32, f32)> = r#"
    SELECT id, paradedb.score(id)
    FROM mock_items
    WHERE description @@@ 'shoes'
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    r#"
    CALL paradedb.create_bm25_test_table(
        schema_name => 'public',
        table_name => 'orders',
        table_type => 'Orders'
    );

    ALTER TABLE orders
    ADD CONSTRAINT foreign_key_product_id
    FOREIGN KEY (product_id)
    REFERENCES mock_items(id);

    CREATE INDEX orders_idx ON orders
    USING bm25 (order_id, customer_name)
    WITH (key_field='order_id');
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, f32)> = r#"
    SELECT o.order_id, paradedb.score(o.order_id) + paradedb.score(m.id) as score
    FROM orders o
    JOIN mock_items m ON o.product_id = m.id
    WHERE o.customer_name @@@ 'Johnson' AND (m.description @@@ 'shoes' OR m.description @@@ 'running')
    ORDER BY score DESC, o.order_id
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows, vec![(3, 8.738735), (6, 5.406531), (36, 5.406531)]);

    // Highlighting
    let rows: Vec<(i32, String)> = r#"
    SELECT id, paradedb.snippet(description)
    FROM mock_items
    WHERE description @@@ 'shoes'
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(i32, String)> = r#"
    SELECT id, paradedb.snippet(description, start_tag => '<i>', end_tag => '</i>')
    FROM mock_items
    WHERE description @@@ 'shoes'
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
    assert!(rows[0].1.contains("<i>"));
    assert!(rows[0].1.contains("</i>"));

    // Order by score
    let rows: Vec<(String, i32, String, f32)> = r#"
        SELECT description, rating, category, paradedb.score(id)
        FROM mock_items
        WHERE description @@@ 'shoes'
        ORDER BY score DESC
        LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].3, 2.8772602);
    assert_eq!(rows[1].3, 2.4849067);
    assert_eq!(rows[2].3, 2.4849067);

    // Order by field
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes'
    ORDER BY rating DESC
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(
        rows,
        vec![
            ("Sleek running shoes".into(), 5, "Footwear".into()),
            ("Generic shoes".into(), 4, "Footwear".into()),
            ("White jogging shoes".into(), 3, "Footwear".into()),
        ]
    );

    // Tiebreaking
    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE category @@@ 'electronics'
    ORDER BY score DESC, rating DESC
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].3, 2.1096356);
    assert_eq!(rows[1].3, 2.1096356);
    assert_eq!(rows[2].3, 2.1096356);
    assert_eq!(rows[3].3, 2.1096356);
    assert_eq!(rows[4].3, 2.1096356);

    // Constant boosting
    let rows: Vec<(i32, f32)> = r#"
    SELECT id, paradedb.score(id)
    FROM mock_items
    WHERE description @@@ 'shoes^2' OR category @@@ 'footwear'
    ORDER BY score DESC
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].1, 7.690798);
    assert_eq!(rows[1].1, 6.9060907);
    assert_eq!(rows[2].1, 6.9060907);
    assert_eq!(rows[3].1, 1.9362776);
    assert_eq!(rows[4].1, 1.9362776);

    // Boost by field
    let rows: Vec<(i32, f64)> = r#"
    SELECT id, paradedb.score(id) * COALESCE(rating, 1) as score
    FROM mock_items
    WHERE description @@@ 'shoes'
    ORDER BY score DESC 
    LIMIT 5
    "#
    .fetch(&mut conn);
    assert_eq!(
        rows,
        vec![
            (3, 12.424533367156982),
            (5, 11.509040832519531),
            (4, 7.4547200202941895),
        ]
    );
}

#[rstest]
fn match_query(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata, weight_range)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.match('description', 'running shoes');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "match": {
            "field": "description",
            "value": "running shoes"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.match(
        'description',
        'running shoes',
        tokenizer => paradedb.tokenizer('whitespace')
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "match": {
            "field": "description",
            "value": "running shoes",
            "tokenizer": {"type": "whitespace", "lowercase": true, "remove_long": 255}
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.match('description', 'ruining shoez', distance => 2);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "match": {
            "field": "description",
            "value": "ruining shoez",
            "distance": 2
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.match('description', 'running shoes', conjunction_mode => true);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "match": {
            "field": "description",
            "value": "running shoes",
            "conjunction_mode": true
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
}

#[rstest]
fn term_level_queries(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata, weight_range)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    // Exists
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.exists('rating')
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "exists": {
            "field": "rating"
        }
    }'::jsonb
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
      must => ARRAY[
        paradedb.term('description', 'shoes'),
        paradedb.exists('rating')
      ]
    )
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "boolean": {
            "must": [
                {"term": {"field": "description", "value": "shoes"}},
                {"exists": {"field": "rating"}}
            ]
        }
    }'::jsonb
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Fuzzy term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.fuzzy_term('description', 'shoez')
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "fuzzy_term": {
            "field": "description",
            "value": "shoez"
        }
    }'::jsonb
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Range
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.range(
        field => 'rating',
        range => int4range(1, 3, '[)')
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 4);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "range": {
            "field": "rating",
            "lower_bound": {"included": 1},
            "upper_bound": {"excluded": 3}
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 4);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.range(
        field => 'rating',
        range => int4range(1, 3, '[]')
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 13);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "range": {
            "field": "rating",
            "lower_bound": {"included": 1},
            "upper_bound": {"included": 3}
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 13);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.range(
        field => 'rating',
        range => int4range(1, NULL, '[)')
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 41);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "range": {
            "field": "rating",
            "lower_bound": {"included": 1},
            "upper_bound": null
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 41);

    // Range term
    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@ paradedb.range_term('weight_range', 1);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 16);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@
    '{
        "range_term": {
            "field": "weight_range",
            "value": 1
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 16);

    let rows: Vec<(i32,)> = r#"
    SELECT id, description, category, weight_range FROM mock_items
    WHERE id @@@ paradedb.boolean(
        must => ARRAY[
            paradedb.range_term('weight_range', 1),
            paradedb.term('category', 'footwear')
        ]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(i32,)> = r#"
    SELECT id, description, category, weight_range FROM mock_items
    WHERE id @@@
    '{
        "boolean": {
            "must": [
                {
                    "range_term": {
                        "field": "weight_range",
                        "value": 1
                    }
                },
                {
                    "term": {
                        "field": "category",
                        "value": "footwear"
                    }
                }
            ]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@ paradedb.range_term('weight_range', '(10, 12]'::int4range, 'Intersects');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@
    '{
        "range_intersects": {
            "field": "weight_range",
            "lower_bound": {"excluded": 10},
            "upper_bound": {"included": 12}
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@ paradedb.range_term('weight_range', '(3, 9]'::int4range, 'Contains');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 7);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@
    '{
        "range_contains": {
            "field": "weight_range",
            "lower_bound": {"excluded": 3},
            "upper_bound": {"included": 9}
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 7);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@ paradedb.range_term('weight_range', '(2, 11]'::int4range, 'Within');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let rows: Vec<(i32,)> = r#"
    SELECT id, weight_range FROM mock_items
    WHERE id @@@
    '{
        "range_within": {
            "field": "weight_range", 
            "lower_bound": {"excluded": 2},
            "upper_bound": {"included": 11}
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    // Regex
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.regex('description', '(plush|leather)');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "regex": {
            "field": "description",
            "pattern": "(plush|leather)"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.regex('description', 'key.*rd');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "regex": {
            "field": "description",
            "pattern": "key.*rd"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);

    // Term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.term('description', 'shoes');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "term": {
            "field": "description",
            "value": "shoes"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.term('rating', 4);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 16);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "term": {
            "field": "rating",
            "value": 4
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 16);

    // Term set
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.term_set(
    	terms => ARRAY[
    		paradedb.term('description', 'shoes'),
    		paradedb.term('description', 'novel')
    	]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "term_set": {
            "terms": [
                {"field": "description", "value": "shoes"},
                {"field": "description", "value": "novel"}
            ]
        }
    }'::jsonb ORDER BY id;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);
}

#[rstest]
fn phrase_level_queries(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    // Phrase
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.phrase(
        field => 'description',
        phrases => ARRAY['running', 'shoes']
    )"#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "phrase": {
            "field": "description",
            "phrases": ["running", "shoes"]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.phrase('description', ARRAY['sleek', 'shoes'], slop => 1);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "phrase": {
            "field": "description",
            "phrases": ["sleek", "shoes"],
            "slop": 1
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Test both function and JSON syntax for phrase_prefix
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.phrase_prefix('description', ARRAY['running', 'sh'])
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "phrase_prefix": {
            "field": "description",
            "phrases": ["running", "sh"]
        }
    }'::jsonb
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Regex phrase
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.regex_phrase('description', ARRAY['run.*', 'shoe.*'])
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.regex_phrase('description', ARRAY['run.*', 'sh.*'])
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
}

#[rstest]
fn json_queries(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    UPDATE mock_items
    SET metadata = '{"attributes": {"score": 3, "tstz": "2023-05-01T08:12:34Z"}}'::jsonb 
    WHERE id = 1;

    UPDATE mock_items
    SET metadata = '{"attributes": {"score": 4, "tstz": "2023-05-01T09:12:34Z"}}'::jsonb 
    WHERE id = 2;


    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id', json_fields='{"metadata": {"fast": true}}');
    "#
    .execute(&mut conn);

    // Term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.term('metadata.color', 'white')
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
        FROM mock_items
        WHERE id @@@
    '{
        "term": {
            "field": "metadata.color",
            "value": "white"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Datetime Handling
    let rows: Vec<(i32,)> = r#"
    SELECT id FROM mock_items WHERE mock_items @@@ '{
        "range": {
            "field": "metadata.attributes.tstz",
            "lower_bound": {"included": "2023-05-01T08:12:34Z"},
            "upper_bound": null,
            "is_datetime": true
        }
    }'::jsonb
    ORDER BY id;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
}

#[rstest]
fn json_arrays(mut conn: PgConnection) {
    //
    // 1) Create the mock_items test table with ParadeDB helper
    //
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );
    "#
    .execute(&mut conn);

    //
    // 2) Insert some JSON arrays so we can test array-flattening
    //
    r#"
    UPDATE mock_items
    SET metadata = '{"colors": ["red", "green", "blue"]}'::jsonb
    WHERE id = 1;
    UPDATE mock_items
    SET metadata = '{"colors": ["red", "yellow"]}'::jsonb
    WHERE id = 2;
    UPDATE mock_items
    SET metadata = '{"colors": ["blue", "purple"]}'::jsonb
    WHERE id = 3;
    "#
    .execute(&mut conn);

    //
    // 3) Create an index that includes metadata
    //
    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    //
    // 4) Query via function syntax
    //
    let rows: Vec<(String, serde_json::Value)> = r#"
    SELECT description, metadata
    FROM mock_items
    WHERE id @@@ paradedb.term('metadata.colors', 'blue')
       OR id @@@ paradedb.term('metadata.colors', 'red');
    "#
    .fetch(&mut conn);

    // We expect these three rows to match IDs 1, 2, and/or 3
    assert_eq!(rows.len(), 3);

    //
    // 5) Query via JSON syntax
    //
    let rows2: Vec<(String, serde_json::Value)> = r#"
    SELECT description, metadata
    FROM mock_items
    WHERE id @@@
    '{
        "term": {
            "field": "metadata.colors",
            "value": "blue"
        }
    }'::jsonb
    OR id @@@
    '{
        "term": {
            "field": "metadata.colors",
            "value": "red"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);

    // Same three rows should appear
    assert_eq!(rows2.len(), 3);
}

#[rstest]
fn custom_enum(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE TYPE color AS ENUM ('red', 'green', 'blue');
    ALTER TABLE mock_items ADD COLUMN color color;
    INSERT INTO mock_items (color) VALUES ('red'), ('green'), ('blue');

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, color, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    // Term
    let rows: Vec<(Option<String>, Option<i32>, Option<String>)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.term('color', 'red'::color);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(Option<String>, Option<i32>, Option<String>)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "term": {
            "field": "color",
            "value": 1.0
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Parse
    let rows: Vec<(Option<String>, Option<i32>, Option<String>)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('color:1.0');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(Option<String>, Option<i32>, Option<String>)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "parse": {
            "query_string": "color:1.0"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
}

#[rstest]
fn compound_queries(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    // Overview
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
        should => ARRAY[
            paradedb.boost(query => paradedb.term('description', 'shoes'), factor => 2.0),
            paradedb.term('description', 'running')
        ]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "boolean": {
            "should": [
                {"boost": {"query": {"term": {"field": "description", "value": "shoes"}}, "factor": 2.0}},
                {"term": {"field": "description", "value": "running"}}
            ]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // All
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
        should => ARRAY[paradedb.all()],
        must_not => ARRAY[paradedb.term('description', 'shoes')]
    )
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 38);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "boolean": {
            "should": [{"all": null}],
            "must_not": [{"term": {"field": "description", "value": "shoes"}}]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 38);

    // Boolean
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
        should => ARRAY[
          paradedb.term('description', 'headphones')
        ],
        must => ARRAY[
          paradedb.term('category', 'electronics'),
          paradedb.fuzzy_term('description', 'bluetooht')
        ],
        must_not => ARRAY[
          paradedb.range('rating', int4range(NULL, 2, '()'))
        ]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
        should => ARRAY[
          paradedb.term('description', 'headphones')
        ],
        must => ARRAY[
          paradedb.term('category', 'electronics'),
          paradedb.fuzzy_term('description', 'bluetooht')
        ],
        must_not => ARRAY[
          paradedb.range('rating', int4range(NULL, 2, '()'))
        ]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Boost
    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
      should => ARRAY[
        paradedb.term('description', 'shoes'),
        paradedb.boost(2.0, paradedb.term('description', 'running'))
      ]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@
    '{
        "boolean": {
            "should": [
                {"term": {"field": "description", "value": "shoes"}},
                {"boost": {"factor": 2.0, "query": {"term": {"field": "description", "value": "running"}}}}
            ]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Const score
    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@ paradedb.boolean(
      should => ARRAY[
        paradedb.const_score(1.0, paradedb.term('description', 'shoes')),
        paradedb.term('description', 'running')
      ]
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@
    '{
        "boolean": {
            "should": [
                {"const_score": {"score": 1.0, "query": {"term": {"field": "description", "value": "shoes"}}}},
                {"term": {"field": "description", "value": "running"}}
            ]
        }
    }'::jsonb;    
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Disjunction max
    // Test both function and JSON syntax for disjunction_max
    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@ paradedb.disjunction_max(ARRAY[
      paradedb.term('description', 'shoes'),
      paradedb.term('description', 'running')
    ]);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@
    '{
        "disjunction_max": {
            "disjuncts": [
                {"term": {"field": "description", "value": "shoes"}},
                {"term": {"field": "description", "value": "running"}}
            ]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String, f32)> = r#"
    SELECT description, rating, category, paradedb.score(id)
    FROM mock_items
    WHERE id @@@
    '{
        "disjunction_max": {
            "disjuncts": [
                {"term": {"field": "description", "value": "shoes"}},
                {"term": {"field": "description", "value": "running"}}
            ]
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    // Empty
    let rows: Vec<(String, i32, String, f32)> = r#"
    -- Returns no rows
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.empty();
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 0);

    let rows: Vec<(String, i32, String, f32)> = r#"
    -- Returns no rows
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ '{"empty": null}'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 0);

    // Parse
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('description:"running shoes" OR category:footwear');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.boolean(should => ARRAY[
      paradedb.phrase('description', ARRAY['running', 'shoes']),
      paradedb.term('category', 'footwear')
    ]);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ '{
      "parse": {"query_string": "description:\"running shoes\" OR category:footwear"}
    }'::jsonb
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ '{
      "boolean": {
        "should": [
          {
            "phrase": {
              "field": "description",
              "phrases": ["running", "shoes"]
            }
          },
          {
            "term": {
              "field": "category",
              "value": "footwear"
            }
          }
        ]
      }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 6);

    // Lenient parse
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('speaker electronics', lenient => true);
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "parse": {
            "query_string": "speaker electronics",
            "lenient": true
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    // Conjunction mode
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('description:speaker category:electronics');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('description:speaker OR category:electronics');
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "parse": {
            "query_string": "description:speaker category:electronics"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 5);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse(
      'description:speaker category:electronics',
      conjunction_mode => true
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse(
    'description:speaker AND category:electronics'
    )"#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "parse": {
            "query_string": "description:speaker category:electronics",
            "conjunction_mode": true
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "parse": {
            "query_string": "description:speaker AND category:electronics"
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    // Parse with field
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse_with_field(
      'description',
      'speaker bluetooth',
      conjunction_mode => true
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "parse_with_field": {
            "field": "description",
            "query_string": "speaker bluetooth",
            "conjunction_mode": true
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
}

#[rstest]
fn specialized_queries(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (
        key_field='id',
        text_fields='{
            "description": { "stored": true },
            "category": { "stored": true }
        }',
        numeric_fields='{"rating": { "stored": true } }',
        boolean_fields='{"in_stock": { "stored": true } }',
        json_fields='{"metadata": { "stored": true } }',
        datetime_fields='{
            "created_at": { "stored": true }
        }'
    );
    "#
    .execute(&mut conn);

    // More like this
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.more_like_this(
      document_id => 3,
      min_term_frequency => 1
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 16);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.more_like_this(
      document_fields => '{"description": "shoes"}',
      min_doc_frequency => 0,
      max_doc_frequency => 100,
      min_term_frequency => 1
    );
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "more_like_this": {
            "document_id": 3,
            "min_term_frequency": 1
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 16);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@
    '{
        "more_like_this": {
            "document_fields": [["description", "shoes"]],
            "min_doc_frequency": 0,
            "max_doc_frequency": 100,
            "min_term_frequency": 1
        }
    }'::jsonb;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 3);
}

#[rstest]
fn autocomplete(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let expected = vec![
        ("Sleek running shoes".into(), 5, "Footwear".into()),
        ("Generic shoes".into(), 4, "Footwear".into()),
        ("White jogging shoes".into(), 3, "Footwear".into()),
    ];

    // Fuzzy term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category FROM mock_items
    WHERE id @@@ paradedb.fuzzy_term(
        field => 'description',
        value => 'shoez'
    ) ORDER BY rating DESC
    "#
    .fetch(&mut conn);
    assert_eq!(rows, expected);

    // Match
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category FROM mock_items
    WHERE id @@@ paradedb.match(
        field => 'description',
        value => 'ruining shoez',
        distance => 2
    ) ORDER BY rating DESC
    "#
    .fetch(&mut conn);
    assert_eq!(rows, expected);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category FROM mock_items
    WHERE id @@@ paradedb.match(
        field => 'description',
        value => 'ruining shoez',
        distance => 2,
        conjunction_mode => true
    )
    "#
    .fetch(&mut conn);
    assert_eq!(rows, vec![expected[0].clone()]);

    // Multiple fuzzy fields
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category FROM mock_items
    WHERE id @@@ paradedb.boolean(
        should => ARRAY[
            paradedb.match(field => 'description', value => 'ruining shoez', distance => 2),
            paradedb.match(field => 'category', value => 'ruining shoez', distance => 2)
        ]
    ) ORDER BY rating DESC
    "#
    .fetch(&mut conn);
    assert_eq!(rows, expected);

    r#"
    DROP INDEX search_idx;
    CREATE INDEX ngrams_idx ON public.mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{"description": {"tokenizer": {"type": "ngram", "min_gram": 3, "max_gram": 3, "prefix_only": false}}}'
    );
    "#
    .execute(&mut conn);

    // Ngram term
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category FROM mock_items
    WHERE description @@@ 'sho'
    ORDER BY rating DESC
    "#
    .fetch(&mut conn);
    assert_eq!(rows, expected);

    // Ngram term set
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category FROM mock_items
    WHERE id @@@ paradedb.match(
        field => 'description',
        value => 'hsoes',
        distance => 0
    ) ORDER BY rating DESC
    "#
    .fetch(&mut conn);
    assert_eq!(rows, expected);
}

#[rstest]
fn hybrid_search(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');

    CREATE EXTENSION vector;
    ALTER TABLE mock_items ADD COLUMN embedding vector(3);

    UPDATE mock_items m
    SET embedding = ('[' ||
        ((m.id + 1) % 10 + 1)::integer || ',' ||
        ((m.id + 2) % 10 + 1)::integer || ',' ||
        ((m.id + 3) % 10 + 1)::integer || ']')::vector;
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, BigDecimal, String, Vector)> = r#"
    WITH bm25_candidates AS (
        SELECT id, paradedb.score(id)
        FROM mock_items
        WHERE description @@@ 'keyboard'
        ORDER BY paradedb.score(id) DESC
        LIMIT 20
    ),
    bm25_ranked AS (
        SELECT id, RANK() OVER (ORDER BY score DESC) AS rank
        FROM bm25_candidates
    ),
    semantic_search AS (
        SELECT id, RANK() OVER (ORDER BY embedding <=> '[1,2,3]') AS rank
        FROM mock_items
        ORDER BY embedding <=> '[1,2,3]'
        LIMIT 20
    )
    SELECT
        COALESCE(semantic_search.id, bm25_ranked.id) AS id,
        COALESCE(1.0 / (60 + semantic_search.rank), 0.0) +
        COALESCE(1.0 / (60 + bm25_ranked.rank), 0.0) AS score,
        mock_items.description,
        mock_items.embedding
    FROM semantic_search
    FULL OUTER JOIN bm25_ranked ON semantic_search.id = bm25_ranked.id
    JOIN mock_items ON mock_items.id = COALESCE(semantic_search.id, bm25_ranked.id)
    ORDER BY score DESC, description
    LIMIT 5;
    "#
    .fetch(&mut conn);

    // Expected results
    let expected = vec![
        (
            1,
            BigDecimal::from_str("0.03062178588125292193").unwrap(),
            String::from("Ergonomic metal keyboard"),
            Vector::from(vec![3.0, 4.0, 5.0]),
        ),
        (
            2,
            BigDecimal::from_str("0.02990695613646433318").unwrap(),
            String::from("Plastic Keyboard"),
            Vector::from(vec![4.0, 5.0, 6.0]),
        ),
        (
            19,
            BigDecimal::from_str("0.01639344262295081967").unwrap(),
            String::from("Artistic ceramic vase"),
            Vector::from(vec![1.0, 2.0, 3.0]),
        ),
        (
            29,
            BigDecimal::from_str("0.01639344262295081967").unwrap(),
            String::from("Designer wall paintings"),
            Vector::from(vec![1.0, 2.0, 3.0]),
        ),
        (
            39,
            BigDecimal::from_str("0.01639344262295081967").unwrap(),
            String::from("Handcrafted wooden frame"),
            Vector::from(vec![1.0, 2.0, 3.0]),
        ),
    ];

    // Compare each row individually
    for (actual, expected) in rows.iter().zip(expected.iter()) {
        assert_eq!(actual.0, expected.0); // Compare IDs
        assert_relative_eq!(
            actual.1.to_f64().unwrap(),
            expected.1.to_f64().unwrap(),
            epsilon = 0.000265
        ); // Compare BigDecimal scores
        assert_eq!(actual.2, expected.2); // Compare descriptions
        assert_eq!(actual.3, expected.3); // Compare embeddings
    }
}

#[rstest]
fn create_bm25_test_tables(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
        schema_name => 'public',
        table_name => 'orders',
        table_type => 'Orders'
    );

    CALL paradedb.create_bm25_test_table(
        schema_name => 'public',
        table_name => 'parts',
        table_type => 'Parts'
    );
    "#
    .execute(&mut conn);

    let rows: Vec<(i32, i32, i32, f32, String)> = r#"
        SELECT order_id, product_id, order_quantity, order_total::REAL, customer_name FROM orders
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 64);
    assert_eq!(rows[0], (1, 1, 3, 99.99, "John Doe".into()));

    let rows: Vec<(i32, i32, String)> = r#"
        SELECT part_id, parent_part_id, description FROM parts
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 36);
    assert_eq!(rows[0], (1, 0, "Chassis Assembly".into()));
}

#[rstest]
fn concurrent_indexing(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating)
    WITH (key_field='id'); 
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX CONCURRENTLY search_idx_v2 ON mock_items
    USING bm25 (id, description, category, rating, in_stock)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    r#"
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    // Verify the new index is being used by running a query that includes in_stock
    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE description @@@ 'shoes' AND id @@@ 'in_stock:true'
    ORDER BY rating DESC
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 2);
}

#[rstest]
fn schema(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let rows: Vec<(String, String)> =
        "SELECT name, field_type FROM paradedb.schema('search_idx')".fetch(&mut conn);

    let expected = vec![
        ("category".to_string(), "Str".to_string()),
        ("created_at".to_string(), "Date".to_string()),
        ("ctid".to_string(), "U64".to_string()),
        ("description".to_string(), "Str".to_string()),
        ("id".to_string(), "I64".to_string()),
        ("in_stock".to_string(), "Bool".to_string()),
        ("metadata".to_string(), "JsonObject".to_string()),
        ("rating".to_string(), "I64".to_string()),
    ];

    assert_eq!(rows, expected);
}

#[rstest]
fn index_size(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );

    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field='id');
    "#
    .execute(&mut conn);

    let size: i64 = "SELECT pg_relation_size('search_idx')"
        .fetch_one::<(i64,)>(&mut conn)
        .0;

    assert!(size > 0);
}

#[rstest]
fn field_configuration(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "ngram", "min_gram": 2, "max_gram": 3, "prefix_only": false}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "ngram", "min_gram": 2, "max_gram": 3, "prefix_only": false}
            },
            "category": {
                "tokenizer": {"type": "ngram", "min_gram": 2, "max_gram": 3, "prefix_only": false}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "fast": true,
            "tokenizer": {"type": "ngram", "min_gram": 2, "max_gram": 3, "prefix_only": false}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, metadata)
    WITH (
    key_field = 'id',
    json_fields = '{
        "metadata": {
        "fast": true
        }
    }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, rating)
    WITH (
        key_field = 'id',
        numeric_fields = '{
            "rating": {"fast": true}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, in_stock)
    WITH (
    key_field = 'id',
    boolean_fields = '{
        "in_stock": {"fast": true}
    }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, created_at)
    WITH (
    key_field = 'id',
    datetime_fields = '{
        "created_at": {"fast": true}
    }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, weight_range)
    WITH (
    key_field = 'id',
    range_fields = '{
        "weight_range": {"stored": true}
    }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);
}

#[rstest]
fn available_tokenizers(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "whitespace"}}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "default"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "whitespace"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "raw"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "regex", "pattern": "\\W+"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "ngram", "min_gram": 2, "max_gram": 3, "prefix_only": false}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "source_code"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "chinese_compatible"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field = 'id',
        text_fields = '{
            "description": {
            "tokenizer": {"type": "chinese_lindera"}
            }
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    if cfg!(feature = "icu") {
        r#"
        CREATE INDEX search_idx ON mock_items
        USING bm25 (id, description)
        WITH (
            key_field = 'id',
            text_fields = '{
                "description": {
                "tokenizer": {"type": "icu"}
                }
            }'
        );
        DROP INDEX search_idx;
        "#
        .execute(&mut conn);
    }

    r#"
    SELECT * FROM paradedb.tokenizers();
    "#
    .execute(&mut conn);

    r#"
    SELECT * FROM paradedb.tokenize(
    paradedb.tokenizer('ngram', min_gram => 3, max_gram => 3, prefix_only => false),
    'keyboard'
    );
    "#
    .execute(&mut conn);

    // Test multiple tokenizers for the same field
    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "whitespace"}},
            "description_ngram": {"tokenizer": {"type": "ngram", "min_gram": 3, "max_gram": 3, "prefix_only": false}, "column": "description"},
            "description_stem": {"tokenizer": {"type": "default", "stemmer": "English"}, "column": "description"}
        }'
    );
    "#
    .execute(&mut conn);

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('description_ngram:cam AND description_stem:digitally')
    ORDER BY rating DESC
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].0.contains("camera"));
    assert!(rows[0].0.contains("digital"));

    let rows: Vec<(String, i32, String)> = r#"
    SELECT description, rating, category
    FROM mock_items
    WHERE id @@@ paradedb.parse('description:"Soft cotton" OR description_stem:shirts')
    ORDER BY rating DESC
    LIMIT 5;
    "#
    .fetch(&mut conn);
    assert_eq!(rows.len(), 1);
    assert!(rows.iter().any(|r| r.0.contains("cotton")));
    assert!(rows.iter().any(|r| r.0.contains("shirt")));
}

#[rstest]
fn token_filters(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "default", "stemmer": "English"}}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "default", "remove_long": 255}}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"tokenizer": {"type": "default", "lowercase": false}}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);
}

#[rstest]
fn fast_fields(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, rating)
    WITH (
        key_field = 'id',
        text_fields ='{
            "description": {"fast": true}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, category)
    WITH (
        key_field='id',
        text_fields='{
            "category": {"fast": true, "normalizer": "raw"}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);
}

#[rstest]
fn record(mut conn: PgConnection) {
    r#"
    CALL paradedb.create_bm25_test_table(
      schema_name => 'public',
      table_name => 'mock_items'
    );
    "#
    .execute(&mut conn);

    r#"
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (
        key_field='id',
        text_fields='{
            "description": {"record": "freq"}
        }'
    );
    DROP INDEX search_idx;
    "#
    .execute(&mut conn);
}
