mod fixtures;

use async_std::stream::StreamExt;
use fixtures::*;

use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::{types::BigDecimal, PgConnection};
use std::str::FromStr;
use time::{macros::format_description, Date, PrimitiveDateTime};

#[rstest]
#[ignore]
fn basic_select(mut conn: PgConnection) {
    UserSessionLogsTable::setup().execute(&mut conn);

    let columns: UserSessionLogsTableVec =
        "SELECT * FROM user_session_logs ORDER BY id".fetch_collect(&mut conn);

    // Check that the first ten ids are in order.
    let ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    assert_eq!(&columns.id[0..10], ids, "ids are in expected order");
    let event_names =
        "Login,Purchase,Logout,Signup,ViewProduct,AddToCart,RemoveFromCart,Checkout,Payment,Review";

    assert_eq!(
        &columns.event_name[0..10],
        event_names.split(',').collect::<Vec<_>>(),
        "event names are in expected order"
    );
}

#[rstest]
#[ignore]
fn array_results(mut conn: PgConnection) {
    ResearchProjectArraysTable::setup().execute(&mut conn);

    let columns: Vec<ResearchProjectArraysTable> =
        "SELECT * FROM research_project_arrays".fetch_collect(&mut conn);

    // Using defaults for fields below that are unimplemented.
    let first = ResearchProjectArraysTable {
        project_id: Default::default(),
        experiment_flags: vec![false, true, false],
        binary_data: Default::default(),
        notes: vec![
            "Need to re-evaluate methodology".into(),
            "Unexpected results in phase 2".into(),
        ],
        keywords: vec!["sustainable farming".into(), "soil health".into()],
        short_descriptions: vec!["FARMEX    ".into(), "SOILQ2    ".into()],
        participant_ages: vec![22, 27, 32],
        participant_ids: vec![201, 202, 203],
        observation_counts: vec![160, 140, 135],
        related_project_o_ids: Default::default(),
        measurement_errors: vec![0.025, 0.02, 0.01],
        precise_measurements: vec![2.0, 2.1, 2.2],
        observation_timestamps: Default::default(),
        observation_dates: Default::default(),
        budget_allocations: Default::default(),
        participant_uuids: Default::default(),
    };

    let second = ResearchProjectArraysTable {
        project_id: Default::default(),
        experiment_flags: vec![true, false, true],
        binary_data: Default::default(),
        notes: vec![
            "Initial setup complete".into(),
            "Preliminary results promising".into(),
        ],
        keywords: vec!["climate change".into(), "coral reefs".into()],
        short_descriptions: vec!["CRLRST    ".into(), "OCEAN1    ".into()],
        participant_ages: vec![28, 34, 29],
        participant_ids: vec![101, 102, 103],
        observation_counts: vec![150, 120, 130],
        related_project_o_ids: Default::default(),
        measurement_errors: vec![0.02, 0.03, 0.015],
        precise_measurements: vec![1.5, 1.6, 1.7],
        observation_timestamps: Default::default(),
        observation_dates: Default::default(),
        budget_allocations: Default::default(),
        participant_uuids: Default::default(),
    };

    assert_eq!(columns[0], first);
    assert_eq!(columns[1], second);
}

#[rstest]
#[ignore]
fn alter(mut conn: PgConnection) {
    match "CREATE TABLE t (a int, b text) USING deltalake; ALTER TABLE t ADD COLUMN c int"
        .execute_result(&mut conn)
    {
        Err(err) => assert!(
            err.to_string().contains("ALTER TABLE is not yet supported"),
            "alter table error message not present in '{}'",
            err
        ),
        _ => panic!("alter table should be unsupported"),
    }
}

#[rstest]
#[ignore = "known bug where results after delete are out of order"]
fn delete(mut conn: PgConnection) {
    "CREATE TABLE employees (salary bigint, id smallint) USING deltalake".execute(&mut conn);

    "INSERT INTO employees VALUES (100, 1), (200, 2), (300, 3), (400, 4), (500, 5)"
        .execute(&mut conn);
    "DELETE FROM employees WHERE id = 5 OR salary <= 200".execute(&mut conn);

    // TODO: Known bug here! The results are not in the correct order!
    let rows: Vec<(i64, i16)> = "SELECT * FROM employees".fetch(&mut conn);
    assert_eq!(rows, vec![(300, 3), (400, 4)]);
}

#[rstest]
#[ignore]
fn drop(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text) USING deltalake".execute(&mut conn);
    "DROP TABLE t".execute(&mut conn);

    match "SELECT * FROM t".fetch_result::<()>(&mut conn) {
        Ok(_) => panic!("relation 't' should not exist after drop"),
        Err(err) => assert!(err.to_string().contains("does not exist")),
    };

    "CREATE TABLE t (a int, b text) USING deltalake".execute(&mut conn);
    "CREATE TABLE s (a int, b text)".execute(&mut conn);
    "DROP TABLE s, t".execute(&mut conn);

    match "SELECT * FROM s".fetch_result::<()>(&mut conn) {
        Ok(_) => panic!("relation 's' should not exist after drop"),
        Err(err) => assert!(err.to_string().contains("does not exist")),
    };

    match "SELECT * FROM t".fetch_result::<()>(&mut conn) {
        Ok(_) => panic!("relation 's' should not exist after drop"),
        Err(err) => assert!(err.to_string().contains("does not exist")),
    };
}

#[rstest]
#[ignore]
fn insert(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b int)".execute(&mut conn);
    "INSERT INTO t VALUES (1, 2)".execute(&mut conn);
    "CREATE TABLE s (a int, b int) USING deltalake".execute(&mut conn);
    "INSERT INTO s SELECT * FROM t".execute(&mut conn);

    let rows: Vec<(i32, i32)> = "SELECT * FROM s".fetch(&mut conn);
    assert_eq!(rows[0], (1, 2));
}

#[rstest]
#[ignore]
fn join_two_deltalake_tables(mut conn: PgConnection) {
    "CREATE TABLE t ( id INT PRIMARY KEY, name VARCHAR(50), department_id INT ) USING deltalake"
        .execute(&mut conn);
    "CREATE TABLE s ( id INT PRIMARY KEY, department_name VARCHAR(50) ) USING deltalake"
        .execute(&mut conn);

    r#"
    INSERT INTO t (id, name, department_id) VALUES
    (1, 'Alice', 101),
    (2, 'Bob', 102),
    (3, 'Charlie', 103),
    (4, 'David', 101);
    INSERT INTO s (id, department_name) VALUES
    (101, 'Human Resources'),
    (102, 'Finance'),
    (103, 'IT');
    "#
    .execute(&mut conn);

    let count: (i64,) =
        "SELECT COUNT(*) FROM t JOIN s ON t.department_id = s.id".fetch_one(&mut conn);
    assert_eq!(count, (4,));
}

#[rstest]
#[ignore]
fn join_heap_and_deltalake_table(mut conn: PgConnection) {
    "CREATE TABLE u ( id INT PRIMARY KEY, name VARCHAR(50), department_id INT ) USING deltalake"
        .execute(&mut conn);
    "CREATE TABLE v ( id INT PRIMARY KEY, department_name VARCHAR(50) )".execute(&mut conn);
    r#"
    INSERT INTO u (id, name, department_id) VALUES
    (1, 'Alice', 101),
    (2, 'Bob', 102),
    (3, 'Charlie', 103),
    (4, 'David', 101);
    INSERT INTO v (id, department_name) VALUES
    (101, 'Human Resources'),
    (102, 'Finance'),
    (103, 'IT');
    "#
    .execute(&mut conn);

    match "SELECT COUNT(*) FROM u JOIN v ON u.department_id = v.id".fetch_result::<()>(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not yet supported")),
        _ => panic!("heap and deltalake talbes in same query should be unsupported"),
    }
}

#[rstest]
#[ignore]
fn rename(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text) USING deltalake".execute(&mut conn);
    "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')".execute(&mut conn);
    "ALTER TABLE t RENAME TO s".execute(&mut conn);

    let rows: Vec<(i32, String)> = "SELECT * FROM s".fetch(&mut conn);
    assert_eq!(rows[0], (3, "c".into()));
    assert_eq!(rows[1], (2, "b".into()));
    assert_eq!(rows[2], (1, "a".into()));
}

#[rstest]
#[ignore]
fn schema(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text NOT NULL) USING deltalake".execute(&mut conn);
    "INSERT INTO t values (1, 'test');".execute(&mut conn);

    let row: (i32, String) = "SELECT * FROM t".fetch_one(&mut conn);
    assert_eq!(row, (1, "test".into()));
}

#[rstest]
#[ignore]
fn select(mut conn: PgConnection) {
    UserSessionLogsTable::setup().execute(&mut conn);

    let rows: Vec<(Date, BigDecimal)> = r#"
    SELECT event_date, SUM(revenue) AS total_revenue
    FROM user_session_logs
    GROUP BY event_date
    ORDER BY event_date"#
        .fetch(&mut conn);

    let expected_dates = "
        2024-01-01,2024-01-02,2024-01-03,2024-01-04,2024-01-05,2024-01-06,2024-01-07,
        2024-01-08,2024-01-09,2024-01-10,2024-01-11,2024-01-12,2024-01-13,2024-01-14,
        2024-01-15,2024-01-16,2024-01-17,2024-01-18,2024-01-19,2024-01-20"
        .split(',')
        .map(|s| Date::parse(s.trim(), format_description!("[year]-[month]-[day]")).unwrap());

    let expected_revenues = "
        20.00,150.50,0.00,0.00,30.75,75.00,0.00,200.25,300.00,50.00,0.00,125.30,0.00,
        0.00,45.00,80.00,0.00,175.50,250.00,60.00"
        .split(',')
        .map(|s| BigDecimal::from_str(s.trim()).unwrap());

    assert!(rows.iter().map(|r| r.0).eq(expected_dates));
    assert!(rows.iter().map(|r| r.1.clone()).eq(expected_revenues));
}

#[rstest]
#[ignore]
fn truncate(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text) USING deltalake".execute(&mut conn);
    "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'); TRUNCATE t".execute(&mut conn);

    let rows: Vec<(i32, String)> = "SELECT * FROM t".fetch(&mut conn);
    assert!(rows.is_empty())
}

#[rstest]
#[ignore]
fn types(mut conn: PgConnection) {
    "CREATE TABLE test_text (a text) USING deltalake".execute(&mut conn);
    "INSERT INTO test_text VALUES ('hello world')".execute(&mut conn);
    let row: (String,) = "SELECT * FROM test_text".fetch_one(&mut conn);
    assert_eq!(row.0, "hello world".to_string());

    "CREATE TABLE test_varchar (a varchar) USING deltalake".execute(&mut conn);
    "INSERT INTO test_varchar VALUES ('hello world')".execute(&mut conn);
    let row: (String,) = "SELECT * FROM test_varchar".fetch_one(&mut conn);
    assert_eq!(row.0, "hello world".to_string());

    "CREATE TABLE test_char (a char) USING deltalake".execute(&mut conn);
    "INSERT INTO test_char VALUES ('h')".execute(&mut conn);
    let row: (String,) = "SELECT * FROM test_char".fetch_one(&mut conn);
    assert_eq!(row.0, "h".to_string());

    "CREATE TABLE test_smallint (a smallint) USING deltalake".execute(&mut conn);
    "INSERT INTO test_smallint VALUES (1)".execute(&mut conn);
    let row: (i16,) = "SELECT * FROM test_smallint".fetch_one(&mut conn);
    assert_eq!(row.0, 1);

    "CREATE TABLE test_integer (a integer) USING deltalake".execute(&mut conn);
    "INSERT INTO test_integer VALUES (1)".execute(&mut conn);
    let row: (i32,) = "SELECT * FROM test_integer".fetch_one(&mut conn);
    assert_eq!(row.0, 1);

    "CREATE TABLE test_bigint (a bigint) USING deltalake".execute(&mut conn);
    "INSERT INTO test_bigint VALUES (1)".execute(&mut conn);
    let row: (i64,) = "SELECT * FROM test_bigint".fetch_one(&mut conn);
    assert_eq!(row.0, 1);

    "CREATE TABLE test_real (a real) USING deltalake".execute(&mut conn);
    "INSERT INTO test_real VALUES (1.0)".execute(&mut conn);
    let row: (f32,) = "SELECT * FROM test_real".fetch_one(&mut conn);
    assert_eq!(row.0, 1.0);

    "CREATE TABLE test_double (a double precision) USING deltalake".execute(&mut conn);
    "INSERT INTO test_double VALUES (1.0)".execute(&mut conn);
    let row: (f64,) = "SELECT * FROM test_double".fetch_one(&mut conn);
    assert_eq!(row.0, 1.0);

    "CREATE TABLE test_bool (a bool) USING deltalake".execute(&mut conn);
    "INSERT INTO test_bool VALUES (true)".execute(&mut conn);
    let row: (bool,) = "SELECT * FROM test_bool".fetch_one(&mut conn);
    assert_eq!(row.0, true);

    "CREATE TABLE test_numeric (a numeric(5, 2)) USING deltalake".execute(&mut conn);
    "INSERT INTO test_numeric VALUES (1.01)".execute(&mut conn);
    let row: (BigDecimal,) = "SELECT * FROM test_numeric".fetch_one(&mut conn);
    assert_eq!(row.0, BigDecimal::from_str("1.01").unwrap());

    "CREATE TABLE test_timestamp (a timestamp) USING deltalake".execute(&mut conn);
    "INSERT INTO test_timestamp VALUES ('2024-01-29 15:30:00')".execute(&mut conn);
    let row: (PrimitiveDateTime,) = "SELECT * FROM test_timestamp".fetch_one(&mut conn);
    let fd = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    assert_eq!(
        row.0,
        PrimitiveDateTime::parse("2024-01-29 15:30:00", fd).unwrap()
    );

    "CREATE TABLE test_date (a date) USING deltalake".execute(&mut conn);
    "INSERT INTO test_date VALUES ('2024-01-29')".execute(&mut conn);
    let row: (Date,) = "SELECT * FROM test_date".fetch_one(&mut conn);
    let fd = format_description!("[year]-[month]-[day]");
    assert_eq!(row.0, Date::parse("2024-01-29", fd).unwrap());

    match "CREATE TABLE t (a bytea) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("bytes should not be supported"),
    };
    match "CREATE TABLE t (a uuid) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("uuid should not be supported"),
    };
    match "CREATE TABLE t (a oid) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("oid should not be supported"),
    };
    match "CREATE TABLE t (a json) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("json should not be supported"),
    };
    match "CREATE TABLE t (a jsonb) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("jsonb should not be supported"),
    };
    match "CREATE TABLE t (a time) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("time should not be supported"),
    };
    match "CREATE TABLE t (a timetz) USING deltalake".execute_result(&mut conn) {
        Err(err) => assert!(err.to_string().contains("not supported")),
        _ => panic!("timetz should not be supported"),
    };
}

#[rstest]
#[ignore = "known bug where vacuuming breaks other databases"]
fn vacuum(mut conn: PgConnection) {
    "CREATE TABLE t (a int) USING deltalake".execute(&mut conn);
    "CREATE TABLE s (a int)".execute(&mut conn);
    "INSERT INTO t VALUES (1), (2), (3)".execute(&mut conn);
    "INSERT INTO s VALUES (4), (5), (6)".execute(&mut conn);
    "VACUUM".execute(&mut conn);
    "VACUUM FULL".execute(&mut conn);
    "VACUUM t".execute(&mut conn);
    "VACUUM FULL t".execute(&mut conn);
    "DROP TABLE t, s".execute(&mut conn);
    "VACUUM".execute(&mut conn);
}

#[rstest]
async fn copy_out_arrays(mut conn: PgConnection) {
    ResearchProjectArraysTable::setup().execute(&mut conn);

    let mut copy = conn
        .copy_out_raw(
            "COPY (SELECT * FROM research_project_arrays) TO STDOUT WITH (FORMAT CSV, HEADER)",
        )
        .await
        .unwrap();

    assert_eq!(copy.next().await.unwrap().unwrap(), "experiment_flags,notes,keywords,short_descriptions,participant_ages,participant_ids,observation_counts,measurement_errors,precise_measurements\n");
    assert_eq!(String::from_utf8_lossy(&copy.next().await.unwrap().unwrap()), "\"{f,t,f}\",\"{\"\"Need to re-evaluate methodology\"\",\"\"Unexpected results in phase 2\"\"}\",\"{\"\"sustainable farming\"\",\"\"soil health\"\"}\",\"{\"\"FARMEX    \"\",\"\"SOILQ2    \"\"}\",\"{22,27,32}\",\"{201,202,203}\",\"{160,140,135}\",\"{0.025,0.02,0.01}\",\"{2,2.1,2.2}\"\n");
    assert_eq!(String::from_utf8_lossy(&copy.next().await.unwrap().unwrap()), "\"{t,f,t}\",\"{\"\"Initial setup complete\"\",\"\"Preliminary results promising\"\"}\",\"{\"\"climate change\"\",\"\"coral reefs\"\"}\",\"{\"\"CRLRST    \"\",\"\"OCEAN1    \"\"}\",\"{28,34,29}\",\"{101,102,103}\",\"{150,120,130}\",\"{0.02,0.03,0.015}\",\"{1.5,1.6,1.7}\"\n");
    assert_eq!(copy.next().await.is_some(), false);
}

#[rstest]
async fn copy_out_basic(mut conn: PgConnection) {
    UserSessionLogsTable::setup().execute(&mut conn);

    let mut copy = conn
        .copy_out_raw(
            "COPY (SELECT * FROM user_session_logs ORDER BY id) TO STDOUT WITH (FORMAT CSV, HEADER)",
        )
        .await
        .unwrap();

    assert_eq!(
        copy.next().await.unwrap().unwrap(),
        "id,event_date,user_id,event_name,session_duration,page_views,revenue\n"
    );
    assert_eq!(
        String::from_utf8_lossy(&copy.next().await.unwrap().unwrap()),
        "1,2024-01-01,1,Login,300,5,20.00\n"
    );
    assert_eq!(
        String::from_utf8_lossy(&copy.next().await.unwrap().unwrap()),
        "2,2024-01-02,2,Purchase,450,8,150.50\n"
    );
    assert_eq!(
        String::from_utf8_lossy(&copy.next().await.unwrap().unwrap()),
        "3,2024-01-03,3,Logout,100,2,0.00\n"
    );
    assert_eq!(
        String::from_utf8_lossy(&copy.next().await.unwrap().unwrap()),
        "4,2024-01-04,4,Signup,200,3,0.00\n"
    );
}

#[rstest]
fn add_column(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text) USING deltalake".execute(&mut conn);

    match "ALTER TABLE t ADD COLUMN a int".execute_result(&mut conn) {
        Err(err) => assert_eq!(
            err.to_string(),
            "error returned from database: column \"a\" of relation \"t\" already exists"
        ),
        _ => panic!("Adding a column with the same name should not be supported"),
    };

    "ALTER TABLE t ADD COLUMN c int".execute(&mut conn);
    "INSERT INTO t VALUES (1, 'a', 2)".execute(&mut conn);
    let row: (i32, String, i32) = "SELECT * FROM t".fetch_one(&mut conn);
    assert_eq!(row, (1, "a".into(), 2));
}

#[rstest]
fn drop_column(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text, c int) USING deltalake".execute(&mut conn);

    match "ALTER TABLE t DROP COLUMN a".execute_result(&mut conn) {
        Err(err) => assert_eq!(err.to_string(), "error returned from database: DROP COLUMN is not yet supported. Please recreate the table instead."),
        _ => panic!("Dropping a column should not be supported"),
    };
}

#[rstest]
fn rename_column(mut conn: PgConnection) {
    "CREATE TABLE t (a int, b text) USING deltalake".execute(&mut conn);

    match "ALTER TABLE t RENAME COLUMN a TO c".execute_result(&mut conn) {
        Err(err) => assert_eq!(err.to_string(), "error returned from database: RENAME COLUMN is not yet supported. Please recreate the table instead."),
        _ => panic!("Renaming a column should not be supported"),
    };
}
