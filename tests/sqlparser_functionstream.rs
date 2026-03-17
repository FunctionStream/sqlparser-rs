// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]

use sqlparser::ast::{BinaryOperator, ColumnOption, Expr, Ident, Statement, TableConstraint};
use sqlparser::dialect::FunctionStreamDialect;
use sqlparser::parser::Parser;
use sqlparser::test_utils;
use sqlparser::tokenizer::{Location, Span};

#[test]
fn test_watermark_with_expr() {
    let sql = "CREATE TABLE orders (
        customer_id INT,
        order_id INT,
        date_string TEXT,
        timestamp TIMESTAMP GENERATED ALWAYS AS (CAST(date_string as TIMESTAMP)),
        WATERMARK FOR timestamp AS timestamp + 5
    ) WITH (
        connector = 'kafka',
        format = 'json',
        type = 'source',
        bootstrap_servers = 'localhost:9092',
        topic = 'order_topic'
    )";

    let parse = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(
        ct.constraints,
        vec![TableConstraint::Watermark {
            column_name: Ident::new("timestamp"),
            watermark_expr: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("timestamp"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(
                    test_utils::number("5")
                        .with_span(Span::new(Location::new(5, 4), Location::new(5, 10)))
                )),
            }),
        }]
    );
}

#[test]
fn test_watermark_without_expr() {
    let sql = "CREATE TABLE users (
        customer_id INT,
        timestamp TIMESTAMP,
        WATERMARK FOR timestamp
    ) WITH (
        connector = 'kafka',
        format = 'json',
        type = 'source',
        bootstrap_servers = 'localhost:9092',
        topic = 'order_topic'
    )";

    let parse = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(
        ct.constraints,
        vec![TableConstraint::Watermark {
            column_name: Ident::new("timestamp"),
            watermark_expr: None,
        }]
    );
}

#[test]
fn test_metadata_field() {
    let sql = "CREATE TABLE logs (
        id TEXT,
        kafka_topic STRING METADATA FROM 'topic',
        log TEXT
    )";

    let parse = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(ct.columns.len(), 3);

    // Check the middle column with METADATA FROM
    let column = &ct.columns[1];
    assert_eq!(column.name, Ident::new("kafka_topic"));

    // Check for the METADATA FROM option
    let mut found_metadata = false;
    for option_def in &column.options {
        if let ColumnOption::MetadataField(key, _) = &option_def.option {
            found_metadata = true;
            assert_eq!(key, "topic");
        }
    }

    assert!(
        found_metadata,
        "Expected METADATA FROM option in column definition"
    );
}

#[test]
fn test_iceberg_partitioned_by() {
    let sql = "CREATE TABLE ice (
        ts TIMESTAMP NOT NULL,
        id INT NOT NULL,
        favorite_color TEXT
    ) WITH (
        connector = 'iceberg',
        format = 'parquet',
        table_name = 'functionstream_test'
    ) PARTITIONED BY (
        hour(ts),
        bucket(32, id),
        truncate(8, favorite_color)
    )";

    let parse = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    // Verify basic structure
    assert_eq!(ct.name.to_string(), "ice");
    assert_eq!(ct.columns.len(), 3);

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions to be Some");
    assert_eq!(partitions.len(), 3);

    // Check each partition transform
    // hour(ts)
    match &partitions[0] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "hour");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 1);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for hour(ts)"),
    }

    // bucket(32, id)
    match &partitions[1] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "bucket");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 2);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for bucket(32, id)"),
    }

    // truncate(8, favorite_color)
    match &partitions[2] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "truncate");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 2);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for truncate(8, favorite_color)"),
    }

    // Test round-trip: the formatted output should parse back to the same structure
    let formatted = ct.to_string();
    let reparsed = Parser::parse_sql(&FunctionStreamDialect {}, &formatted).unwrap();
    let Statement::CreateTable(ct2) = reparsed.get(0).unwrap() else {
        panic!("not create table on reparse")
    };

    assert_eq!(ct.arroyo_partitions, ct2.arroyo_partitions);
}

#[test]
fn test_iceberg_partitioned_by_single() {
    let sql = "CREATE TABLE events (
        event_time TIMESTAMP
    ) WITH (
        connector = 'iceberg'
    ) PARTITIONED BY (day(event_time))";

    let parse = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions");
    assert_eq!(partitions.len(), 1);

    match &partitions[0] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "day");
        }
        _ => panic!("Expected Function for day(event_time)"),
    }
}

#[test]
fn test_iceberg_partitioned_by_identity() {
    // Test identity transform (just a column name)
    let sql = "CREATE TABLE data (
        region TEXT,
        value INT
    ) WITH (
        connector = 'iceberg'
    ) PARTITIONED BY (region)";

    let parse = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions");
    assert_eq!(partitions.len(), 1);

    match &partitions[0] {
        Expr::Identifier(ident) => {
            assert_eq!(ident.value, "region");
        }
        _ => panic!("Expected Identifier for region"),
    }
}

#[test]
fn test_create_function_with() {
    let sql = "CREATE FUNCTION WITH ('name' = 'my_func', 'path' = '/path/to/jar')";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::CreateFunctionWith { options } = &stmts[0] else {
        panic!("expected CreateFunctionWith, got {:?}", stmts[0]);
    };
    assert_eq!(options.len(), 2);
    let formatted = stmts[0].to_string();
    let reparsed = Parser::parse_sql(&FunctionStreamDialect {}, &formatted).unwrap();
    assert_eq!(stmts.len(), reparsed.len());
}

#[test]
fn test_drop_function() {
    let sql = "DROP FUNCTION my_func";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::DropFunction { func_desc, .. } = &stmts[0] else {
        panic!("expected DropFunction, got {:?}", stmts[0]);
    };
    assert_eq!(func_desc.len(), 1);
    assert_eq!(func_desc[0].name.to_string(), "my_func");
}

#[test]
fn test_start_function() {
    let sql = "START FUNCTION my_func";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::StartFunction { name } = &stmts[0] else {
        panic!("expected StartFunction, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "my_func");
    assert_eq!(stmts[0].to_string(), "START FUNCTION my_func");
}

#[test]
fn test_stop_function() {
    let sql = "STOP FUNCTION my_func";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::StopFunction { name } = &stmts[0] else {
        panic!("expected StopFunction, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "my_func");
    assert_eq!(stmts[0].to_string(), "STOP FUNCTION my_func");
}

#[test]
fn test_show_functions() {
    let sql = "SHOW FUNCTIONS";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::ShowFunctions { .. } = &stmts[0] else {
        panic!("expected ShowFunctions, got {:?}", stmts[0]);
    };
    assert_eq!(stmts[0].to_string(), "SHOW FUNCTIONS");
}

#[test]
fn test_show_functions_case_insensitive() {
    for sql in [
        "SHOW FUNCTIONS",
        "show functions",
        "Show Functions",
        "sHoW fUnCtIoNs",
    ] {
        let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
        assert_eq!(stmts.len(), 1, "failed for {:?}", sql);
        let Statement::ShowFunctions { .. } = &stmts[0] else {
            panic!("expected ShowFunctions for {:?}, got {:?}", sql, stmts[0]);
        };
    }
}

#[test]
fn test_create_streaming_table() {
    let sql = r#"CREATE STREAMING TABLE error_logs_pipeline
WITH (
    'parallelism' = '4',
    'checkpoint_interval' = '10s',
    'connector' = 'kafka',
    'kafka.topic' = 'errors_topic',
    'kafka.broker' = 'localhost:9092'
)
COMMENT "Extracts error level logs and routes to Kafka"
AS
SELECT
    hop_start(event_time, interval '1' minute, interval '5' minute) as window_start,
    device_id,
    count(*) as error_count
FROM raw_stream
WHERE level = 'ERROR'
GROUP BY 1, 2"#;
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::CreateStreamingTable {
        name,
        with_options,
        comment,
        query,
    } = &stmts[0]
    else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "error_logs_pipeline");
    assert_eq!(with_options.len(), 5);
    assert_eq!(
        comment.as_deref(),
        Some("Extracts error level logs and routes to Kafka")
    );
    assert!(matches!(*query.body, sqlparser::ast::SetExpr::Select(_)));
}

#[test]
fn test_create_streaming_table_minimal() {
    let sql = "CREATE STREAMING TABLE t AS SELECT 1";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::CreateStreamingTable {
        name,
        with_options,
        comment,
        query,
    } = &stmts[0]
    else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "t");
    assert!(with_options.is_empty());
    assert!(comment.is_none());
    assert!(matches!(*query.body, sqlparser::ast::SetExpr::Select(_)));
}

#[test]
fn test_create_streaming_table_with_only() {
    let sql = "CREATE STREAMING TABLE sink_t WITH ('connector' = 'kafka', 'topic' = 'out') AS SELECT * FROM src";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::CreateStreamingTable {
        name,
        with_options,
        comment,
        ..
    } = &stmts[0]
    else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "sink_t");
    assert_eq!(with_options.len(), 2);
    assert!(comment.is_none());
}

#[test]
fn test_create_streaming_table_comment_single_quoted() {
    let sql = "CREATE STREAMING TABLE t COMMENT 'single quoted comment' AS SELECT 1 as x";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::CreateStreamingTable { name, comment, .. } = &stmts[0] else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "t");
    assert_eq!(comment.as_deref(), Some("single quoted comment"));
}

#[test]
fn test_create_streaming_table_roundtrip() {
    let sql =
        "CREATE STREAMING TABLE my_pipeline WITH ('a' = '1') COMMENT 'desc' AS SELECT id FROM t";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let formatted = stmts[0].to_string();
    let reparsed = Parser::parse_sql(&FunctionStreamDialect {}, &formatted).unwrap();
    assert_eq!(reparsed.len(), 1);
    let Statement::CreateStreamingTable {
        name: n2,
        with_options: w2,
        comment: c2,
        ..
    } = &reparsed[0]
    else {
        panic!(
            "expected CreateStreamingTable on reparse, got {:?}",
            reparsed[0]
        );
    };
    let Statement::CreateStreamingTable {
        name: n1,
        with_options: w1,
        comment: c1,
        ..
    } = &stmts[0]
    else {
        panic!("unreachable");
    };
    assert_eq!(n1.to_string(), n2.to_string());
    assert_eq!(w1.len(), w2.len());
    assert_eq!(c1, c2);
}

#[test]
fn test_create_streaming_table_qualified_name() {
    let sql = "CREATE STREAMING TABLE db.schema.pipeline AS SELECT 1";
    let stmts = Parser::parse_sql(&FunctionStreamDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let Statement::CreateStreamingTable { name, .. } = &stmts[0] else {
        panic!("expected CreateStreamingTable, got {:?}", stmts[0]);
    };
    assert_eq!(name.to_string(), "db.schema.pipeline");
}
