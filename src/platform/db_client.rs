use super::Settings;
use crate::utils::get_utc_now_str;
use crate::utils::to_sql_type;
use std::boxed::Box;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tokio_postgres::Error;
use tokio_postgres::NoTls;
use tokio_postgres::Row;
use tokio_postgres::Statement;
use tracing::info;
use uuid::Uuid;

use anyhow::Result;

#[derive(Debug)]
pub struct SqlQueryBuilder;

impl SqlQueryBuilder {
    pub fn prepare_insert_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        let mut sql = format!("INSERT INTO {} ({})", table, columns.join(", "));
        let mut placeholders: String = (1..=columns.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<String>>()
            .join(", ");

        format!("{} VALUES ({})", sql, placeholders)
    }

    pub fn prepare_update_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        let mut sql = format!("UPDATE {} SET", table);

        let mut placeholders: String = (1..=columns.len() - 1)
            .map(|i| format!("{} = ${}", columns[i - 1], i))
            .collect::<Vec<String>>()
            .join(", ");

        let num_of_cols = columns.len();
        format!(
            "{} {} WHERE {} = ${}",
            sql,
            placeholders,
            columns[num_of_cols - 1],
            num_of_cols
        )
    }

    pub fn prepare_fetch_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        if columns.len() == 0 {
            return format!("SELECT * FROM {}", table);
        }

        let mut sql = format!("SELECT * FROM {}", table);
        let mut placeholders: String = (1..=columns.len())
            .map(|i| format!("{} = ${}", columns[i - 1], i))
            .collect::<Vec<String>>()
            .join(" AND ");

        format!("{} WHERE {}", sql, placeholders)
    }

    pub fn prepare_delete_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        if columns.len() == 0 {
            return format!("DELETE FROM {}", table);
        }

        let mut sql = format!("DELETE FROM {}", table);
        let mut placeholders: String = (1..=columns.len())
            .map(|i| format!("{} = ${}", columns[i - 1], i))
            .collect::<Vec<String>>()
            .join(" AND ");

        format!("{} WHERE {}", sql, placeholders)
    }
}

#[derive(Debug)]
pub struct DBClient {
    client: Client,
    query_builder: SqlQueryBuilder,
}

impl DBClient {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>> {
        let db_cfg = &settings.database;
        let database_url = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode=disable",
            db_cfg.user, db_cfg.password, db_cfg.host, db_cfg.port, db_cfg.db_name
        );
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("db connection error: {}", e);
            }
        });

        Ok(Arc::new(DBClient {
            client,
            query_builder: SqlQueryBuilder {},
        }))
    }

    pub async fn insert(
        &self,
        table: &str,
        columns: Vec<&str>,
        values: Vec<Box<dyn ToSql + Sync>>,
    ) -> Result<()> {
        let stmt = self.query_builder.prepare_insert_statement(table, &columns);
        let values_refs: Vec<&(dyn ToSql + Sync)> =
            values.iter().map(|boxed| boxed.as_ref()).collect();
        self.client.execute(&stmt, &values_refs).await?;
        Ok(())
    }

    pub async fn update(
        &self,
        table: &str,
        columns: Vec<&str>,
        values: Vec<Box<dyn ToSql + Sync>>,
    ) -> Result<()> {
        let stmt = self.query_builder.prepare_update_statement(table, &columns);
        let values_refs: Vec<&(dyn ToSql + Sync)> =
            values.iter().map(|boxed| boxed.as_ref()).collect();
        self.client.execute(&stmt, &values_refs).await?;
        Ok(())
    }

    pub async fn fetch(
        &self,
        table: &str,
        columns: Vec<&str>,
        values: Vec<Box<dyn ToSql + Sync>>,
    ) -> Result<Vec<Row>> {
        let stmt = self.query_builder.prepare_fetch_statement(table, &columns);
        let values_refs: Vec<&(dyn ToSql + Sync)> =
            values.iter().map(|boxed| boxed.as_ref()).collect();
        Ok(self.client.query(&stmt, &values_refs).await?)
    }

    pub async fn remove(
        &self,
        table: &str,
        columns: Vec<&str>,
        values: &Vec<Box<dyn ToSql + Sync>>,
    ) -> Result<()> {
        let stmt = self.query_builder.prepare_delete_statement(table, &columns);
        let values_refs: Vec<&(dyn ToSql + Sync)> =
            values.iter().map(|boxed| boxed.as_ref()).collect();
        self.client.execute(&stmt, &values_refs).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::DatabaseConfig;
    use chrono::offset::Utc;
    use chrono::DateTime;
    use std::error::Error;
    use tokio_postgres::types::to_sql_checked;
    use tokio_postgres::types::IsNull;

    #[test]
    fn test_sql_insert_statement() {
        let builder = SqlQueryBuilder {};

        let table = "test";
        let columns = vec!["one", "two", "three", "four"];
        let sql = builder.prepare_insert_statement(table, &columns);
        assert_eq!(
            sql,
            "INSERT INTO test (one, two, three, four) VALUES ($1, $2, $3, $4)"
        );
    }

    #[test]
    fn test_sql_update_statement() {
        let builder = SqlQueryBuilder {};

        let table = "test";
        let columns = vec!["one", "two", "three", "four", "local_id"];
        let sql = builder.prepare_update_statement(table, &columns);
        assert_eq!(
            sql,
            "UPDATE test SET one = $1, two = $2, three = $3, four = $4 WHERE local_id = $5"
        );
    }

    #[test]
    fn test_sql_fetch_statement_whole_table() {
        let builder = SqlQueryBuilder {};

        let table = "test";
        let sql = builder.prepare_fetch_statement(table, &Vec::default());
        assert_eq!(sql, "SELECT * FROM test");
    }

    #[test]
    fn test_sql_fetch_statement_with_filter() {
        let builder = SqlQueryBuilder {};

        let table = "test";
        let columns = vec!["one", "two", "three"];
        let sql = builder.prepare_fetch_statement(table, &columns);
        assert_eq!(
            sql,
            "SELECT * FROM test WHERE one = $1 AND two = $2 AND three = $3"
        );
    }

    #[test]
    fn test_sql_delete_statement() {
        let builder = SqlQueryBuilder {};

        let table = "test";
        let sql = builder.prepare_delete_statement(table, &Vec::default());
        assert_eq!(sql, "DELETE FROM test");
    }

    #[test]
    fn test_sql_delete_statement_with_filters() {
        let builder = SqlQueryBuilder {};

        let table = "test";
        let columns = vec!["one", "two", "three"];
        let sql = builder.prepare_delete_statement(table, &columns);
        assert_eq!(
            sql,
            "DELETE FROM test WHERE one = $1 AND two = $2 AND three = $3"
        );
    }

    #[derive(Debug)]
    struct TestTransaction {
        local_id: Uuid,
        symbol: String,
        entry_price: f64,
        exit_price: Option<f64>,
        quantity: i64,
    }

    impl TestTransaction {
        pub fn serialise(&self) -> Vec<Box<dyn ToSql + Sync>> {
            vec![
                to_sql_type(self.symbol.clone()),
                to_sql_type(self.entry_price.clone()),
                to_sql_type(self.exit_price),
                to_sql_type(self.quantity),
                to_sql_type(self.local_id),
            ]
        }
    }

    // tests require local db setup name=test user=test pass=test
    #[tokio::test]
    async fn test_with_local_db() {
        let db_config = DatabaseConfig {
            db_name: "test".to_string(),
            port: 5432,
            host: "0.0.0.0".to_string(),
            user: "test".to_string(),
            password: "test".to_string(),
        };
        let mut settings = Settings {
            database: db_config,
            ..Default::default()
        };

        let db_client = DBClient::new(&settings).await.unwrap();
        let table = "transaction";
        let columns = vec![
            "symbol",
            "entry_price",
            "exit_price",
            "quantity",
            "local_id",
        ];
        let mut transaction = TestTransaction {
            local_id: Uuid::new_v4(),
            symbol: "MSFT".to_string(),
            entry_price: 120.0,
            exit_price: None,
            quantity: 12,
        };

        let _ = match db_client
            .insert(table, columns.clone(), &transaction.serialise())
            .await
        {
            Err(err) => panic!("Failed to insert into db, err={}", err),
            _ => (),
        };

        transaction.exit_price = Some(124.1);
        let _ = match db_client
            .update(table, columns, &transaction.serialise())
            .await
        {
            Err(err) => panic!("Failed to update, err={}", err),
            _ => (),
        };

        let columns = vec!["local_id"];
        let filters = vec![to_sql_type(transaction.local_id)];
        let rows: Vec<Row> = match db_client.fetch(table, columns.clone(), &filters).await {
            Err(err) => panic!("Failed to fetch from db, err={}", err),
            Ok(val) => val,
        };

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get::<&str, f64>("entry_price"),
            transaction.entry_price
        );
        assert_eq!(
            rows[0].get::<&str, f64>("exit_price"),
            transaction.exit_price.unwrap()
        );
        assert_eq!(rows[0].get::<&str, String>("symbol"), transaction.symbol);

        let _ = match db_client.remove(table, columns, &filters).await {
            Err(err) => panic!("Failed to fetch from db, err={}", err),
            _ => (),
        };
    }
}
