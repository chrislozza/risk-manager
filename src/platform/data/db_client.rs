use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::Pool;
use sqlx::Postgres;
use std::env;
use std::sync::Arc;
use uuid::Uuid;

use super::Settings;

#[derive(Debug)]
pub struct SqlQueryBuilder;

impl SqlQueryBuilder {
    pub fn prepare_insert_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        let sql = format!("INSERT INTO {} ({})", table, columns.join(", "));
        let placeholders: String = (1..=columns.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<String>>()
            .join(", ");

        format!("{} VALUES ({})", sql, placeholders)
    }

    pub fn prepare_update_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        let sql = format!("UPDATE {} SET", table);

        let placeholders: String = (1..=columns.len() - 1)
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
        if columns.is_empty() {
            return format!("SELECT * FROM {}", table);
        }

        let sql = format!("SELECT * FROM {}", table);
        let placeholders: String = (1..=columns.len())
            .map(|i| format!("{} = ${}", columns[i - 1], i))
            .collect::<Vec<String>>()
            .join(" AND ");

        let sql = format!("{} WHERE {}", sql, placeholders);
        sql
    }

    #[cfg(test)]
    pub fn prepare_delete_statement(&self, table: &str, columns: &Vec<&str>) -> String {
        if columns.is_empty() {
            return format!("DELETE FROM {}", table);
        }

        let sql = format!("DELETE FROM {}", table);
        let placeholders: String = (1..=columns.len())
            .map(|i| format!("{} = ${}", columns[i - 1], i))
            .collect::<Vec<String>>()
            .join(" AND ");

        format!("{} WHERE {}", sql, placeholders)
    }
}

#[derive(Debug)]
pub struct DBClient {
    pub pool: Pool<Postgres>,
    pub query_builder: SqlQueryBuilder,
}

impl DBClient {
    pub async fn new(settings: &Settings) -> Result<Arc<Self>> {
        let db_cfg = &settings.database;
        let dbpass = match &db_cfg.password {
            Some(pass) => pass.clone(),
            None => {
                env::var("DB_PASSWORD").expect("Failed to read the 'dbpass' environment variable.")
            }
        };
        let database_url = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode=disable",
            db_cfg.user, dbpass, db_cfg.host, db_cfg.port, db_cfg.name
        );
        let pool = match PgPoolOptions::new()
            .min_connections(2)
            .max_connections(5)
            .test_before_acquire(false)
            .connect(&database_url)
            .await
        {
            std::result::Result::Ok(pool) => pool,
            std::result::Result::Err(err) => {
                bail!(
                    "Failed to startup db connection pool with url: {} error={}",
                    database_url,
                    err
                );
            }
        };

        Ok(Arc::new(DBClient {
            pool,
            query_builder: SqlQueryBuilder {},
        }))
    }

    pub fn get_sql_stmt(
        &self,
        table_name: &str,
        local_id: &Uuid,
        columns: Vec<&str>,
        db: &Arc<DBClient>,
    ) -> String {
        if Uuid::is_nil(local_id) {
            db.query_builder
                .prepare_insert_statement(table_name, &columns)
        } else {
            db.query_builder
                .prepare_update_statement(table_name, &columns)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::DatabaseConfig;
    use anyhow::bail;
    use uuid::Uuid;

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
    #[derive(Debug, sqlx::FromRow)]
    struct TestTransaction {
        local_id: Option<Uuid>,
        symbol: String,
        entry_price: f64,
        exit_price: Option<f64>,
        quantity: i64,
    }

    impl TestTransaction {
        async fn persist_to_db(&mut self, db: Arc<DBClient>) -> Result<()> {
            let columns = vec![
                "symbol",
                "entry_price",
                "exit_price",
                "quantity",
                "local_id",
            ];
            let stmt = match self.local_id {
                Some(_) => db
                    .query_builder
                    .prepare_update_statement("transaction", &columns),
                None => {
                    self.local_id = Some(Uuid::new_v4());
                    db.query_builder
                        .prepare_insert_statement("transaction", &columns)
                }
            };

            if let Err(err) = sqlx::query(&stmt)
                .bind(self.symbol.clone())
                .bind(self.entry_price)
                .bind(self.exit_price)
                .bind(self.quantity)
                .bind(self.local_id)
                .fetch_one(&db.pool)
                .await
            {
                bail!("Failed to publish to db, error={}", err)
            }
            Ok(())
        }

        async fn fetch(&self, db: Arc<DBClient>) -> Result<TestTransaction> {
            let columns = vec!["local_id"];
            let stmt = db
                .query_builder
                .prepare_fetch_statement("transaction", &columns);

            let transaction: TestTransaction = match sqlx::query_as::<_, TestTransaction>(&stmt)
                .bind(self.local_id.unwrap())
                .fetch_one(&db.pool)
                .await
            {
                Err(err) => bail!("Failed to pull data from db, error={}", err),
                core::result::Result::Ok(val) => val,
            };
            Ok(transaction)
        }
    }

    // tests require local db setup name=test user=test pass=test
    #[tokio::test]
    async fn test_with_local_db() {
        let db_config = DatabaseConfig {
            name: "test".to_string(),
            port: 5432,
            host: "0.0.0.0".to_string(),
            user: "test".to_string(),
            password: Some("test".to_string()),
        };
        let settings = Settings {
            database: db_config,
            ..Default::default()
        };

        let db_client = DBClient::new(&settings).await.unwrap();
        let mut transaction = TestTransaction {
            local_id: None,
            symbol: "MSFT".to_string(),
            entry_price: 120.0,
            exit_price: None,
            quantity: 12,
        };

        if let Err(err) = transaction.persist_to_db(db_client.clone()).await {
            panic!("Failed to insert into db, err={}", err)
        };

        transaction.exit_price = Some(124.1);
        if let Err(err) = transaction.persist_to_db(db_client.clone()).await {
            panic!("Failed to update, err={}", err)
        };

        let fetched_transaction = match transaction.fetch(db_client.clone()).await {
            anyhow::Result::Ok(val) => val,
            Err(err) => panic!("Failed to fetch from db, err={}", err),
        };

        assert_eq!(fetched_transaction.entry_price, transaction.entry_price);
        assert_eq!(
            fetched_transaction.exit_price.unwrap(),
            transaction.exit_price.unwrap()
        );
        assert_eq!(fetched_transaction.symbol, transaction.symbol);

        let _ = sqlx::query("delete from transaction")
            .fetch_one(&db_client.pool)
            .await;
    }
}
