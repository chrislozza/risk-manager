use tokio_postgres::{Client, NoTls, Error, Statement};
use uuid::Uuid;
use std::time::{SystemTime, Duration};
use std::collections::HashMap;
use log::{info, error};
use super::settings::Settings;

pub(crate) struct PostgresConnector {
    client: Client,
}

impl PostgresConnector {
    pub fn new(database_url: &str) -> Result<Self, Error> {

        let (client, connection) =
            tokio_postgres::connect(database_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        Ok(Self { client })
    }

    async fn prepare_insert_statement(&self, table: &str, values: HashMap<String, String>) -> Result<Statement, Error> {
        let mut query = format!("INSERT INTO {} ", table);

        let columns: Vec<String> = values.keys().collect();
        let column_names = columns.join(", ");
        let placeholders: String = (1..=values.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<String>>()
            .join(", ");

        placeholders.pop(); //remove trailing ','
        placeholders.pop(); //remove trailing ' '

        query.push_str(&format!("({}) VALUES ({})", column_names, placeholders));

        info!("Prepared insert statement {query}");
        client.prepare(&query).await
    }

    async fn prepare_update_statement(&self, table: &str, values: HashMap<String, String>, local_id: &str) -> Result<Statement, Error> {
        let mut query = format!("UPDATE your_table SET ");

        let columns: Vec<String> = values.keys().collect();
        let placeholders: String = (1..=columns.len())
            .map(|i| format!("{} = ${}", columns[i], i))
            .collect::<Vec<String>>()
            .join(", ");

        placeholders.pop(); //remove trailing ','
        placeholders.pop(); //remove trailing ' '

        query.push_str(" WHERE local_id = $");

        info!("Prepared update statement {query}");
        client.prepare(&query).await
    }

    async fn extract_values(&self, statement: &Statement, values: &HashMap<String, String>, local_id: Option<i32>) -> Result<u64, Error> {
        let mut params = Vec::new();
        for i in 1..=values.len() {
            let value = values[i]; // Default to an empty string if the value is not provided
            params.push(value);
        }
        if let Some(uuid) = local_id {
            params.push(uuid);
        }

        let result = client.execute(statement, &params[..]).await?;
        Ok(result)
    }


    pub async fn insert(&self, table: &str, values: &HashMap<String, Option<String>>) -> Result<(), Error> {
        let id = Uuid::new_v4();
        let timestamp = SystemTime::now();
        let mut stmt = self.prepare_insert_statement(table, values).await?;
        let mut data = self.extract_values(&stmt, &values, None).await?;
        self.client.execute_raw(&stmt, &data[..]).await?;
        Ok(())
    }

    pub async fn update(&self, table: &str, data: &[(&str, &str)], local_id: i32) -> Result<(), Error> {
        let mut stmt = self.prepare_update_statement(table, values, local_id).await?;
        let mut data = self.extract_values(&stmt, &values, None).await?;
        self.client.execute_raw(&stmt, &data[..]).await?;
        Ok(())
    }

    pub async fn remove(&self, table: &str, id: i32) -> Result<(), Error> {
        let mut stmt = self.client.prepare("DELETE FROM $table WHERE id = $id").await?;
        stmt.execute(table, &[("id", &id)]).await?;
        Ok(())
    }

    pub async fn fetch_single(&self, table: &str, id: i32) -> Result<Option<Vec<(&str, &str)>>, Error> {
        let mut stmt = self.client.prepare("SELECT * FROM $table WHERE id = $id").await?;
        let rows = stmt.query(table, &[("id", &id)]).await?;
        let row = rows.get(0);
        if row.is_none() {
            return Ok(None);
        }
        let data = row.unwrap();
        Ok(Some(data.iter().map(|(k, v)| (k, v)).collect()))
    }

    pub async fn fetch_multi(&self, table: &str, conditions: &[(&str, &str)]) -> Result<Vec<Vec<(&str, &str)>>, Error> {
        let mut stmt = self.client.prepare("SELECT * FROM $table WHERE $($cols = $values),*)").await?;
        let rows = stmt.query(table, conditions).await?;
        let mut data = Vec::new();
        for row in rows {
            let data = row.iter().map(|(k, v)| (k, v)).collect();
            data.push(data);
        }
        Ok(data)
    }
}

struct DBClient {
    connector: PostgresConnector
}

impl DBClient {
    pub fn new(settings: &Settings) -> Self {
        if let db_cfg = settings.database {
            let database_url = format!("postgresql://postgres:{}@{}:{}/{}?sslmode=disable", db_cfg.password, db_cfg.host, db_cfg.port, db_cfg.name);
            DBClient {
                connector: PostgresConnector::new(database_url.as_str()),
            }
        } else {
            panic!("No database settings found, exiting early")
        }
    }
}
