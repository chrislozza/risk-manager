use super::settings::Settings;
use log::info;
use std::collections::HashMap;
use std::time::SystemTime;
use tokio_postgres::{Client, Error, NoTls, Statement};
use uuid::Uuid;

use anyhow::Result;

pub(crate) struct PostgresConnector {
    client: Client,
}

impl PostgresConnector {
    pub async fn new(database_url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });

        Ok(Self { client })
    }

    async fn prepare_insert_statement(
        &self,
        table: &str,
        values: &HashMap<&str, &str>,
    ) -> Result<Statement, Error> {
        let mut query = format!("INSERT INTO {} ", table);

        let columns: Vec<String> = Vec::from_iter(values.keys().map(|s| String::from(*s)));
        let column_names = columns.join(", ");
        let mut placeholders: String = (1..=values.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<String>>()
            .join(", ");

        placeholders.pop(); //remove trailing ','
        placeholders.pop(); //remove trailing ' '

        query.push_str(&format!("({}) VALUES ({})", column_names, placeholders));

        info!("Prepared insert statement {query}");
        self.client.prepare(&query).await
    }

    async fn prepare_update_statement(
        &self,
        _table: &str,
        values: &HashMap<&str, &str>,
    ) -> Result<Statement, Error> {
        let mut query = "UPDATE your_table SET ".to_string();

        let columns: Vec<String> = Vec::from_iter(values.keys().map(|s| String::from(*s)));
        let mut placeholders: String = (1..=columns.len())
            .map(|i| format!("{} = ${}", columns[i], i))
            .collect::<Vec<String>>()
            .join(", ");

        placeholders.pop(); //remove trailing ','
        placeholders.pop(); //remove trailing ' '

        query.push_str(" WHERE local_id = $");

        info!("Prepared update statement {query}");
        self.client.prepare(&query).await
    }

    pub async fn insert(&self, table: &str, values: HashMap<&str, &str>) -> Result<()> {
        let _id = Uuid::new_v4();
        let _timestamp = SystemTime::now();
        let stmt = self.prepare_insert_statement(table, &values).await?;
        let data = Vec::from_iter(values.values().map(|s| String::from(*s)));
        self.client.execute_raw(&stmt, &data[..]).await?;
        Ok(())
    }

    pub async fn update(&self, table: &str, values: HashMap<&str, &str>) -> Result<()> {
        let stmt = self.prepare_update_statement(table, &values).await?;
        let data = Vec::from_iter(values.values().map(|s| String::from(*s)));
        self.client.execute_raw(&stmt, &data[..]).await?;
        Ok(())
    }

    pub async fn remove(&self, _table: &str, id: &str) -> Result<()> {
        let stmt = self
            .client
            .prepare("DELETE FROM $table WHERE id = $id")
            .await?;
        self.client.execute(&stmt, &[&id]).await?;
        Ok(())
    }

    pub async fn fetch_single(
        &self,
        _table: &str,
        _filters: &HashMap<String, String>,
        _local_id: Option<i32>,
    ) -> Option<Vec<(&str, &str)>> {
        //        let mut stmt = self.client.prepare("SELECT * FROM $table WHERE id = $id").await?;
        //        let rows = self.client.query(table, &[("id", &id)]).await?;
        //        let row = rows.get(0);
        //        if row.is_none() {
        //            return None;
        //        }
        //        let data = row.unwrap();
        //        Some(data.iter().map(|(k, v)| (k, v)).collect())
        None
    }

    pub async fn fetch_multi(
        &self,
        _table: &str,
        _filters: &HashMap<String, String>,
        _local_id: Option<i32>,
    ) -> Vec<HashMap<&str, &str>> {
        //        let mut stmt = self.client.prepare("SELECT * FROM $table WHERE $($cols = $values),*)").await?;
        //        let rows = self.client.query(table, conditions).await?;
        //        let mut data = Vec::new();
        //        for row in rows {
        //            let data = row.iter().map(|(k, v)| (k, v)).collect();
        //            data.push(data);
        //        }
        //        return data
        Vec::new()
    }
}

struct DBClient {
    connector: PostgresConnector,
}

impl DBClient {
    pub async fn new(settings: &Settings) -> Result<Self> {
        if let Some(db_cfg) = &settings.database {
            let password = &db_cfg.secret_id;
            let database_url = format!(
                "postgresql://postgres:{}@{}:{}/{}?sslmode=disable",
                password, db_cfg.host, db_cfg.port, db_cfg.name
            );
            Ok(DBClient {
                connector: PostgresConnector::new(database_url.as_str()).await?,
            })
        } else {
            panic!("No database settings found, exiting early")
        }
    }
}
