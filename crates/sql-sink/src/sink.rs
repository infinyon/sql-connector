use anyhow::{Context, Result};
use async_trait::async_trait;
use url::Url;

use fluvio::Offset;
use fluvio_connector_common::{tracing::info, LocalBoxSink, Sink};
use fluvio_model_sql::Operation;

use crate::{config::SqlConfig, db::Db};

#[derive(Debug)]
pub(crate) struct SqlSink {
    url: Url,
}

impl SqlSink {
    pub(crate) fn new(config: &SqlConfig) -> Result<Self> {
        let url = Url::parse(&config.url.resolve()?).context("unable to parse sql url")?;

        Ok(Self { url })
    }
}

#[async_trait]
impl Sink<Operation> for SqlSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<Operation>> {
        let db = Db::connect(self.url.as_str()).await?;
        info!("connected to database {}", db.kind());
        let unfold = futures::sink::unfold(db, |mut db: Db, record: Operation| async move {
            db.execute(record).await?;
            Ok::<_, anyhow::Error>(db)
        });
        Ok(Box::pin(unfold))
    }
}
