use anyhow::{Context, Result};
use bollard::Docker;
use log::info;
use sqlx::PgConnection;

use fluvio::Fluvio;

use super::{db, docker_conn, fluvio_conn, smdk};

pub struct TestContext {
    pub fluvio: Fluvio,
    pub pg_conn: PgConnection,
    pub docker: Docker,
}

impl TestContext {
    pub async fn setup() -> Result<Self> {
        let docker = docker_conn::connect_docker()
            .await
            .context("unable to connect to docker engine")?;

        let start_fluvio = std::env::var("START_FLUVIO")
            .unwrap_or("true".to_owned())
            .parse::<bool>()
            .context("unable to parse START_FLUVIO env")?;

        if start_fluvio {
            fluvio_conn::start_cluster()
                .await
                .context("unable to start fluvio cluster")?;
        }

        smdk::load("json-sql").await?;

        let fluvio = fluvio_conn::connect_fluvio()
            .await
            .context("unable to connect to fluvio cluster")?;

        let pg_conn = db::run_postgres(&docker)
            .await
            .context("unable to run posgres container")?;

        Ok(Self {
            fluvio,
            pg_conn,
            docker,
        })
    }

    pub async fn teardown(&mut self) -> Result<()> {
        info!("tearing down environment");
        db::remove_postgres(&self.docker).await?;
        fluvio_conn::delete_cluster().await?;
        info!("environment teardown finished");
        Ok(())
    }

    pub async fn start_postgres(&mut self) -> Result<()> {
        self.pg_conn = db::start_postgres(&self.docker).await?;
        Ok(())
    }

    pub async fn stop_postgres(&mut self) -> Result<()> {
        db::stop_postgres(&self.docker).await?;
        Ok(())
    }
}
