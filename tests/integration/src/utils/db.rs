use std::collections::HashMap;

use anyhow::Result;
use bollard::{
    container::{Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions},
    image::CreateImageOptions,
    service::{HostConfig, PortBinding},
    Docker,
};
use futures_util::stream::TryStreamExt;
use log::{debug, info};
use sqlx::{Connection, PgConnection};

use fluvio_future::retry::{retry, ExponentialBackoff};

const POSTGRES_IMAGE: &str = "postgres:15.2";
const POSTGRES_HOST_PORT: &str = "5432";
const POSTGRES_PASSWORD: &str = "passpass";
const POSTGRES_USER: &str = "pguser";
const POSTGRES_DB: &str = POSTGRES_USER;

pub(crate) async fn run_postgres(docker: &Docker) -> Result<PgConnection> {
    info!("starting postgres container");

    let config: Config<String> = Config {
        image: Some(POSTGRES_IMAGE.to_owned()),
        exposed_ports: Some(HashMap::from([(
            POSTGRES_HOST_PORT.to_owned(),
            Default::default(),
        )])),
        host_config: Some(HostConfig {
            port_bindings: Some(HashMap::from([(
                POSTGRES_HOST_PORT.to_owned(),
                Some(vec![PortBinding {
                    host_ip: Some("0.0.0.0".to_owned()),
                    host_port: Some(POSTGRES_HOST_PORT.to_owned()),
                }]),
            )])),
            ..Default::default()
        }),
        env: Some(vec![
            format!("POSTGRES_PASSWORD={POSTGRES_PASSWORD}"),
            format!("POSTGRES_USER={POSTGRES_USER}"),
        ]),
        ..Default::default()
    };
    let _ = &docker
        .create_image(
            Some(CreateImageOptions {
                from_image: POSTGRES_IMAGE,
                ..Default::default()
            }),
            None,
            None,
        )
        .try_collect::<Vec<_>>()
        .await?;

    let _ = &docker
        .create_container(
            Some(CreateContainerOptions {
                name: "postgres",
                platform: None,
            }),
            config,
        )
        .await?;

    let _ = &docker
        .start_container("postgres", None::<StartContainerOptions<String>>)
        .await?;

    info!("postgres container created, waiting for readiness");
    let conn = retry(ExponentialBackoff::from_millis(10).take(6), || {
        connect_postgres()
    })
    .await?;

    info!("postgres container started with {POSTGRES_IMAGE} image");

    Ok(conn)
}

pub(crate) async fn connect_postgres() -> Result<PgConnection> {
    let connection_str = postgres_connection_str();
    debug!("connecting to {connection_str}");
    let mut conn = PgConnection::connect(&connection_str).await?;

    sqlx::query("SELECT count(*) FROM pg_catalog.pg_tables")
        .fetch_one(&mut conn)
        .await?;
    Ok(conn)
}

fn postgres_connection_str() -> String {
    format!(
        "postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{POSTGRES_HOST_PORT}/{POSTGRES_DB}"
    )
}

pub(crate) async fn remove_postgres(docker: &Docker) -> Result<()> {
    let _ = &docker
        .remove_container(
            "postgres",
            Some(RemoveContainerOptions {
                v: true,
                force: true,
                ..Default::default()
            }),
        )
        .await?;
    info!("postgres container removed");
    Ok(())
}
