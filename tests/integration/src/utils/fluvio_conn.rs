use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use fluvio_future::timer::sleep;
use log::info;
use once_cell::sync::Lazy;

use fluvio::{metadata::topic::TopicSpec, Fluvio};

static FLUVIO_BIN: Lazy<String> =
    Lazy::new(|| std::env::var("FLUVIO_BIN").unwrap_or("fluvio".to_string()));

pub(crate) async fn connect_fluvio() -> Result<Fluvio> {
    info!("checking fluvio cluster availability");
    let fluvio = fluvio::Fluvio::connect().await?;
    info!("connected to fluvio version: {}", fluvio.platform_version());
    Ok(fluvio)
}

pub(crate) async fn remove_topic(fluvio: &Fluvio, topic: &str) -> Result<()> {
    info!("removing topic: {}", topic);
    fluvio.admin().await.delete::<TopicSpec>(topic).await?;
    info!("topic removed: {}", topic);
    Ok(())
}

pub(crate) async fn start_cluster() -> Result<()> {
    info!("starting fluvio cluster");
    let output = Command::new(FLUVIO_BIN.to_string())
        .arg("cluster")
        .arg("start")
        .arg("--local")
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`fluvio cluster start` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    sleep(Duration::from_secs(5)).await;
    info!("fluvio cluster started");
    Ok(())
}

pub(crate) async fn delete_cluster() -> Result<()> {
    info!("deleting fluvio cluster");

    let output = Command::new(FLUVIO_BIN.to_string())
        .arg("cluster")
        .arg("delete")
        .arg("--force")
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`fluvio cluster delete` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    info!("fluvio cluster deleted");
    Ok(())
}
