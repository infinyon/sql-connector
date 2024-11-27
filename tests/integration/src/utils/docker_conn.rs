use anyhow::Result;
use bollard::Docker;
use log::info;

pub async fn connect_docker() -> Result<Docker> {
    info!("checking docker engine availability");

    let docker = Docker::connect_with_local_defaults()?;
    let version = docker.version().await?;
    info!(
        "connected to docker version: {:?}, api_version: {:?}",
        &version.version, &version.api_version
    );
    Ok(docker)
}
