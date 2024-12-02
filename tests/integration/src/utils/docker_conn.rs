use anyhow::Result;
use bollard::Docker;
use log::info;

pub(crate) async fn connect_docker() -> Result<Docker> {
    info!("checking docker engine availability");

    let docker = Docker::connect_with_local_defaults()?;
    let version = docker.version().await?;
    info!(
        "connected to docker version: {:?}, api_version: {:?}",
        &version.version, &version.api_version
    );
    Ok(docker)
}

pub(crate) async fn stop_container(docker: &Docker, container_name: &str) -> Result<()> {
    info!("stopping container: {}", container_name);
    docker.stop_container(container_name, None).await?;
    info!("container stopped: {}", container_name);
    Ok(())
}
