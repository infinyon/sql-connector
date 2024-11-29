use std::{path::Path, process::Command, time::Duration};

use anyhow::Result;
use async_std::task;
use log::info;
use once_cell::sync::Lazy;
use serde::Deserialize;

use crate::utils::{cdk, connector_dir};

static CDK_BIN: Lazy<String> = Lazy::new(|| std::env::var("CDK_BIN").unwrap_or("cdk".to_string()));

pub(crate) async fn cdk_deploy_start(
    config_path: &Path,
    env: Option<(&str, &str)>,
) -> Result<TestConfig> {
    let connector_dir = connector_dir()?;
    info!(
        "deploying connector with config from {config_path:?}, connector_dir: {}",
        connector_dir.to_string_lossy()
    );
    let mut command = Command::new(CDK_BIN.to_string());
    command.current_dir(&connector_dir);
    command
        .arg("deploy")
        .arg("start")
        .arg("--config")
        .arg(config_path);
    if let Some((env_name, env_value)) = env {
        command.env(env_name, env_value);
    }
    let output = command.output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`cdk deploy start` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    task::sleep(Duration::from_secs(10)).await; // time for connector to start
    let config: TestConfig =
        serde_yaml::from_reader(std::fs::File::open(config_path).unwrap()).unwrap();
    let connector_name = &config.meta.name;
    let connector_status = cdk::cdk_deploy_status(connector_name).unwrap();
    info!("connector: {connector_name}, status: {connector_status:?}");
    Ok(config)
}

pub(crate) fn cdk_deploy_shutdown(connector_name: &str) -> Result<()> {
    info!("shutting down connector {connector_name}");
    let output = Command::new(CDK_BIN.to_string())
        .arg("deploy")
        .arg("shutdown")
        .arg("--name")
        .arg(connector_name)
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`cdk deploy shutdown` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    Ok(())
}

pub(crate) fn cdk_deploy_status(connector_name: &str) -> Result<Option<String>> {
    let output = Command::new(CDK_BIN.to_string())
        .arg("deploy")
        .arg("list")
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`cdk deploy list` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    for line in String::from_utf8_lossy(output.stdout.as_slice())
        .lines()
        .skip(1)
    {
        let mut column_iter = line.split_whitespace();
        match column_iter.next() {
            Some(name) if name.eq(connector_name) => {
                return Ok(column_iter.next().map(|s| s.to_owned()))
            }
            _ => {}
        }
    }
    Ok(None)
}

#[derive(Debug, Deserialize)]
pub(crate) struct MetaConfig {
    pub(crate) name: String,
    pub(crate) topic: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TestConfig {
    pub(crate) meta: MetaConfig,
}
