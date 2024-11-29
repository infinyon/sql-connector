use std::{process::Command, time::Duration};

use anyhow::Result;
use async_std::task;
use log::info;
use once_cell::sync::Lazy;

use crate::utils::sm_dir;

static SMDK_BIN: Lazy<String> =
    Lazy::new(|| std::env::var("SMDK_BIN").unwrap_or("smdk".to_string()));

pub(crate) async fn load(smdk_name: &str) -> Result<()> {
    let connector_dir = sm_dir()?;
    info!("loading smart module {smdk_name}");
    let mut command = Command::new(SMDK_BIN.to_string());
    command.current_dir(&connector_dir);
    command.arg("load").arg("-p").arg(smdk_name);
    let output = command.output()?;
    if !output.status.success() {
        anyhow::bail!(
            "`smdk load` failed with:\n {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        )
    }
    task::sleep(Duration::from_secs(10)).await;
    info!("smart module loaded");
    Ok(())
}
