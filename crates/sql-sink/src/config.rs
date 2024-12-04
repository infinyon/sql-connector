use std::time::Duration;

use fluvio_connector_common::{connector, secret::SecretString};

#[derive(Debug, Clone)]
#[connector(config, name = "sql")]
#[serde(rename_all = "kebab-case")]
pub(crate) struct SqlConfig {
    /// The SQL connection string
    pub url: SecretString,

    /// Maximum backoff duration to reconnect to the database
    #[serde(with = "humantime_serde", default = "default_backoff_max")]
    pub backoff_max: Duration,

    /// Minimum backoff duration to reconnect to the database
    #[serde(with = "humantime_serde", default = "default_backoff_min")]
    pub backoff_min: Duration,
}

#[inline]
fn default_backoff_max() -> Duration {
    Duration::from_secs(60)
}

#[inline]
fn default_backoff_min() -> Duration {
    Duration::from_secs(1)
}
