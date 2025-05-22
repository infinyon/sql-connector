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

    // The size of batches sent to the database at once
    #[serde(default)]
    pub batch_size: Option<usize>,

    // Timeout to send batches, no consequences if batch-size is None
    #[serde(with = "humantime_serde", default = "default_batch_interval")]
    pub batch_interval: Duration,
}

#[inline]
fn default_backoff_max() -> Duration {
    Duration::from_secs(60)
}

#[inline]
fn default_backoff_min() -> Duration {
    Duration::from_secs(1)
}

#[inline]
fn default_batch_interval() -> Duration {
    Duration::from_secs(1)
}
