use fluvio_connector_common::{connector, secret::SecretString};

#[derive(Debug, Clone)]
#[connector(config, name = "sql")]
pub(crate) struct SqlConfig {
    pub url: SecretString,
}
