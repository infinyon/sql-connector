use fluvio_connector_common::connector;
use url::Url;

#[derive(Debug)]
#[connector(config, name = "sql")]
pub(crate) struct SqlConfig {
    pub url: Url,
}
