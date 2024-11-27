mod all_data_types;
mod consumer_offsets;
mod json_sql_transformations;
mod utils;

use anyhow::{Context, Result};
use log::{error, info};

#[async_std::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("preparing environment");
    let mut ctx = utils::ctx::TestContext::setup().await?;

    info!("running sql-connector integration tests");

    if let Err(e) = run_tests(&mut ctx).await {
        error!("test failed: {:#?}", e);
        ctx.teardown().await?;
        return Err(e);
    }

    ctx.teardown().await?;
    info!("ALL PASSED");
    Ok(())
}

async fn run_tests(ctx: &mut utils::ctx::TestContext) -> Result<()> {
    all_data_types::test_postgres_all_data_types(ctx)
        .await
        .context("test_postgres_all_data_types failed")?;

    json_sql_transformations::test_postgres_with_json_sql_transformations(ctx)
        .await
        .context("test_postgres_with_json_sql_transformations failed")?;

    consumer_offsets::test_postgres_consumer_offsets(ctx)
        .await
        .context("test_postgres_consumer_offsets failed")?;

    Ok(())
}
