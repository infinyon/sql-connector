mod all_data_types;
mod consumer_offsets;
mod json_sql_transformations;
mod utils;

use anyhow::Result;
use log::info;

#[async_std::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("preparing environment");
    let mut ctx = utils::ctx::TestContext::setup().await?;

    info!("running sql-connector integration tests");
    run_tests(&mut ctx).await;

    ctx.teardown().await?;
    info!("ALL PASSED");
    Ok(())
}

async fn run_tests(ctx: &mut utils::ctx::TestContext) {
    all_data_types::test_postgres_all_data_types(ctx).await;
    json_sql_transformations::test_postgres_with_json_sql_transformations(ctx).await;
    consumer_offsets::test_postgres_consumer_offsets(ctx).await;
}
