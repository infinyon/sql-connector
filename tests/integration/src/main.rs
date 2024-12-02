mod all_data_types;
mod consumer_offsets;
mod json_sql_transformations;
mod reliable_db_conn;
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
    all_data_types::test(ctx).await;
    json_sql_transformations::test(ctx).await;
    consumer_offsets::test(ctx).await;
    reliable_db_conn::test(ctx).await;
}
