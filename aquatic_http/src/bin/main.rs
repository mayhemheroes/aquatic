use anyhow::Context;
use aquatic_cli_helpers::run_app_with_cli_and_config;
use simplelog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};

use aquatic_http::config::{Config, LogLevel};


#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;


fn main(){
    run_app_with_cli_and_config::<Config>(
        "aquatic: BitTorrent (HTTP/TLS) tracker",
        run
    )
}


// almost identical to ws version
fn run(config: Config) -> anyhow::Result<()> {
    let level_filter = match config.log_level {
        LogLevel::Off => LevelFilter::Off,
        LogLevel::Error => LevelFilter::Error,
        LogLevel::Warn => LevelFilter::Warn,
        LogLevel::Info => LevelFilter::Info,
        LogLevel::Debug => LevelFilter::Debug,
        LogLevel::Trace => LevelFilter::Trace,
    };

    // Note: logger doesn't seem to pick up thread names. Not a huge loss.
    let simplelog_config = ConfigBuilder::new()
        .set_time_to_local(true)
        .set_location_level(LevelFilter::Off)
        .build();

    TermLogger::init(
        level_filter,
        simplelog_config,
        TerminalMode::Stderr
    ).context("Couldn't initialize logger")?;

    aquatic_http::run(config)
}