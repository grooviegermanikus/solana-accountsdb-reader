use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use clap::Parser;
use solana_accounts_db::accounts_db::{ACCOUNTS_DB_CONFIG_FOR_TESTING, AccountShrinkThreshold};
use solana_accounts_db::accounts_index::{AccountSecondaryIndexes, ScanConfig};
use solana_runtime::genesis_utils::create_genesis_config;
use solana_runtime::runtime_config::RuntimeConfig;
use solana_runtime::snapshot_archive_info::FullSnapshotArchiveInfo;
use solana_runtime::snapshot_bank_utils::bank_from_snapshot_archives;
use solana_sdk::native_token::sol_to_lamports;
use solana_sdk::pubkey::Pubkey;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );


    let Args { snapshot_archive_path } = Args::parse();

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();


    let genesis_config = create_genesis_config(sol_to_lamports(1_000_000.)).genesis_config;
    let accounts_dir = PathBuf::from_str("accounts").unwrap();
    // must esist
    let bank_snapshots_dir = PathBuf::from_str("snapshots-unpack").unwrap();

    let full_snapshot_archive_info = FullSnapshotArchiveInfo::new_from_path(archive_path).unwrap();
    let (bank_from_snapshot, _) = bank_from_snapshot_archives(
        &[accounts_dir],
        &bank_snapshots_dir,
        &full_snapshot_archive_info,
        None,
        &genesis_config,
        &RuntimeConfig::default(),
        None,
        None,
        AccountSecondaryIndexes::default(),
        None,
        AccountShrinkThreshold::default(),
        false,
        false,
        false,
        false,
        Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
        None,
        Arc::default(),
    )
        .unwrap();

    let program_key = Pubkey::from_str("89A6cnoZMhsxKQzgJvQZS8UsTnojef5YW4z23Do1GuXv").unwrap();
    bank_from_snapshot.get_program_accounts(&program_key, &ScanConfig::default()).expect("should find program");


    let accounts_db = solana_accounts_db::accounts_db::AccountsDb::new_single_for_tests_with_caching();


    Ok(())
}