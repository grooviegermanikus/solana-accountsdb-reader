use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use clap::Parser;
use log::info;
use solana_accounts_db::accounts_db::{ACCOUNTS_DB_CONFIG_FOR_TESTING, AccountsDb, AccountShrinkThreshold};
use solana_accounts_db::accounts_index::{AccountSecondaryIndexes, ScanConfig};
use solana_runtime::genesis_utils::create_genesis_config;
use solana_runtime::runtime_config::RuntimeConfig;
use solana_runtime::snapshot_archive_info::FullSnapshotArchiveInfo;
use solana_runtime::snapshot_bank_utils::bank_from_snapshot_archives;
use solana_sdk::genesis_config::{ClusterType, GenesisConfig};
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


    let ledger_path = Path::new("/Users/stefan/mango/projects/geyser-misc/test-ledger");
    let genesis_config = GenesisConfig::load(ledger_path).unwrap();
    // let
    //     genesis_config = create_genesis_config(sol_to_lamports(1_000_000.)).genesis_config;
    let accounts_dir = PathBuf::from_str("/Users/stefan/mango/projects/accountsdb-how-it-works/accountsdb-mini").unwrap();
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
    // let accounts = bank_from_snapshot.get_program_accounts(&program_key, &ScanConfig::default()).expect("should find program");
    let accccc = bank_from_snapshot.accounts();
    let accounts_db = accccc.accounts_db.clone();
    let all_tokens = bank_from_snapshot.get_program_accounts(&Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(), &ScanConfig::default()).unwrap();
    for acc in all_tokens {
        println!("{:?}", acc);

    }

    tokio::spawn(async move {
        loop {
            info!("base working path: {:?}", accounts_db.get_base_working_path());
            accounts_db.get_base_working_path();
            sleep(std::time::Duration::from_millis(500));
        }
    });


    // let accountsdb_path = PathBuf::from_str("accounts").unwrap();

    // let accounts_db =  AccountsDb::new_with_config(
    //     vec![accountsdb_path],
    //     &ClusterType::Development,
    //     AccountSecondaryIndexes::default(),
    //     AccountShrinkThreshold::default(),
    //     Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
    //     None,
    //     Arc::default(),
    // );

    sleep(std::time::Duration::from_secs(20));

    Ok(())
}