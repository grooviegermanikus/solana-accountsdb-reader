use std::str::FromStr;
use const_env::from_env;
use sled::{Db, Tree};
use solana_sdk::pubkey::{Pubkey, PUBKEY_BYTES};

#[from_env]
const LEAF_FANOUT: usize = 32;

struct SpaceJamMap {
    db: sled::Db<LEAF_FANOUT>,
    store: Tree<LEAF_FANOUT>,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let config = sled::Config::default()
        .path("sled.db")
        .cache_capacity_bytes(512 * 1024 * 1024)
        .flush_every_ms(Some(1000))
        ;
    let db = config.open().unwrap();

    let sled_store: Tree<32> = db.open_tree("accounts").unwrap();
    println!("len: {}", sled_store.len());


    let somekey = Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap();
    let mango_accounts = sled_store.scan_prefix(somekey.as_ref());

    for (key_pair, _value) in mango_accounts.map(|x| x.unwrap()) {
        let account_key = Pubkey::new_from_array(key_pair[PUBKEY_BYTES..].try_into().unwrap());
        println!("key: {}", account_key.to_string());

    }

    sled_store.flush().unwrap();

    db.flush().unwrap();


    Ok(())
}
