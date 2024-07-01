use std::str::FromStr;
use solana_sdk::pubkey::{Pubkey, PUBKEY_BYTES};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );


    let db = sled::open("sled.db").unwrap();

    let sled_store = db.open_tree("accounts").unwrap();
    println!("len: {}", sled_store.len());


    let somekey = Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap();
    let mango_accounts = sled_store.scan_prefix(somekey.as_ref()).next().unwrap();

    for (key_pair, _value) in mango_accounts {
        let account_key = Pubkey::new_from_array(key_pair[PUBKEY_BYTES..].try_into().unwrap());
        println!("key: {}", account_key.to_string());

    }


    Ok(())
}
