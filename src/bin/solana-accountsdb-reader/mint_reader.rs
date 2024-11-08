use log::{info, trace, warn};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::program_pack::Pack;
use std::collections::HashSet;
use solana_sdk::pubkey::Pubkey;

pub enum TokenMint {
    Spl,
    SplEmpty,
    Token2022,
}


pub fn is_spltoken_mint(data: &[u8], acc_pubkey: Pubkey) -> Option<u64> {
    let unpacked = spl_token::state::Mint::unpack(&data);
    let is_len_matching = data.len() == spl_token::state::Mint::LEN;

    if unpacked.is_ok() != is_len_matching {
        // e.g. token accounts len 82, acc J85gx9rg9QjqXL2WzwuRJAiJTbfJvSkx18qDaUe7Si7F
        warn!(
            "mismatching criteria, token account len {:?}, acc {} - discard",
            data.len(),
            acc_pubkey
        );
    }

    if is_len_matching {
        if let Ok(mint) = unpacked {
            trace!("mint(spl) found {}", acc_pubkey);
            return Some(mint.supply);
        }
    }

    return None;
}

pub fn is_token2022_mint(data: &[u8], acc_pubkey: Pubkey) -> bool {
    let is_unpacked = spl_token_2022::state::Mint::unpack(&data).is_ok();
    let is_len_matching = data.len() == spl_token_2022::state::Mint::LEN;

    if is_unpacked != is_len_matching {
        // e.g. token accounts len 82, acc CAsQPaWGczbxLcGEFTHXdYRD3rs8CU131pjABpU8KuSo
        warn!(
            "mismatching criteria: token account len {:?}, acc {}",
            data.len(),
            acc_pubkey
        );
    }
    if is_unpacked && is_len_matching {
        trace!("mint2022 found {}", acc_pubkey);
        return true;
    }

    return false;
}
