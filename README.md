# Solana AccountsDB Reader

**`solana-accountsdb-reader` efficiently extracts all accounts in a snapshot**

# Running
```bash
cargo run --bin solana-accountsdb-reader -- --snapshot-archive-path snapshot-78017-6vfFEs6YnFZPfPRBPnjqgMN8UmE5jnpBGscKXsPCtdV7.tar.zst
```



# Trie

(testnet, macbook)
```
[2024-07-12T17:05:34Z INFO  solana_accountsdb_reader] built trie size in 23227ms with 27062102 entries
[2024-07-12T17:05:34Z INFO  solana_accountsdb_reader] rate 1165114.0 entries/sec
[2024-07-12T17:05:34Z INFO  solana_accountsdb_reader] iterated over trie with 392 items in 0ms
[2024-07-12T17:05:36Z INFO  solana_accountsdb_reader] serialized trie to 865987272 bytes (32.0bytes/item)
[2024-07-12T17:05:42Z INFO  solana_accountsdb_reader] deserialized trie in 6218ms with 27062102 entries
```
