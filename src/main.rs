use std::{fs::File, sync::Arc, time::Duration};

use bincode;
use clap::Parser;
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::{broadcast::Sender, mpsc::unbounded_channel},
    time::{interval, sleep},
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;
use yellowstone_grpc_swap_api::{
    geyser_client::{
        AccountsUpdateNotification, UpdateOneofNotifications, geyser_subscribe_accounts_loop,
    },
    geyser_streaming::SlotTracker,
};

#[derive(Parser)]
struct Args {
    #[clap(long)]
    rpc_url: String,
    #[clap(long)]
    accounts: String,
    #[clap(long)]
    with_add_accounts: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    env_logger::builder()
        .format_timestamp(Some(env_logger::TimestampPrecision::Micros))
        .init();

    log::info!(
        "worker_threads: {}",
        tokio::runtime::Handle::current().metrics().num_workers()
    );

    let Args {
        rpc_url,
        accounts,
        with_add_accounts,
    } = Args::parse();

    let (endpoint, x_token) = extract_address_and_access_token(&rpc_url);

    let keys = load_accounts(&accounts);

    let (update_oneof_notifications_sender, mut update_oneof_notifications_receiver) =
        unbounded_channel::<UpdateOneofNotifications>();
    let geyser_accounts_update_sender = Sender::new(100);
    let accounts_chunk_count = 12;

    let slot_tracker = Arc::new(SlotTracker::new(accounts_chunk_count));

    let geyser_accounts_update_sender_clone = geyser_accounts_update_sender.clone();
    tokio::spawn(async move {
        let mut add_ran = false;
        if with_add_accounts {
            loop {
                if geyser_accounts_update_sender_clone.receiver_count() > 0 {
                    break;
                }
                log::info!("Waiting for geyser_accounts_update_receivers");
                sleep(Duration::from_millis(200)).await;
            }

            let mut interval = interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                let add_accounts = if !add_ran {
                    (0..200_000)
                        .map(|_| Pubkey::new_unique())
                        .collect::<Vec<_>>()
                } else {
                    vec![Pubkey::new_unique()]
                };

                add_ran = true;
                geyser_accounts_update_sender_clone
                    .send(AccountsUpdateNotification::AddAccounts(add_accounts))
                    .unwrap();
            }
        }
    });

    geyser_subscribe_accounts_loop(
        endpoint,
        x_token,
        Some(CompressionEncoding::Zstd),
        false,
        keys,
        update_oneof_notifications_sender,
        geyser_accounts_update_sender,
        accounts_chunk_count,
        slot_tracker,
    )
    .await;
}

pub fn load_accounts(accounts: &str) -> Vec<Pubkey> {
    let file = File::open(accounts).unwrap();
    bincode::deserialize_from(file).unwrap()
}

pub fn extract_address_and_access_token(rpc_url: &str) -> (String, Option<String>) {
    // check if rpc_url contains rpcpool.com
    if rpc_url.contains("rpcpool.com") {
        rpc_url
            .rsplit_once('/')
            .map(|(a, b)| (a.into(), Some(b.into())))
            .expect("Could not split address and token")
    } else if rpc_url.contains("helius-rpc.com") {
        // helius does not have access token, but it can be added but a completely different endpoint
        (rpc_url.into(), None)
    } else {
        // add more extraction method here
        (rpc_url.into(), None)
    }
}
