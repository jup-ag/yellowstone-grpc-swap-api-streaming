use anyhow::Result;
use futures::{
    SinkExt, StreamExt, TryStreamExt,
    future::join_all,
    stream::{FuturesUnordered, TryReadyChunksError},
};
use solana_sdk::{
    account::Account, clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey,
};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, broadcast, mpsc::UnboundedSender},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::{
    geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestPing, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdatePong,
        subscribe_update::UpdateOneof,
    },
    tonic::codec::CompressionEncoding,
};

use crate::geyser_streaming::SlotTracker;

#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    pub pubkey: Pubkey,
    pub is_startup: bool,
    pub slot: u64,
    pub write_version: u64,
    // pub txn_signature: String,
    pub account: Account,
}

impl From<SubscribeUpdateAccount> for AccountPretty {
    fn from(
        SubscribeUpdateAccount {
            is_startup,
            slot,
            account,
        }: SubscribeUpdateAccount,
    ) -> Self {
        let account = account.expect("should be defined");
        Self {
            is_startup,
            slot,
            account: Account {
                lamports: account.lamports,
                data: account.data,
                owner: Pubkey::try_from(account.owner).expect("valid pubkey"),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
            },
            pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
            write_version: account.write_version,
            // txn_signature: bs58::encode(account.txn_signature.unwrap_or_default()).into_string(),
        }
    }
}

pub type UpdateOneofNotifications = Vec<UpdateOneofNotification>;

#[derive(Debug)]
pub enum UpdateOneofNotification {
    Account(AccountPretty),
    Slot {
        slot: Slot,
        commitment: CommitmentLevel,
    },
}

#[derive(Debug, Clone)]
pub enum AccountsUpdateNotification {
    AddAccounts(Vec<Pubkey>),
    RemoveAccounts(Vec<Pubkey>),
}

#[allow(clippy::too_many_arguments)]
pub async fn geyser_subscribe_accounts_loop(
    endpoint: String,
    x_token: Option<String>,
    compression_encoding: Option<CompressionEncoding>,
    enable_ping: bool,
    keys: Vec<Pubkey>,
    update_oneof_notifications_sender: UnboundedSender<UpdateOneofNotifications>,
    geyser_accounts_update_sender: broadcast::Sender<AccountsUpdateNotification>,
    accounts_chunk_count: usize,
    slot_tracker: Arc<SlotTracker>,
) {
    log::info!(
        "geyser_subscribe_accounts_loop start, initial filter with {}",
        keys.len()
    );

    let chunk_size = keys.len().div_ceil(accounts_chunk_count - 1);
    let mut chunked_keys: Vec<Vec<Pubkey>> = keys
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<Vec<_>>>();
    // Memory leak mitigation, empty chunk where new accounts will be subscribed to
    chunked_keys.insert(0, vec![]);

    log::info!("Starting geyser client with accounts chunk size: {chunk_size}");

    let futures_unordered =
        FuturesUnordered::from_iter(chunked_keys.into_iter().enumerate().map(|(idx, keys)| {
            geyser_accounts_streamer_loop(
                idx,
                endpoint.clone(),
                x_token.clone(),
                compression_encoding,
                keys,
                enable_ping,
                &geyser_accounts_update_sender,
                &update_oneof_notifications_sender,
                slot_tracker.clone(),
            )
        }));
    futures_unordered.collect::<Vec<_>>().await;
}

pub const SUBSCRIBE_ACCOUNTS_CHUNK_SIZE: usize = 10_000;

const STUB_PUBKEY: Pubkey = Pubkey::new_from_array([
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
]);

fn connect(
    endpoint: String,
    x_token: Option<String>,
    compression_encoding: Option<CompressionEncoding>,
) -> Result<GeyserGrpcClient<impl Interceptor>> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(x_token)?
        .connect_timeout(Duration::from_secs(10))
        .tls_config(ClientTlsConfig::new().with_native_roots())?;

    if let Some(compression_encoding) = compression_encoding {
        builder = builder
            .accept_compressed(compression_encoding)
            .send_compressed(compression_encoding);
    }
    builder = builder.http2_adaptive_window(true); // Dynamic window sizing

    builder.connect_lazy().map_err(Into::into)
}

struct StreamerMetrics {
    updated_at: Instant,
    sum_account_data_len: usize,
    accounts_received: usize,
}

impl StreamerMetrics {
    fn new() -> Self {
        Self {
            updated_at: Instant::now(),
            sum_account_data_len: 0,
            accounts_received: 0,
        }
    }

    fn update(&mut self, idx: usize) {
        let elapsed = self.updated_at.elapsed();
        if elapsed > Duration::from_secs(15) {
            let bandwidth = self.sum_account_data_len as f64 / elapsed.as_secs_f64();
            log::info!(
                "idx={idx}, sum_account_data_len: {}, accounts_received: {}, bandwidth: {}",
                self.sum_account_data_len,
                self.accounts_received,
                bandwidth,
            );
            *self = Self::new();
        }
    }
}

enum PingStatus {
    Waiting(Instant),
    Sent(Instant),
}

struct PingService {
    ping_status: PingStatus,
    id: i32,
}

impl PingService {
    const WAITING_DURATION: Duration = Duration::from_secs(10);

    fn new() -> Self {
        Self {
            ping_status: PingStatus::Waiting(Instant::now()),
            id: 0,
        }
    }

    fn should_send(&self) -> bool {
        match self.ping_status {
            PingStatus::Waiting(instant) => {
                if instant.elapsed() > Self::WAITING_DURATION {
                    true
                } else {
                    false
                }
            }
            PingStatus::Sent(_) => false,
        }
    }

    fn start_next_ping(&mut self) -> i32 {
        self.ping_status = PingStatus::Sent(Instant::now());
        self.id
    }

    fn handle_pong(&mut self, idx: usize, subscribe_update_pong: SubscribeUpdatePong) {
        match self.ping_status {
            PingStatus::Waiting(_) => {
                log::error!(
                    "idx={idx} Received pong {} while waiting",
                    subscribe_update_pong.id
                );
            }
            PingStatus::Sent(instant) => {
                if self.id == subscribe_update_pong.id {
                    log::info!(
                        "idx={idx} Pong {} received after: {:.3?}",
                        self.id,
                        instant.elapsed()
                    );
                    self.id += 1;
                } else {
                    log::error!(
                        "idx={idx} Received pong {} not matching id",
                        subscribe_update_pong.id
                    );
                }
                self.ping_status = PingStatus::Waiting(Instant::now());
            }
        }
    }
}

struct AccountMetrics {
    started_at: Instant,
    address_to_updates_and_bytes: HashMap<Pubkey, (usize, usize)>,
}

impl AccountMetrics {
    const DURATION: Duration = Duration::from_secs(10);

    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            address_to_updates_and_bytes: HashMap::default(),
        }
    }

    fn update(&mut self, idx: usize, address: Pubkey, data_len: usize) {
        let (count, total_data_len) = self
            .address_to_updates_and_bytes
            .entry(address)
            .or_default();
        *count += 1;
        *total_data_len += data_len;

        if self.started_at.elapsed() > Self::DURATION {
            let top_n = top_n_by_total_data_len_heap(&self.address_to_updates_and_bytes, 10);
            log::info!("idx={idx} top_n_by_total_data_len: {top_n:?}");

            *self = Self::new();
        }
    }
}

fn top_n_by_total_data_len_heap(
    map: &HashMap<Pubkey, (usize, usize)>,
    n: usize,
) -> Vec<(Pubkey, (usize, usize))> {
    let mut heap: BinaryHeap<Reverse<(usize, Pubkey, usize)>> = BinaryHeap::new();
    // store as (total_len, address, count) in Reverse so it becomes a min-heap

    for (addr, (count, total_len)) in map {
        heap.push(Reverse((*total_len, *addr, *count)));
        if heap.len() > n {
            heap.pop(); // remove smallest
        }
    }

    // Convert back
    let mut result: Vec<_> = heap
        .into_iter()
        .map(|Reverse((total_len, addr, count))| (addr, (count, total_len)))
        .collect();
    // Optional: sort descending for final output
    result.sort_by_key(|&(_, (_, total_len))| std::cmp::Reverse(total_len));
    result
}

#[allow(clippy::too_many_arguments)]
async fn geyser_accounts_streamer_loop(
    idx: usize,
    endpoint: String,
    x_token: Option<String>,
    compression_encoding: Option<CompressionEncoding>,
    keys: Vec<Pubkey>,
    enable_ping: bool,
    geyser_accounts_update_sender: &broadcast::Sender<AccountsUpdateNotification>,
    update_oneof_notifications_sender: &UnboundedSender<UpdateOneofNotifications>,
    slot_tracker: Arc<SlotTracker>,
) {
    let is_zero = idx == 0;
    let account_filter_map = HashMap::from([(
        "client".into(),
        SubscribeRequestFilterAccounts {
            account: keys
                .iter()
                .map(|key| key.to_string())
                // Use stub key so that no keys yet does not trigger listening to the entire blockchain
                .chain(std::iter::once(STUB_PUBKEY.to_string()))
                .collect(),
            ..Default::default()
        },
    )]);
    let slots = HashMap::from([(
        "client2".into(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        },
    )]);

    let account_filter_map_shared_state = Arc::new(Mutex::new(account_filter_map));
    let mut last_backpressure_reported_at = Instant::now();

    let mut first_try = true;

    let mut streamer_metrics = StreamerMetrics::new();
    let mut ping_service = PingService::new();
    let mut account_metrics = AccountMetrics::new();

    loop {
        if !first_try {
            sleep(Duration::from_millis(500)).await;
            log::info!("Retry to connect to subscribe to the geyser server idx={idx}");
        } else {
            first_try = false;
            log::info!("Connect to subscribe to the geyser server idx={idx}");
        }

        let mut client = connect(endpoint.clone(), x_token.clone(), compression_encoding)
            .expect("Invalid configuration to connect to the yellowstone geyser server");
        let (mut subscribe_request_sink, stream) = match client.subscribe().await {
            Ok(value) => value,
            Err(e) => {
                log::error!("Geyser grpc client error {e} idx={idx}");
                continue;
            }
        };

        let accounts = { account_filter_map_shared_state.lock().await.clone() };
        match subscribe_request_sink
            .send(SubscribeRequest {
                accounts,
                slots: slots.clone(),
                ..Default::default()
            })
            .await
        {
            Ok(()) => log::info!("subscribed to new accounts idx={idx}"),
            Err(e) => {
                log::error!("error sending resubscribe request idx={idx}: {e:?}");
                continue;
            }
        }

        let subscribe_request_sink = Arc::new(Mutex::new(subscribe_request_sink));

        let account_filter_map_shared_state = account_filter_map_shared_state.clone();
        let token = CancellationToken::new();
        let cloned_token = token.clone();
        let slots = slots.clone();
        let cloned_subscribe_request_sink = subscribe_request_sink.clone();
        let mut geyser_accounts_update_receiver = geyser_accounts_update_sender.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    maybe_accounts_update = geyser_accounts_update_receiver.recv() => {
                        let accounts_update = match maybe_accounts_update {
                            Ok(accounts_update) => accounts_update,
                            Err(recv_error) => {
                                panic!("Error receiving accounts update: {recv_error}");
                            }
                        };

                        match accounts_update {
                            AccountsUpdateNotification::RemoveAccounts(accounts_to_remove) => {
                                log::info!("Removing accounts from geyser: {:?}", accounts_to_remove.len());
                                let accounts_to_remove_string = accounts_to_remove
                                    .iter()
                                    .map(|key| key.to_string())
                                    .collect::<HashSet<_>>();

                                let mut account_filter_map = account_filter_map_shared_state.lock().await;

                                let accounts_filter = account_filter_map
                                    .get_mut("client")
                                    .unwrap();

                                let initial_len = accounts_filter.account.len();

                                accounts_filter.account.retain(|key| !accounts_to_remove_string.contains(key));

                                if initial_len != accounts_filter.account.len() {
                                    log::info!("removed {:?} accounts from geyser", initial_len - accounts_filter.account.len());

                                    let accounts = account_filter_map.clone();

                                    // resub
                                    match cloned_subscribe_request_sink.lock().await
                                        .send(SubscribeRequest {
                                            accounts,
                                            slots: slots.clone(),
                                            ..Default::default()
                                        })
                                        .await
                                    {
                                        Ok(()) => log::info!("subscribed to new accounts"),
                                        Err(e) => {
                                            log::error!("error sending resubscribe request: {e:?}"); // TODO: Failing to send should trigger a retry
                                        }
                                    }
                                }
                            }
                            AccountsUpdateNotification::AddAccounts(new_accounts) => {
                                // only first geyser loop should handle account addition
                                if is_zero {
                                    let new_accounts_string = new_accounts
                                        .iter()
                                        .map(|key| key.to_string())
                                        .collect::<Vec<_>>();

                                    let mut account_filter_map = account_filter_map_shared_state.lock().await;
                                    let client_account_filter = account_filter_map
                                        .get_mut("client")
                                        .unwrap();
                                    client_account_filter
                                        .account
                                        .extend(new_accounts_string);
                                    let client_account_filter_account_len = client_account_filter.account.len();

                                    // resub
                                    let accounts = account_filter_map.clone();
                                    match cloned_subscribe_request_sink.lock().await
                                        .send(SubscribeRequest {
                                            accounts,
                                            slots: slots.clone(),
                                            ..Default::default()
                                        })
                                        .await
                                    {
                                        Ok(()) => log::info!("subscribed to new accounts, account filter length: {client_account_filter_account_len}"),
                                        Err(e) => {
                                            log::error!("error sending resubscribe request: {e:?}"); // TODO: Failing to send should trigger a retry
                                        }
                                    }
                                }
                            }
                        };
                    }
                    _ = cloned_token.cancelled() => {
                        // The token was cancelled, task can shut down
                        break;
                    }
                }
            }
        });

        log::info!("stream opened idx={idx}");
        let mut slot_comparison_instant = if is_zero { Some(Instant::now()) } else { None };

        // Take all available account updates to clear any backlog and prevent backpressure
        let mut stream = stream.try_ready_chunks(SUBSCRIBE_ACCOUNTS_CHUNK_SIZE);
        while let Some(subscribe_update_result) = stream.next().await {
            let (subscribe_updates, should_break) = match subscribe_update_result {
                Ok(subscribe_updates) => (subscribe_updates, false),
                Err(TryReadyChunksError(subscribe_updates, err)) => {
                    log::error!("idx_{idx} error while getting subscribe update: {err:?}");
                    (subscribe_updates, true)
                }
            };

            if subscribe_updates.len() == SUBSCRIBE_ACCOUNTS_CHUNK_SIZE
                && last_backpressure_reported_at.elapsed() > Duration::from_secs(10)
            {
                last_backpressure_reported_at = Instant::now();
                log::warn!("Backpressure building up from geyser account idx={idx}");
            }

            let mut ping_received = false;
            let mut update_oneof_notifications = Vec::with_capacity(subscribe_updates.len());
            for subscribe_update in subscribe_updates {
                let (update_oneof_notification, has_ping) = handle_subscribe_update(
                    idx,
                    subscribe_update,
                    &slot_tracker,
                    &mut slot_comparison_instant,
                    &mut streamer_metrics,
                    &mut ping_service,
                    &mut account_metrics,
                );
                ping_received |= has_ping;

                update_oneof_notifications.extend(update_oneof_notification.into_iter());
            }

            if !update_oneof_notifications.is_empty() {
                update_oneof_notifications_sender
                    .send(update_oneof_notifications)
                    .unwrap();
            }

            if should_break {
                break;
            }

            if enable_ping && ping_received {
                match subscribe_request_sink
                    .lock()
                    .await
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1 }),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(()) => (),
                    Err(e) => {
                        log::error!("ping error: {e:?}");
                        break;
                    }
                }
            }

            if ping_service.should_send() {
                let lock_guard_at = Instant::now();
                let mut sink = subscribe_request_sink.lock().await;
                let elapsed = lock_guard_at.elapsed();
                if elapsed > Duration::from_millis(100) {
                    log::warn!("Took some time to lock: {elapsed:?}");
                }

                let id = ping_service.start_next_ping();
                match sink
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id }),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(()) => (),
                    Err(e) => {
                        log::error!("ping service ping error: {e:?}");
                        break;
                    }
                }
            }
        }

        log::error!("stream closed idx={idx}");
        token.cancel();
    }
}

fn handle_subscribe_update(
    idx: usize,
    subscribe_update: SubscribeUpdate,
    slot_tracker: &SlotTracker,
    slot_comparison_instant: &mut Option<Instant>,
    streamer_metrics: &mut StreamerMetrics,
    ping_service: &mut PingService,
    account_metrics: &mut AccountMetrics,
) -> (Option<UpdateOneofNotification>, bool) {
    let mut ping_received = false;

    let Some(update_oneof) = subscribe_update.update_oneof else {
        return (None, ping_received);
    };

    let update_one_of_notification = match update_oneof {
        UpdateOneof::Account(subscribe_update_account) => {
            streamer_metrics.accounts_received += 1;
            let account_pretty: AccountPretty = subscribe_update_account.into();

            streamer_metrics.sum_account_data_len += account_pretty.account.data.len();
            // account_metrics.update(
            //     idx,
            //     account_pretty.pubkey,
            //     account_pretty.account.data.len(),
            // );

            streamer_metrics.update(idx);
            Some(UpdateOneofNotification::Account(account_pretty))
        }
        UpdateOneof::Slot(subscribe_update_slot) => {
            let commitment = into_solana_sdk_commitment(subscribe_update_slot.status());

            match commitment {
                Some(commitment) => {
                    if commitment == CommitmentLevel::Processed {
                        slot_tracker.update(idx, subscribe_update_slot.slot);

                        if let Some(slot_comparison_instant) = slot_comparison_instant.as_mut() {
                            if slot_comparison_instant.elapsed() > Duration::from_secs(5) {
                                *slot_comparison_instant = Instant::now();
                                let (min, max, slow_index) = slot_tracker.get_min_max_slow_index();

                                if max - min > 3 {
                                    log::warn!(
                                        "Chunked geyser slots are lagging min: {min}, max: {max}, slow_idx: {slow_index}"
                                    );
                                }
                            }
                        }
                    }

                    Some(UpdateOneofNotification::Slot {
                        slot: subscribe_update_slot.slot,
                        commitment,
                    })
                }
                None => None,
            }
        }
        UpdateOneof::Ping(ping) => {
            log::debug!("Received geyser ping: {ping:?}");
            ping_received = true;
            None
        }
        UpdateOneof::Pong(subscribe_update_pong) => {
            ping_service.handle_pong(idx, subscribe_update_pong);
            None
        }
        _ => {
            log::debug!("Unhandled UpdateOneof message");
            None
        }
    };
    (update_one_of_notification, ping_received)
}

fn into_solana_sdk_commitment(
    commitment: yellowstone_grpc_proto::geyser::SlotStatus,
) -> Option<CommitmentLevel> {
    match commitment {
        yellowstone_grpc_proto::geyser::SlotStatus::SlotFinalized => {
            Some(CommitmentLevel::Finalized)
        }
        yellowstone_grpc_proto::geyser::SlotStatus::SlotConfirmed => {
            Some(CommitmentLevel::Confirmed)
        }
        yellowstone_grpc_proto::geyser::SlotStatus::SlotProcessed => {
            Some(CommitmentLevel::Processed)
        }
        // we ignore all other statuses
        _ => None,
    }
}
