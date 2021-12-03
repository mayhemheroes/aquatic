use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aquatic_common::access_list::{create_access_list_cache, AccessListArcSwap, AccessListCache};
use aquatic_common::convert_ipv4_mapped_ipv6;
use aquatic_ws_protocol::*;
use async_tungstenite::WebSocketStream;
use futures::stream::{SplitSink, SplitStream};
use futures_lite::future::race;
use futures_lite::StreamExt;
use futures_rustls::server::TlsStream;
use futures_rustls::TlsAcceptor;
use glommio::channels::channel_mesh::{MeshBuilder, Partial, Role, Senders};
use glommio::channels::local_channel::{new_bounded, LocalReceiver, LocalSender};
use glommio::channels::shared_channel::ConnectedReceiver;
use glommio::net::{TcpListener, TcpStream};
use glommio::timer::TimerActionRepeat;
use glommio::{enclose, prelude::*};
use hashbrown::HashMap;
use slab::Slab;

use crate::config::Config;

use crate::common::*;

use super::common::*;

const LOCAL_CHANNEL_SIZE: usize = 16;

struct PendingScrapeResponse {
    pending_worker_out_messages: usize,
    stats: HashMap<InfoHash, ScrapeStatistics>,
}

struct ConnectionReference {
    out_message_sender: Rc<LocalSender<(ConnectionMeta, OutMessage)>>,
}

pub async fn run_socket_worker(
    config: Config,
    state: State,
    tls_config: Arc<TlsConfig>,
    in_message_mesh_builder: MeshBuilder<(ConnectionMeta, InMessage), Partial>,
    out_message_mesh_builder: MeshBuilder<(ConnectionMeta, OutMessage), Partial>,
    num_bound_sockets: Arc<AtomicUsize>,
) {
    let config = Rc::new(config);
    let access_list = state.access_list;

    let listener = TcpListener::bind(config.network.address).expect("bind socket");
    num_bound_sockets.fetch_add(1, Ordering::SeqCst);

    let (in_message_senders, _) = in_message_mesh_builder.join(Role::Producer).await.unwrap();
    let in_message_senders = Rc::new(in_message_senders);

    let tq_prioritized =
        executor().create_task_queue(Shares::Static(50), Latency::NotImportant, "prioritized");
    let tq_regular =
        executor().create_task_queue(Shares::Static(1), Latency::NotImportant, "regular");

    let (_, mut out_message_receivers) =
        out_message_mesh_builder.join(Role::Consumer).await.unwrap();
    let out_message_consumer_id = ConsumerId(out_message_receivers.consumer_id().unwrap());

    let connection_slab = Rc::new(RefCell::new(Slab::new()));
    let connections_to_remove = Rc::new(RefCell::new(Vec::new()));

    // Periodically remove closed connections
    TimerActionRepeat::repeat_into(
        enclose!((config, connection_slab, connections_to_remove) move || {
            remove_closed_connections(
                config.clone(),
                connection_slab.clone(),
                connections_to_remove.clone(),
            )
        }),
        tq_prioritized,
    )
    .unwrap();

    for (_, out_message_receiver) in out_message_receivers.streams() {
        spawn_local_into(
            receive_out_messages(out_message_receiver, connection_slab.clone()),
            tq_regular,
        )
        .unwrap()
        .detach();
    }

    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        match stream {
            Ok(stream) => {
                let (out_message_sender, out_message_receiver) = new_bounded(LOCAL_CHANNEL_SIZE);
                let out_message_sender = Rc::new(out_message_sender);

                let key = RefCell::borrow_mut(&connection_slab).insert(ConnectionReference {
                    out_message_sender: out_message_sender.clone(),
                });

                spawn_local_into(enclose!((config, access_list, in_message_senders, tls_config, connections_to_remove) async move {
                    if let Err(err) = run_connection(
                        config,
                        access_list,
                        in_message_senders,
                        tq_prioritized,
                        tq_regular,
                        out_message_sender,
                        out_message_receiver,
                        out_message_consumer_id,
                        ConnectionId(key),
                        tls_config,
                        stream
                    ).await {
                        ::log::debug!("Connection::run() error: {:?}", err);
                    }

                    RefCell::borrow_mut(&connections_to_remove).push(key);
                }), tq_regular)
                .unwrap()
                .detach();
            }
            Err(err) => {
                ::log::error!("accept connection: {:?}", err);
            }
        }
    }
}

async fn remove_closed_connections(
    config: Rc<Config>,
    connection_slab: Rc<RefCell<Slab<ConnectionReference>>>,
    connections_to_remove: Rc<RefCell<Vec<usize>>>,
) -> Option<Duration> {
    let connections_to_remove = connections_to_remove.replace(Vec::new());

    for connection_id in connections_to_remove {
        if let Some(_) = RefCell::borrow_mut(&connection_slab).try_remove(connection_id) {
            ::log::debug!("removed connection with id {}", connection_id);
        } else {
            ::log::error!(
                "couldn't remove connection with id {}, it is not in connection slab",
                connection_id
            );
        }
    }

    Some(Duration::from_secs(
        config.cleaning.connection_cleaning_interval,
    ))
}

async fn receive_out_messages(
    mut out_message_receiver: ConnectedReceiver<(ConnectionMeta, OutMessage)>,
    connection_references: Rc<RefCell<Slab<ConnectionReference>>>,
) {
    while let Some(channel_out_message) = out_message_receiver.next().await {
        if let Some(reference) = connection_references
            .borrow()
            .get(channel_out_message.0.connection_id.0)
        {
            match reference.out_message_sender.try_send(channel_out_message) {
                Ok(()) | Err(GlommioError::Closed(_)) => {}
                Err(err) => {
                    ::log::info!(
                        "Couldn't send out_message from shared channel to local receiver: {:?}",
                        err
                    );
                }
            }
        }

        yield_if_needed().await;
    }
}

async fn run_connection(
    config: Rc<Config>,
    access_list: Arc<AccessListArcSwap>,
    in_message_senders: Rc<Senders<(ConnectionMeta, InMessage)>>,
    tq_prioritized: TaskQueueHandle,
    tq_regular: TaskQueueHandle,
    out_message_sender: Rc<LocalSender<(ConnectionMeta, OutMessage)>>,
    out_message_receiver: LocalReceiver<(ConnectionMeta, OutMessage)>,
    out_message_consumer_id: ConsumerId,
    connection_id: ConnectionId,
    tls_config: Arc<TlsConfig>,
    stream: TcpStream,
) -> anyhow::Result<()> {
    let peer_addr = stream
        .peer_addr()
        .map_err(|err| anyhow::anyhow!("Couldn't get peer addr: {:?}", err))?;

    let tls_acceptor: TlsAcceptor = tls_config.into();
    let stream = tls_acceptor.accept(stream).await?;

    let ws_config = tungstenite::protocol::WebSocketConfig {
        max_frame_size: Some(config.network.websocket_max_frame_size),
        max_message_size: Some(config.network.websocket_max_message_size),
        ..Default::default()
    };
    let stream = async_tungstenite::accept_async_with_config(stream, Some(ws_config)).await?;
    let (ws_out, ws_in) = futures::StreamExt::split(stream);

    let pending_scrape_slab = Rc::new(RefCell::new(Slab::new()));
    let access_list_cache = create_access_list_cache(&access_list);

    let reader_handle = spawn_local_into(
        enclose!((pending_scrape_slab) async move {
            let mut reader = ConnectionReader {
                config,
                access_list_cache,
                in_message_senders,
                out_message_sender,
                pending_scrape_slab,
                out_message_consumer_id,
                ws_in,
                peer_addr,
                connection_id,
            };

            reader.run_in_message_loop().await
        }),
        tq_regular,
    )
    .unwrap()
    .detach();

    let writer_handle = spawn_local_into(
        async move {
            let mut writer = ConnectionWriter {
                out_message_receiver,
                ws_out,
                pending_scrape_slab,
                peer_addr,
            };

            writer.run_out_message_loop().await
        },
        tq_prioritized,
    )
    .unwrap()
    .detach();

    race(reader_handle, writer_handle).await.unwrap()
}

struct ConnectionReader {
    config: Rc<Config>,
    access_list_cache: AccessListCache,
    in_message_senders: Rc<Senders<(ConnectionMeta, InMessage)>>,
    out_message_sender: Rc<LocalSender<(ConnectionMeta, OutMessage)>>,
    pending_scrape_slab: Rc<RefCell<Slab<PendingScrapeResponse>>>,
    out_message_consumer_id: ConsumerId,
    ws_in: SplitStream<WebSocketStream<TlsStream<TcpStream>>>,
    peer_addr: SocketAddr,
    connection_id: ConnectionId,
}

impl ConnectionReader {
    async fn run_in_message_loop(&mut self) -> anyhow::Result<()> {
        loop {
            ::log::debug!("read_in_message");

            let message = self.ws_in.next().await.unwrap()?;

            match InMessage::from_ws_message(message) {
                Ok(in_message) => {
                    ::log::debug!("received in_message: {:?}", in_message);

                    self.handle_in_message(in_message).await?;
                }
                Err(err) => {
                    ::log::debug!("Couldn't parse in_message: {:?}", err);

                    self.send_error_response("Invalid request".into(), None);
                }
            }

            yield_if_needed().await;
        }
    }

    async fn handle_in_message(&mut self, in_message: InMessage) -> anyhow::Result<()> {
        match in_message {
            InMessage::AnnounceRequest(announce_request) => {
                let info_hash = announce_request.info_hash;

                if self
                    .access_list_cache
                    .load()
                    .allows(self.config.access_list.mode, &info_hash.0)
                {
                    let in_message = InMessage::AnnounceRequest(announce_request);

                    let consumer_index =
                        calculate_in_message_consumer_index(&self.config, info_hash);

                    // Only fails when receiver is closed
                    self.in_message_senders
                        .send_to(
                            consumer_index,
                            (self.make_connection_meta(None), in_message),
                        )
                        .await
                        .unwrap();
                } else {
                    self.send_error_response("Info hash not allowed".into(), Some(info_hash));
                }
            }
            InMessage::ScrapeRequest(ScrapeRequest { info_hashes, .. }) => {
                let info_hashes = if let Some(info_hashes) = info_hashes {
                    info_hashes
                } else {
                    // If request.info_hashes is empty, don't return scrape for all
                    // torrents, even though reference server does it. It is too expensive.
                    self.send_error_response("Full scrapes are not allowed".into(), None);

                    return Ok(());
                };

                let mut info_hashes_by_worker: BTreeMap<usize, Vec<InfoHash>> = BTreeMap::new();

                for info_hash in info_hashes.as_vec() {
                    let info_hashes = info_hashes_by_worker
                        .entry(calculate_in_message_consumer_index(&self.config, info_hash))
                        .or_default();

                    info_hashes.push(info_hash);
                }

                let pending_worker_out_messages = info_hashes_by_worker.len();

                let pending_scrape_response = PendingScrapeResponse {
                    pending_worker_out_messages,
                    stats: Default::default(),
                };

                let pending_scrape_id = PendingScrapeId(
                    RefCell::borrow_mut(&mut self.pending_scrape_slab)
                        .insert(pending_scrape_response),
                );
                let meta = self.make_connection_meta(Some(pending_scrape_id));

                for (consumer_index, info_hashes) in info_hashes_by_worker {
                    let in_message = InMessage::ScrapeRequest(ScrapeRequest {
                        action: ScrapeAction,
                        info_hashes: Some(ScrapeRequestInfoHashes::Multiple(info_hashes)),
                    });

                    // Only fails when receiver is closed
                    self.in_message_senders
                        .send_to(consumer_index, (meta, in_message))
                        .await
                        .unwrap();
                }
            }
        }

        Ok(())
    }

    fn send_error_response(&self, failure_reason: Cow<'static, str>, info_hash: Option<InfoHash>) {
        let out_message = OutMessage::ErrorResponse(ErrorResponse {
            action: Some(ErrorResponseAction::Scrape),
            failure_reason,
            info_hash,
        });

        if let Err(err) = self
            .out_message_sender
            .try_send((self.make_connection_meta(None), out_message))
        {
            ::log::error!("ConnectionWriter::send_error_response failed: {:?}", err)
        }
    }

    fn make_connection_meta(&self, pending_scrape_id: Option<PendingScrapeId>) -> ConnectionMeta {
        ConnectionMeta {
            connection_id: self.connection_id,
            out_message_consumer_id: self.out_message_consumer_id,
            naive_peer_addr: self.peer_addr,
            converted_peer_ip: convert_ipv4_mapped_ipv6(self.peer_addr.ip()),
            pending_scrape_id,
        }
    }
}

struct ConnectionWriter {
    out_message_receiver: LocalReceiver<(ConnectionMeta, OutMessage)>,
    ws_out: SplitSink<WebSocketStream<TlsStream<TcpStream>>, tungstenite::Message>,
    pending_scrape_slab: Rc<RefCell<Slab<PendingScrapeResponse>>>,
    peer_addr: SocketAddr,
}

impl ConnectionWriter {
    async fn run_out_message_loop(&mut self) -> anyhow::Result<()> {
        loop {
            let (meta, out_message) = self.out_message_receiver.recv().await.ok_or_else(|| {
                anyhow::anyhow!("ConnectionWriter couldn't receive message, sender is closed")
            })?;

            if meta.naive_peer_addr != self.peer_addr {
                return Err(anyhow::anyhow!("peer addresses didn't match"));
            }

            match out_message {
                OutMessage::ScrapeResponse(out_message) => {
                    let pending_scrape_id = meta
                        .pending_scrape_id
                        .expect("meta.pending_scrape_id not set");

                    let finished = if let Some(pending) = Slab::get_mut(
                        &mut RefCell::borrow_mut(&self.pending_scrape_slab),
                        pending_scrape_id.0,
                    ) {
                        pending.stats.extend(out_message.files);
                        pending.pending_worker_out_messages -= 1;

                        pending.pending_worker_out_messages == 0
                    } else {
                        return Err(anyhow::anyhow!("pending scrape not found in slab"));
                    };

                    if finished {
                        let out_message = {
                            let mut slab = RefCell::borrow_mut(&self.pending_scrape_slab);

                            let pending = slab.remove(pending_scrape_id.0);

                            slab.shrink_to_fit();

                            OutMessage::ScrapeResponse(ScrapeResponse {
                                action: ScrapeAction,
                                files: pending.stats,
                            })
                        };

                        self.send_out_message(&out_message).await?;
                    }
                }
                out_message => {
                    self.send_out_message(&out_message).await?;
                }
            };

            yield_if_needed().await;
        }
    }

    async fn send_out_message(&mut self, out_message: &OutMessage) -> anyhow::Result<()> {
        futures::SinkExt::send(&mut self.ws_out, out_message.to_ws_message()).await?;
        futures::SinkExt::flush(&mut self.ws_out).await?;

        Ok(())
    }
}

fn calculate_in_message_consumer_index(config: &Config, info_hash: InfoHash) -> usize {
    (info_hash.0[0] as usize) % config.request_workers
}