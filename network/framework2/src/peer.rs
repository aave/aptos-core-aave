// Copyright © Aptos Foundation
// TODO: move into network/framework2

use std::io::{Error, ErrorKind};
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use futures::channel::oneshot;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Receiver;
use crate::protocols::wire::messaging::v1::{MultiplexMessage, MultiplexMessageSink, MultiplexMessageStream, NetworkMessage, WriteError};
use bytes::Bytes;
use futures::io::{AsyncRead,AsyncReadExt,AsyncWrite};
use futures::StreamExt;
use futures::SinkExt;
use futures::stream::Fuse;
use tokio::sync::mpsc::error::{SendError, TryRecvError};
use aptos_config::config::NetworkConfig;
use aptos_config::network_id::PeerNetworkId;
use aptos_logger::{error, info, warn};
use crate::application::ApplicationCollector;
use crate::application::interface::{Closer, OpenRpcRequestState, OutboundRpcMatcher};
use crate::application::storage::PeersAndMetadata;
use crate::ProtocolId;
use crate::protocols::network::{OutboundPeerConnections, PeerStub, ReceivedMessage, RpcError};
use crate::protocols::stream::{StreamFragment, StreamHeader, StreamMessage};
use crate::transport::ConnectionMetadata;

// TODO: move into network/framework2
pub fn start_peer<TSocket>(
    config: &NetworkConfig,
    socket: TSocket,
    connection_metadata: ConnectionMetadata,
    apps: Arc<ApplicationCollector>,
    handle: Handle,
    remote_peer_network_id: PeerNetworkId,
    peers_and_metadata: Arc<PeersAndMetadata>,
    peer_senders: Arc<OutboundPeerConnections>,
)
where
    TSocket: crate::transport::TSocket
{
    let (sender, to_send) = tokio::sync::mpsc::channel::<NetworkMessage>(config.network_channel_size);
    let open_outbound_rpc = OutboundRpcMatcher::new();
    let max_frame_size = config.max_frame_size;
    let (read_socket, write_socket) = socket.split();
    let reader =
        MultiplexMessageStream::new(read_socket, max_frame_size).fuse();
    let writer = MultiplexMessageSink::new(write_socket, max_frame_size);
    let closed = Closer::new();
    handle.spawn(open_outbound_rpc.clone().cleanup(Duration::from_millis(100), closed.clone()));
    handle.spawn(writer_task(to_send, writer, max_frame_size, closed.clone()));
    handle.spawn(reader_task(reader, apps, remote_peer_network_id, open_outbound_rpc.clone(), handle.clone(), closed.clone()));
    let stub = PeerStub::new(sender, open_outbound_rpc);
    peers_and_metadata.insert_connection_metadata(remote_peer_network_id, connection_metadata.clone());
    peer_senders.insert(remote_peer_network_id, stub);
    handle.spawn(peer_cleanup_task(remote_peer_network_id, connection_metadata, closed, peers_and_metadata, peer_senders));
}

/// state needed in writer_task()
struct WriterContext<WriteThing: AsyncWrite + Unpin + Send> {
    /// increment for each new fragment stream
    stream_request_id : u32,
    /// remaining payload bytes of curretnly fragmenting large message
    large_message: Option<Vec<u8>>,
    /// index into chain of fragments
    large_fragment_id: u8,
    /// toggle to send normal msg or send fragment of large message
    send_large: bool,
    /// if we have a large message in flight and another arrives, stash it here
    next_large_msg: Option<NetworkMessage>,
    /// TODO: pull this from node config
    max_frame_size: usize,

    /// messages from apps to send to the peer
    to_send: Receiver<NetworkMessage>,
    /// encoder wrapper around socket write half
    writer: MultiplexMessageSink<WriteThing>,
}

impl<WriteThing: AsyncWrite + Unpin + Send> WriterContext<WriteThing> {
    fn new(
        to_send: Receiver<NetworkMessage>,
        writer: MultiplexMessageSink<WriteThing>,
        max_frame_size: usize,
    ) -> Self {
        Self {
            stream_request_id: 0,
            large_message: None,
            large_fragment_id: 0,
            send_large: false,
            next_large_msg: None,
            max_frame_size,
            to_send,
            writer,
        }
    }

    /// send a next chunk from a currently fragmenting large message
    fn next_large(&mut self) -> MultiplexMessage {
        let mut blob = self.large_message.take().unwrap();
        if blob.len() > self.max_frame_size {
            let rest = blob.split_off(self.max_frame_size);
            self.large_message = Some(rest);
        }
        self.large_fragment_id = self.large_fragment_id + 1;
        self.send_large = false;
        MultiplexMessage::Stream(StreamMessage::Fragment(StreamFragment {
            request_id: self.stream_request_id,
            fragment_id: self.large_fragment_id,
            raw_data: blob,
        }))
    }

    fn start_large(&mut self, msg: NetworkMessage) -> MultiplexMessage {
        self.stream_request_id = self.stream_request_id + 1;
        self.send_large = false;
        self.large_fragment_id = 0;
        let mut num_fragments = msg.data_len() / self.max_frame_size;
        let mut msg = msg;
        while num_fragments * self.max_frame_size < msg.data_len() {
            num_fragments = num_fragments + 1;
        }
        if num_fragments > 0x0ff {
            panic!("huge message cannot be fragmented {:?} > 255 * {:?}", msg.data_len(), self.max_frame_size);
        }
        let num_fragments = num_fragments as u8;
        let rest = match &mut msg {
            NetworkMessage::Error(_) => {
                unreachable!("NetworkMessage::Error should always fit in a single frame")
            },
            NetworkMessage::RpcRequest(request) => {
                request.raw_request.split_off(self.max_frame_size)
            },
            NetworkMessage::RpcResponse(response) => {
                response.raw_response.split_off(self.max_frame_size)
            },
            NetworkMessage::DirectSendMsg(message) => {
                message.raw_msg.split_off(self.max_frame_size)
            },
        };
        self.large_message = Some(rest);
        MultiplexMessage::Stream(StreamMessage::Header(StreamHeader {
            request_id: self.stream_request_id,
            num_fragments,
            message: msg,
        }))
    }

    async fn run(mut self, mut closed: Closer) {
        loop {
            let mm = if self.large_message.is_some() {
                if self.send_large || self.next_large_msg.is_some() {
                    self.next_large()
                } else {
                    match self.to_send.try_recv() {
                        Ok(msg) => {
                            if msg.data_len() > self.max_frame_size {
                                // finish prior large message before starting a new large message
                                self.next_large_msg = Some(msg);
                                self.next_large()
                            } else {
                                // send small message now, large chunk next
                                self.send_large = true;
                                MultiplexMessage::Message(msg)
                            }
                        }
                        Err(err) => match err {
                            TryRecvError::Empty => {
                                // ok, no next small msg, continue with chunks of large message
                                self.next_large()
                            }
                            TryRecvError::Disconnected => {
                                info!("peer writer source closed");
                                break
                            }
                        }
                    }
                }
            } else if self.next_large_msg.is_some() {
                let msg = self.next_large_msg.take().unwrap();
                self.start_large(msg)
            } else {
                tokio::select! {
                    send_result = self.to_send.recv() => match send_result {
                    None => {
                        info!("peer writer source closed");
                        break;
                    },
                    Some(msg) => {
                        if msg.data_len() > self.max_frame_size {
                            // start stream
                            self.start_large(msg)
                        } else {
                            MultiplexMessage::Message(msg)
                        }
                    },
                    },
                    // TODO: why does select on close.wait() work below but I did this workaround here?
                    wait_result = closed.done.wait_for(|x| *x) => {
                        info!("wait result {:?}", wait_result);
                        break;
                    },
                }
            };
            tokio::select! {
                send_result = self.writer.send(&mm) => match send_result {
                    Ok(_) => {
                        // TODO: counter msg sent, msg size sent
                    }
                    Err(err) => {
                        // TODO: counter net write err
                        warn!("error sending message to peer: {:?}", err);
                        break;
                    }
                },
                _ = closed.wait() => {
                    break;
                }
            }
        }
        closed.close();
        info!("peer writer closing");
    }

    fn split_message(&self, msg: &mut NetworkMessage) -> Vec<u8> {
        match msg {
            NetworkMessage::Error(_) => {
                unreachable!("NetworkMessage::Error should always fit in a single frame")
            },
            NetworkMessage::RpcRequest(request) => {
                request.raw_request.split_off(self.max_frame_size)
            },
            NetworkMessage::RpcResponse(response) => {
                response.raw_response.split_off(self.max_frame_size)
            },
            NetworkMessage::DirectSendMsg(message) => {
                message.raw_msg.split_off(self.max_frame_size)
            },
        }
    }
}

async fn writer_task(
    mut to_send: Receiver<NetworkMessage>,
    mut writer: MultiplexMessageSink<impl AsyncWrite + Unpin + Send + 'static>,
    max_frame_size: usize,
    closed: Closer,
) {
    let wt = WriterContext::new(to_send, writer, max_frame_size);
    wt.run(closed).await;
}

async fn complete_rpc(sender: oneshot::Sender<Result<Bytes,RpcError>>, nmsg: NetworkMessage) {
    if let NetworkMessage::RpcResponse(response) = nmsg {
        let blob = response.raw_response;
        match sender.send(Ok(blob.into())) {
            Ok(_) => {
                // TODO: counter rpc completion to app
            }
            Err(err) => {
                // TODO: counter rpc completion dropped at app
                warn!("rpc completion dropped at app")
            }
        }
    } else {
        unreachable!("complete_rpc called on other than NetworkMessage::RpcResponse")
    }
}

struct ReaderContext<ReadThing: AsyncRead + Unpin> {
    reader: Fuse<MultiplexMessageStream<ReadThing>>,
    apps: Arc<ApplicationCollector>,
    remote_peer_network_id: PeerNetworkId,
    open_outbound_rpc: OutboundRpcMatcher,
    handle: Handle,

    // defragment context
    current_stream_id : u32,
    large_message : Option<NetworkMessage>,
    fragment_index : u8,
    num_fragments : u8,
}

impl<ReadThing: AsyncRead + Unpin> ReaderContext<ReadThing> {
    fn new(
        reader: Fuse<MultiplexMessageStream<ReadThing>>,
        apps: Arc<ApplicationCollector>,
        remote_peer_network_id: PeerNetworkId,
        open_outbound_rpc: OutboundRpcMatcher,
        handle: Handle,
    ) -> Self {
        Self {
            reader,
            apps,
            remote_peer_network_id,
            open_outbound_rpc,
            handle,

            current_stream_id: 0,
            large_message: None,
            fragment_index: 0,
            num_fragments: 0,
        }
    }

    async fn forward(&self, protocol_id: ProtocolId, nmsg: NetworkMessage) {
        match self.apps.apps.get(&protocol_id) {
            None => {
                // TODO: counter
                warn!("got rpc req for protocol {:?} we don't handle", protocol_id);
                // TODO: drop connection
            }
            Some(app) => {
                match app.sender.send(ReceivedMessage{ message: nmsg, sender: self.remote_peer_network_id }).await {
                    Ok(_) => {
                        // TODO: counter
                    }
                    Err(err) => {
                        // TODO: counter
                        error!("app channel protocol_id={:?} err={:?}", protocol_id, err);
                    }
                }
            }
        }
    }

    async fn handle_message(&self, nmsg: NetworkMessage) {
        match &nmsg {
            NetworkMessage::Error(errm) => {
                // TODO: counter
                warn!("got error message: {:?}", errm)
            }
            NetworkMessage::RpcRequest(request) => {
                let protocol_id = request.protocol_id;
                self.forward(protocol_id, nmsg);
            }
            NetworkMessage::RpcResponse(response) => {
                match self.open_outbound_rpc.remove(&response.request_id) {
                    None => {
                        // TODO: counter rpc response dropped, no receiver
                    }
                    Some(rpc_state) => {
                        self.handle.spawn(complete_rpc(rpc_state.sender, nmsg));//response.raw_response));
                    }
                }
            }
            NetworkMessage::DirectSendMsg(message) => {
                let protocol_id = message.protocol_id;
                self.forward(protocol_id, nmsg);
            }
        }
    }

    async fn handle_stream(&mut self, fragment: StreamMessage) {
        match fragment {
            StreamMessage::Header(head) => {
                if self.num_fragments != self.fragment_index {
                    warn!("fragment index = {:?} of {:?} total fragments with new stream header", self.fragment_index, self.num_fragments);
                }
                self.current_stream_id = head.request_id;
                self.num_fragments = head.num_fragments;
                self.large_message = Some(head.message);
                self.fragment_index = 1;
            }
            StreamMessage::Fragment(more) => {
                if more.request_id != self.current_stream_id {
                    warn!("got stream request_id={:?} while {:?} was in progress", more.request_id, self.current_stream_id);
                    // TODO: counter? disconnect from peer?
                    self.num_fragments = 0;
                    self.fragment_index = 0;
                    return;
                }
                if more.fragment_id != self.fragment_index {
                    warn!("got fragment_id {:?}, expected {:?}", more.fragment_id, self.fragment_index);
                    // TODO: counter? disconnect from peer?
                    self.num_fragments = 0;
                    self.fragment_index = 0;
                    return;
                }
                match self.large_message.as_mut() {
                    None => {
                        warn!("got fragment without header");
                        return;
                    }
                    Some(lm) => match lm {
                        NetworkMessage::Error(_) => {
                            unreachable!("stream fragment should never be NetworkMessage::Error")
                        }
                        NetworkMessage::RpcRequest(request) => {
                            request.raw_request.extend_from_slice(more.raw_data.as_slice());
                        }
                        NetworkMessage::RpcResponse(response) => {
                            response.raw_response.extend_from_slice(more.raw_data.as_slice());
                        }
                        NetworkMessage::DirectSendMsg(message) => {
                            message.raw_msg.extend_from_slice(more.raw_data.as_slice());
                        }
                    }
                }
                self.fragment_index += 1;
                if self.fragment_index == self.num_fragments {
                    let large_message = self.large_message.take().unwrap();
                    self.handle_message(large_message);
                }
            }
        }
    }

    async fn run(mut self, mut closed: Closer) {
        loop {
            tokio::select! {
                rrmm = self.reader.next() => match rrmm {
                    Some(rmm) => match rmm {
                        Ok(msg) => match msg {
                            MultiplexMessage::Message(nmsg) => {
                                self.handle_message(nmsg);
                            }
                            MultiplexMessage::Stream(fragment) => {
                                self.handle_stream(fragment);
                            }
                        }
                        Err(_) => {}
                    }
                    None => {
                        break;
                    }
                },
                _ = closed.done.wait_for(|x| *x) => {
                    return;
                },
            }
        }
        closed.close();
    }
}

async fn reader_task(
    mut reader: Fuse<MultiplexMessageStream<impl AsyncRead + Unpin>>,
    apps: Arc<ApplicationCollector>,
    remote_peer_network_id: PeerNetworkId,
    open_outbound_rpc: OutboundRpcMatcher,
    handle: Handle,
    closed: Closer,
) {
    let rc = ReaderContext::new(reader, apps, remote_peer_network_id, open_outbound_rpc, handle);
    rc.run(closed).await;
    info!("peer reader finished"); // TODO: cause the writer to close?
}

async fn peer_cleanup_task(
    remote_peer_network_id: PeerNetworkId,
    connection_metadata: ConnectionMetadata,
    mut closed: Closer,
    peers_and_metadata: Arc<PeersAndMetadata>,
    peer_senders: Arc<OutboundPeerConnections>,
) {
    closed.wait().await;
    peer_senders.remove(&remote_peer_network_id);
    peers_and_metadata.remove_peer_metadata(remote_peer_network_id, connection_metadata.connection_id);
}