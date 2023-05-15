use super::message;
use super::peer;

use log::{debug, info, trace};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
pub fn new(
    addr: std::net::SocketAddr,
    msg_sink: Sender<(Vec<u8>, peer::Handle)>,
) -> std::io::Result<(Context, Handle)> {
    let (control_signal_sender, control_signal_receiver) = channel(10000);

    let handle = Handle {
        control_signal_sender: control_signal_sender.clone(),
    };
    let ctx = Context {
        peers: HashMap::new(),
        addr,
        control_signal_receiver: control_signal_receiver,
        control_sender: control_signal_sender,
        new_msg_chan: msg_sink,
    };
    Ok((ctx, handle))
}

pub struct Context {
    peers: HashMap<SocketAddr, peer::Handle>,
    addr: std::net::SocketAddr,
    control_signal_receiver: Receiver<ControlSignal>,
    control_sender: Sender<ControlSignal>,
    new_msg_chan: Sender<(Vec<u8>, peer::Handle)>,
}

impl Context {
    /// Start a new server context.
    pub async fn start(self) -> std::io::Result<()> {
        tokio::spawn(async move {
            self.mainloop().await.unwrap();
        });
        Ok(())
    }

    pub async fn mainloop(mut self) -> std::io::Result<()> {
        // initialize the server socket
        let socket = TcpSocket::new_v4()?;
        socket.bind(self.addr.into())?;
        let listener = socket.listen(1024)?;

        info!("P2P server listening at {}", self.addr);
        let control_chan = self.control_sender.clone();

        tokio::spawn(async move {
            self.dispatch_control().await.unwrap();
        });

        loop {
            let (stream, addr) = listener.accept().await?;
            let _ = control_chan.send(ControlSignal::GetNewPeer(stream)).await;
            info!("Incoming peer from {}", addr);
        }
    }

    async fn dispatch_control(&mut self) -> std::io::Result<()> {
        // read the next control signal
        while let Some(ctrl) = self.control_signal_receiver.recv().await {
            match ctrl {
                ControlSignal::ConnectNewPeer(addr, result_chan) => {
                    trace!("Processing ConnectNewPeer command");
                    let handle = self.connect(&addr).await;
                    result_chan.send(handle).unwrap();
                }
                ControlSignal::BroadcastMessage(msg) => {
                    trace!("Processing BroadcastMessage command");
                    for (_, hd) in self.peers.iter_mut() {
                        hd.write(msg.clone());
                    }
                }
                ControlSignal::GetNewPeer(stream) => {
                    trace!("Processing GetNewPeer command");
                    self.accept(stream).await?;
                }
                ControlSignal::DroppedPeer(addr) => {
                    trace!("Processing DroppedPeer({})", addr);
                    self.peers.remove(&addr);
                    info!("Peer {} disconnected", addr);
                }
            }
        }
        return Ok(());
    }

    /// Connect to a peer, and register this peer
    async fn connect(&mut self, addr: &std::net::SocketAddr) -> std::io::Result<peer::Handle> {
        debug!("Establishing connection to peer {}", addr);

        if self.peers.contains_key(addr) {
            debug!("existed connection {}", addr);
            let handle = self.peers.get(addr).unwrap().clone();
            return Ok(handle);
        }

        let stream = TcpStream::connect(addr).await?;
        // register the new peer
        self.register(stream, peer::Direction::Outgoing).await
    }

    async fn accept(&mut self, stream: TcpStream) -> std::io::Result<()> {
        self.register(stream, peer::Direction::Incoming).await?;
        Ok(())
    }

    async fn register(
        &mut self,
        stream: TcpStream,
        _direction: peer::Direction,
    ) -> std::io::Result<peer::Handle> {
        // create a handle so that we can write to this peer TODO
        let (mut write_queue, handle) = peer::new(&stream)?;

        let new_msg_chan = self.new_msg_chan.clone();
        let handle_copy = handle.clone();
        let control_chan = self.control_sender.clone();
        let addr = stream.peer_addr()?;

        let (reader_stream, writer_stream) = stream.into_split();
        // start the reactor for this peer
        // first, start a task that keeps reading from this guy

        tokio::spawn(async move {
            // the buffer to store the frame header, which contains the length of the frame
            let mut reader = BufReader::new(reader_stream);
            let mut size_buffer: [u8; 4] = [0; 4];
            // the buffer to store the message content
            let mut msg_buffer: Vec<u8> = vec![];
            loop {
                // first, read exactly 4 bytes to get the frame header
                let msg_size = match reader.get_mut().read_exact(&mut size_buffer).await {
                    Ok(_) => u32::from_be_bytes(size_buffer),
                    Err(_) => {
                        break;
                    }
                };
                // then, read exactly msg_size bytes to get the whole message
                if msg_buffer.len() < msg_size as usize {
                    msg_buffer.resize(msg_size as usize, 0);
                }
                match reader
                    .read_exact(&mut msg_buffer[0..msg_size as usize])
                    .await
                {
                    Ok(_) => {
                        let new_payload: Vec<u8> = msg_buffer[0..msg_size as usize].to_vec();
                        let _ = new_msg_chan.send((new_payload, handle_copy.clone())).await;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            // the peer is disconnected
        });

        tokio::spawn(async move {
            let mut writer = BufWriter::new(writer_stream);

            loop {
                // first, get a message to write from the queue
                let new_msg = write_queue.recv().await.unwrap();

                // second, encode the length of the message
                let size_buffer = (new_msg.len() as u32).to_be_bytes();

                // third, write the frame header and the payload
                match writer.write_all(&size_buffer).await {
                    Ok(_) => {}
                    Err(_) => {
                        break;
                    }
                }
                match writer.write_all(&new_msg).await {
                    Ok(_) => {}
                    Err(_) => {
                        break;
                    }
                }
                match writer.flush().await {
                    Ok(_) => {}
                    Err(_) => {
                        break;
                    }
                }
            }
            // the peer is disconnected
            let _ = control_chan.send(ControlSignal::DroppedPeer(addr)).await;
        });

        // second, start a task that keeps writing to this guy

        // insert the peer handle so that we can broadcast to this guy later
        self.peers.insert(addr, handle.clone());
        Ok(handle)
    }
}

#[derive(Clone)]
pub struct Handle {
    control_signal_sender: Sender<ControlSignal>,
}

impl Handle {
    pub fn connect(&self, addr: std::net::SocketAddr) -> std::io::Result<peer::Handle> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let rt = Runtime::new().unwrap();

        rt.block_on(
            self.control_signal_sender
                .send(ControlSignal::ConnectNewPeer(addr, sender)),
        );
        rt.block_on(receiver).unwrap()
    }

    pub fn broadcast(&self, msg: message::Message) {
        let rt = Runtime::new().unwrap();

        rt.block_on(
            self.control_signal_sender
                .send(ControlSignal::BroadcastMessage(msg)),
        );
    }
}

enum ControlSignal {
    ConnectNewPeer(
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<std::io::Result<peer::Handle>>,
    ),
    BroadcastMessage(message::Message),
    GetNewPeer(TcpStream),
    DroppedPeer(std::net::SocketAddr),
}
