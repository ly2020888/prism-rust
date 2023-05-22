use super::message;

use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::trace;

pub fn new(stream: &TcpStream) -> std::io::Result<(UnboundedReceiver<Vec<u8>>, Handle)> {
    let (write_sender, write_receiver) = mpsc::unbounded_channel(); // TODO: think about the buffer size here
    let _addr = stream.peer_addr()?;
    let handle = Handle {
        write_queue: write_sender,
        _addr,
    };
    Ok((write_receiver, handle))
}

#[derive(Copy, Clone)]
pub enum Direction {
    Incoming,
    Outgoing,
}

#[derive(Clone, Debug)]
pub struct Handle {
    _addr: std::net::SocketAddr,
    write_queue: mpsc::UnboundedSender<Vec<u8>>,
}

unsafe impl Send for Handle {}

impl Handle {
    pub fn write(&mut self, msg: message::Message) {
        // TODO: return result
        let buffer = bincode::serialize(&msg).unwrap();

        if self.write_queue.send(buffer).is_err() {
            trace!("Trying to send to disconnected peer");
        }
    }
}
