pub use client::ClientLowLayer;

mod client {
    use std::convert::TryFrom;
    use std::io::{ErrorKind, Result};

    use bytes::{Buf, Bytes, BytesMut};
    use tokio::net::UdpSocket;
    use tokio::sync::mpsc::{Receiver, Sender};

    use crate::packet::Packet;
    use crate::UDP_PAYLOAD_SIZE;

    #[derive(Debug)]
    pub struct ClientLowLayer {
        udp_socket: UdpSocket,
        data_packet_sender: Sender<Packet>,
        ack_packet_sender: Sender<Packet>,
    }

    impl ClientLowLayer {
        pub fn new(
            udp_socket: UdpSocket,
            data_packet_sender: Sender<Packet>,
            ack_packet_sender: Sender<Packet>,
        ) -> Self {
            Self {
                udp_socket,
                data_packet_sender,
                ack_packet_sender,
            }
        }

        pub async fn send_loop(&self, mut packet_data_receiver: Receiver<Bytes>) -> Result<()> {
            while let Some(packet_data) = packet_data_receiver.recv().await {
                self.udp_socket.send(&packet_data).await?;
            }

            Ok(())
        }

        pub async fn receive_loop(&self) -> Result<()> {
            let mut buffer = BytesMut::with_capacity(UDP_PAYLOAD_SIZE as _);

            loop {
                // safety: udp_socket.recv will overwrite the not initialized data
                unsafe {
                    buffer.set_len(UDP_PAYLOAD_SIZE as _);
                }

                let n = self.udp_socket.recv(&mut buffer).await?;
                let udp_data = buffer.copy_to_bytes(n);

                if let Ok(packet) = Packet::try_from(udp_data) {
                    if packet.is_data() {
                        self.data_packet_sender
                            .send(packet)
                            .await
                            .map_err(|_| ErrorKind::ConnectionReset)?;
                    } else if packet.is_ack() {
                        self.ack_packet_sender
                            .send(packet)
                            .await
                            .map_err(|_| ErrorKind::ConnectionReset)?;
                    }
                }
            }
        }
    }
}
