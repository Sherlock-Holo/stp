use std::cmp::Ordering as CmpOrdering;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::FutureExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, Notify};
use tokio::time;

use crate::packet::Packet;

const MAX_WAIT_ACK_DURATION: Duration = Duration::from_secs(16);

#[derive(Debug)]
pub struct Control {
    seq_number: AtomicU64,
    ack_number: AtomicU64,
    send_control: SendControl,
    receive_control: ReceiveControl,
    low_layer_sender: Sender<Bytes>,
    low_layer_receive_channel: Receiver<Packet>,
}

impl Control {
    pub async fn send_loop(&self, mut ack_packet_receiver: Receiver<Packet>) -> Result<()> {
        let mut wait_for_detect_peer_window_duration = Duration::from_secs(1);

        loop {
            futures_util::select! {
                // no data to send and no ACK packet receive, wait a few seconds then detect the
                // peer window
                _ = time::sleep(wait_for_detect_peer_window_duration).fuse() => {
                    self.detect_peer_window(&mut ack_packet_receiver).await?;

                    if wait_for_detect_peer_window_duration < MAX_WAIT_ACK_DURATION {
                        wait_for_detect_peer_window_duration *= 2;
                    }
                }

                // no data to send but receive ACK packet, may receive a update window ACK packet
                ack_packet = ack_packet_receiver.recv().fuse() => {
                    let ack_packet = ack_packet.ok_or(ErrorKind::ConnectionReset)?;
                    let ack_number = ack_packet.ack_number();
                    let current_seq_number = self.seq_number.load(Ordering::SeqCst);

                    wait_for_detect_peer_window_duration = Duration::from_secs(1);

                    match ack_number.cmp(&current_seq_number) {
                        CmpOrdering::Less => continue,

                        CmpOrdering::Equal => {
                            let peer_window = ack_packet.receive_window();
                            if peer_window > 0 {
                                self.send_control.update_peer_window(peer_window).await;
                            }
                        }

                        CmpOrdering::Greater => return Err(ErrorKind::InvalidInput.into()),
                    }
                }

                // have data to send and peer window > 0
                data = self.send_control.get_send_data().fuse() => {
                    let data = data?;

                    self.send_data(data, &mut ack_packet_receiver).await?;

                    wait_for_detect_peer_window_duration = Duration::from_secs(1);
                }
            }
        }
    }

    async fn detect_peer_window(&self, ack_packet_receiver: &mut Receiver<Packet>) -> Result<()> {
        let seq_number = self.seq_number.load(Ordering::SeqCst);
        let ack_number = self.ack_number.load(Ordering::SeqCst);

        let detect_peer_window_packet = Packet::new_data(seq_number, ack_number, Bytes::new());
        let detect_peer_window_data: Bytes = detect_peer_window_packet.into();

        let mut duration = Duration::from_millis(500);

        for _ in 0..5 {
            if duration < MAX_WAIT_ACK_DURATION {
                duration *= 2;
            }

            let detect_peer_window_data = detect_peer_window_data.clone();

            self.low_layer_sender
                .send(detect_peer_window_data)
                .await
                .map_err(|_| ErrorKind::ConnectionReset)?;

            // loop to drop old ACK packet
            loop {
                match time::timeout(duration, ack_packet_receiver.recv()).await {
                    // the detect packet may loose in network
                    Err(_) => break,

                    Ok(packet) => {
                        let ack_packet = packet.ok_or(ErrorKind::ConnectionReset)?;
                        let ack_number = ack_packet.ack_number();

                        match ack_number.cmp(&seq_number) {
                            // drop old ACK packet
                            CmpOrdering::Less => continue,

                            CmpOrdering::Greater => return Err(ErrorKind::InvalidInput.into()),

                            CmpOrdering::Equal => {
                                let peer_window = ack_packet.receive_window();
                                if peer_window > 0 {
                                    self.send_control.update_peer_window(peer_window).await;
                                }

                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        return Err(ErrorKind::TimedOut.into());
    }

    async fn send_data(
        &self,
        data: Bytes,
        ack_packet_receiver: &mut Receiver<Packet>,
    ) -> Result<()> {
        let seq_number = self.seq_number.fetch_add(1, Ordering::SeqCst) + 1;
        let ack_number = self.ack_number.load(Ordering::SeqCst);

        let data_packet_data: Bytes = Packet::new_data(seq_number, ack_number, data).into();

        let mut duration = Duration::from_millis(500);

        for _ in 0..5 {
            if duration < MAX_WAIT_ACK_DURATION {
                duration *= 2;
            }

            let data_packet_data = data_packet_data.clone();

            self.low_layer_sender
                .send(data_packet_data)
                .await
                .map_err(|_| ErrorKind::ConnectionReset)?;

            // loop to drop old ACK packet
            loop {
                match time::timeout(duration, ack_packet_receiver.recv()).await {
                    // the DATA packet may loose in network
                    Err(_) => break,

                    Ok(packet) => {
                        let ack_packet = packet.ok_or(ErrorKind::ConnectionReset)?;
                        let ack_number = ack_packet.ack_number();

                        match ack_number.cmp(&seq_number) {
                            // drop old ACK packet
                            CmpOrdering::Less => continue,

                            // receive invalid ACK packet, maybe some errors occurred in peer
                            CmpOrdering::Greater => return Err(ErrorKind::InvalidInput.into()),

                            // data sent and peer received
                            CmpOrdering::Equal => {
                                self.send_control
                                    .update_peer_window(ack_packet.receive_window())
                                    .await;

                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        return Err(ErrorKind::TimedOut.into());
    }
}

impl Control {
    pub async fn receive_loop(&self, mut data_packet_receiver: Receiver<Packet>) -> Result<()> {
        loop {
            futures_util::select! {
                // receive window not 0 again, notify peer
                _ = self.receive_control.receive_window_not_zero.notified().fuse() => {
                    self.update_receive_window().await?;
                }

                // received data packet
                data_packet = data_packet_receiver.recv().fuse() => {
                    let data_packet = data_packet.ok_or(ErrorKind::ConnectionReset)?;

                    self.receive_data(data_packet).await?;
                }
            }
        }
    }

    async fn receive_data(&self, data_packet: Packet) -> Result<()> {
        let seq_number = data_packet.seq_number();
        let current_ack_number = self.ack_number.load(Ordering::SeqCst);

        match seq_number.cmp(&current_ack_number) {
            CmpOrdering::Less => Ok(()),
            CmpOrdering::Equal => {
                // when seq == ack and payload is empty, it's detect receive window packet
                let receive_window = self.receive_control.receive_window().await;
                let packet = Packet::new_ack(seq_number, current_ack_number, receive_window);

                self.low_layer_sender
                    .send(packet.into())
                    .await
                    .map_err(|_| ErrorKind::ConnectionReset)?;

                Ok(())
            }

            CmpOrdering::Greater => {
                // invalid DATA packet
                if !seq_number == current_ack_number + 1 {
                    return Err(ErrorKind::InvalidInput.into());
                }

                let payload = data_packet.into_payload();

                // None means not enough space to put in the data, peer may not recognize the
                // receive window, drop the data without ACK
                if let Some(receive_window) =
                    self.receive_control.put_received_data(payload).await?
                {
                    let ack_number = self.ack_number.fetch_add(1, Ordering::SeqCst) + 1;

                    let ack_packet = Packet::new_ack(seq_number, ack_number, receive_window);

                    self.low_layer_sender
                        .send(ack_packet.into())
                        .await
                        .map_err(|_| ErrorKind::ConnectionReset)?;
                }

                Ok(())
            }
        }
    }

    async fn update_receive_window(&self) -> Result<()> {
        let current_seq_number = self.seq_number.load(Ordering::SeqCst);
        let current_ack_number = self.ack_number.load(Ordering::SeqCst);
        let receive_window = self.receive_control.receive_window().await;

        let ack_packet = Packet::new_ack(current_seq_number, current_ack_number, receive_window);

        self.low_layer_sender
            .send(ack_packet.into())
            .await
            .map_err(|_| ErrorKind::ConnectionReset)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct SendControl {
    inner: Mutex<InnerSendControl>,
    can_send_notify: Notify,
    can_get_send_data_notify: Notify,
}

#[derive(Debug)]
struct InnerSendControl {
    buffer: BytesMut,
    buffer_size: u16,
    peer_window: u16,
    error: Option<ErrorKind>,
}

impl SendControl {
    pub async fn send(&self, data: &[u8]) -> Result<usize> {
        loop {
            let mut inner = self.inner.lock().await;

            if let Some(err_kind) = inner.error.take() {
                return Err(Error::from(err_kind));
            }

            if inner.buffer.len() == inner.buffer_size as _ {
                drop(inner);

                self.can_send_notify.notified().await;

                continue;
            }

            let can_write_len = inner.buffer.len().min(data.len());

            inner.buffer.put(&data[..can_write_len]);

            if inner.peer_window > 0 {
                self.can_get_send_data_notify.notify_one();
            }

            return Ok(can_write_len);
        }
    }

    /// if receive window is 0, return None. if buffer is empty, will wait until buffer has data
    pub async fn get_send_data(&self) -> Result<Bytes> {
        loop {
            let mut inner = self.inner.lock().await;

            if let Some(err_kind) = inner.error.take() {
                return Err(err_kind.into());
            }

            if inner.peer_window == 0 || inner.buffer.is_empty() {
                drop(inner);

                self.can_get_send_data_notify.notified().await;

                continue;
            }

            let can_send_data_size = inner.buffer.len().min(inner.peer_window as _);
            let data = inner.buffer.copy_to_bytes(can_send_data_size);

            self.can_send_notify.notify_one();

            return Ok(data);
        }
    }

    pub async fn update_peer_window(&self, peer_window: u16) {
        let mut inner = self.inner.lock().await;
        inner.peer_window = peer_window;

        // user can send data, the send control can send data to peer
        if peer_window > 0 {
            self.can_send_notify.notify_one();
            self.can_get_send_data_notify.notify_one();
        }
    }
}

#[derive(Debug)]
pub struct ReceiveControl {
    inner: Mutex<InnerReceiveControl>,
    can_receive_notify: Notify,
    receive_window_not_zero: Notify,
}

#[derive(Debug)]
struct InnerReceiveControl {
    buffer: BytesMut,
    buffer_size: u16,
    error: Option<ErrorKind>,
}

impl ReceiveControl {
    pub async fn receive(&self, data: &mut [u8]) -> Result<usize> {
        loop {
            let mut inner = self.inner.lock().await;

            if let Some(err_kind) = inner.error.take() {
                return Err(err_kind.into());
            }

            if inner.buffer.is_empty() {
                drop(inner);

                self.can_receive_notify.notified().await;

                continue;
            }

            let can_receive_size = inner.buffer.len().min(data.len());
            let old_receive_window = inner.receive_window();

            inner.buffer.copy_to_slice(&mut data[..can_receive_size]);

            if old_receive_window == 0 {
                self.receive_window_not_zero.notify_one();
            }

            return Ok(can_receive_size);
        }
    }

    /// if buffer is not large enough, will return false and drop data, the control should return
    /// the last ack number and the receive window
    pub async fn put_received_data(&self, data: Bytes) -> Result<Option<u16>> {
        let mut inner = self.inner.lock().await;

        if let Some(err_kind) = inner.error.take() {
            return Err(err_kind.into());
        }

        let can_put_size = inner.buffer_size as usize - inner.buffer.len();

        if can_put_size < data.len() {
            return Ok(None);
        }

        inner.buffer.put(data);

        self.can_receive_notify.notify_one();

        Ok(Some(inner.receive_window()))
    }

    pub async fn receive_window(&self) -> u16 {
        self.inner.lock().await.receive_window()
    }
}

impl InnerReceiveControl {
    fn receive_window(&self) -> u16 {
        self.buffer_size - self.buffer.len() as u16
    }
}
