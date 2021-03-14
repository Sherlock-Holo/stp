use std::convert::TryFrom;
use std::hash::Hasher;
use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc::crc16::Digest;
use crc::Hasher16;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("raw data length {0} too short")]
    TooShort(u16),
    #[error("receive crc {receive}, calculate {calculate}")]
    Crc { receive: u16, calculate: u16 },
}

#[derive(Debug, Clone)]
pub struct Packet {
    seq_number: u64,
    /// [SYN|ACK|DATA|FIN|RST|REV|REV|REV]
    r#type: u8,
    ack_number: u64,
    receive_window: u16,
    crc16: u16,
    payload_size: u16,
    payload: Bytes,
}

impl Packet {
    const SYN: u8 = 1 << 7;
    const ACK: u8 = 1 << 6;
    const DATA: u8 = 1 << 5;
    const FIN: u8 = 1 << 4;
    const RST: u8 = 1 << 3;

    pub const fn packet_header_size() -> u8 {
        (mem::size_of::<Packet>() - mem::size_of::<Bytes>()) as _
    }

    fn calculate_crc(&mut self) {
        let mut digest = Digest::new(crc::crc16::X25);

        digest.write_u64(self.seq_number);
        digest.write_u8(self.r#type);
        digest.write_u64(self.ack_number);
        digest.write_u16(self.receive_window);
        digest.write_u16(0);
        digest.write_u16(self.payload_size);
        crc::Hasher16::write(&mut digest, &self.payload);

        let crc16 = digest.sum16();

        self.crc16 = crc16;
    }

    pub fn is_syn(&self) -> bool {
        Self::SYN & self.r#type > 0
    }

    pub fn is_ack(&self) -> bool {
        Self::ACK & self.r#type > 0
    }

    pub fn is_data(&self) -> bool {
        Self::DATA & self.r#type > 0
    }

    pub fn is_fin(&self) -> bool {
        Self::FIN & self.r#type > 0
    }

    pub fn is_rst(&self) -> bool {
        Self::RST & self.r#type > 0
    }

    pub fn new_syn() -> Self {
        let mut packet = Self {
            seq_number: 0,
            r#type: Self::SYN,
            ack_number: 0,
            receive_window: 0,
            crc16: 0,
            payload_size: 0,
            payload: Default::default(),
        };

        packet.calculate_crc();

        packet
    }

    pub fn new_syn_ack(receive_window: u16) -> Self {
        let mut packet = Self {
            seq_number: 0,
            r#type: Self::SYN | Self::ACK,
            ack_number: 0,
            receive_window,
            crc16: 0,
            payload_size: 0,
            payload: Default::default(),
        };

        packet.calculate_crc();

        packet
    }

    pub fn new_ack(seq_number: u64, ack_number: u64, receive_window: u16) -> Self {
        let mut packet = Self {
            seq_number,
            r#type: Self::ACK,
            ack_number,
            receive_window,
            crc16: 0,
            payload_size: 0,
            payload: Default::default(),
        };

        packet.calculate_crc();

        packet
    }

    pub fn new_data(seq_number: u64, ack_number: u64, data: Bytes) -> Self {
        let mut packet = Self {
            seq_number,
            r#type: Self::DATA,
            ack_number,
            receive_window: 0,
            crc16: 0,
            payload_size: data.len() as _,
            payload: data,
        };

        packet.calculate_crc();

        packet
    }

    pub fn new_fin(seq_number: u64) -> Self {
        let mut packet = Self {
            seq_number,
            r#type: Self::FIN,
            ack_number: 0,
            receive_window: 0,
            crc16: 0,
            payload_size: 0,
            payload: Default::default(),
        };

        packet.calculate_crc();

        packet
    }

    pub fn new_rst() -> Self {
        let mut packet = Self {
            seq_number: 0,
            r#type: Self::RST,
            ack_number: 0,
            receive_window: 0,
            crc16: 0,
            payload_size: 0,
            payload: Default::default(),
        };

        packet.calculate_crc();

        packet
    }

    pub fn seq_number(&self) -> u64 {
        self.seq_number
    }

    pub fn ack_number(&self) -> u64 {
        self.ack_number
    }

    pub fn receive_window(&self) -> u16 {
        self.receive_window
    }

    pub fn payload_ref(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

impl TryFrom<Bytes> for Packet {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        if value.len() < Self::packet_header_size() as _ {
            return Err(Error::TooShort(value.len() as _));
        }

        let seq_number = value.get_u64();
        let r#type = value.get_u8();
        let ack_number = value.get_u64();
        let receive_window = value.get_u16();
        let crc16 = value.get_u16();
        let payload_size = value.get_u16();

        let mut digest = Digest::new(crc::crc16::X25);

        digest.write_u64(seq_number);
        digest.write_u8(r#type);
        digest.write_u64(ack_number);
        digest.write_u16(receive_window);
        digest.write_u16(0);
        digest.write_u16(payload_size);
        crc::Hasher16::write(&mut digest, &value);

        let calculate_crc16 = digest.sum16();

        if crc16 != calculate_crc16 {
            return Err(Error::Crc {
                receive: crc16,
                calculate: calculate_crc16,
            });
        }

        Ok(Self {
            seq_number,
            r#type,
            ack_number,
            receive_window,
            crc16,
            payload_size,
            payload: value,
        })
    }
}

impl From<Packet> for Bytes {
    fn from(packet: Packet) -> Self {
        let mut buffer = BytesMut::with_capacity(
            Packet::packet_header_size() as usize + packet.payload_size as usize,
        );

        buffer.put_u64(packet.seq_number);
        buffer.put_u8(packet.r#type);
        buffer.put_u64(packet.ack_number);
        buffer.put_u16(packet.receive_window);
        buffer.put_u16(packet.crc16);
        buffer.put_u16(packet.payload_size);

        buffer.put(packet.payload);

        buffer.freeze()
    }
}
