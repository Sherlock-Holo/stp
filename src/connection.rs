use std::io::Result;

use tokio::net::ToSocketAddrs;

use crate::control::Control;

#[derive(Debug)]
pub struct Connection {
    control: Control,
}

impl Connection {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let control = Control::connect(addr).await?;

        Ok(Self { control })
    }

    pub async fn write(&self, buf: &[u8]) -> Result<usize> {
        self.control.write(buf).await
    }

    pub async fn read(&self, buf: &mut [u8]) -> Result<usize> {
        self.control.read(buf).await
    }
}
