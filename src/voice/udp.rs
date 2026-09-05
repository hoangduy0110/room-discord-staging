use std::{net::SocketAddr, time::Duration};
use tokio::net::UdpSocket;

const DISCOVERY_LEN: usize = 74;

pub struct DiscoveryResult {
    pub external_ip: String,
    pub external_port: u16,
}

pub async fn discover(ssrc: u32, ip: &str, port: u16) -> anyhow::Result<DiscoveryResult> {
    let target: SocketAddr = format!("{ip}:{port}").parse()?;
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(target).await?;

    let mut packet = [0u8; DISCOVERY_LEN];
    packet[0..2].copy_from_slice(&1u16.to_be_bytes());
    packet[2..4].copy_from_slice(&70u16.to_be_bytes());
    packet[4..8].copy_from_slice(&ssrc.to_be_bytes());
    socket.send(&packet).await?;

    let mut buf = [0u8; DISCOVERY_LEN];
    let n = tokio::time::timeout(Duration::from_secs(5), socket.recv(&mut buf)).await??;
    if n < DISCOVERY_LEN { anyhow::bail!("short UDP discovery reply: {n}"); }
    let addr_end = buf[8..72].iter().position(|b| *b == 0).unwrap_or(64);
    let external_ip = std::str::from_utf8(&buf[8..8 + addr_end])?.to_string();
    let external_port = u16::from_be_bytes([buf[72], buf[73]]);
    Ok(DiscoveryResult { external_ip, external_port })
}
