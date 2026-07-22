use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolFrame<T> {
    pub protocol_version: u16,
    pub payload: T,
}

impl<T> ProtocolFrame<T> {
    pub fn v1(payload: T) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Register {
        instance_id: Uuid,
        queue_name: String,
        running_jobs: u32,
        free_slots: u32,
    },
    /// Sent by Maqistor to the single registered worker queue.
    JobDispatch {
        job_id: i64,
        dispatch_id: String,
        execution_count: u32,
        payload: Vec<u8>,
    },
    /// Sent by a worker after the dispatched job finishes.
    JobResult {
        job_id: i64,
        dispatch_id: String,
        result: JobResult,
        running_jobs: u32,
        free_slots: u32,
    },
    Heartbeat,
    Registered {
        queue_name: String,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum JobResult {
    Succeeded { payload: Vec<u8> },
    Failed { message: String },
}

pub type WireFrame = ProtocolFrame<WorkerMessage>;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
    #[error("frame exceeds {MAX_FRAME_BYTES} byte limit")]
    FrameTooLarge,
    #[error("malformed frame: {0}")]
    Malformed(#[from] serde_cbor::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub fn encode_frame(frame: &WireFrame) -> Result<Vec<u8>, ProtocolError> {
    if frame.protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(frame.protocol_version));
    }
    let body = serde_cbor::to_vec(frame)?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let mut result = Vec::with_capacity(4 + body.len());
    result.extend_from_slice(&(body.len() as u32).to_be_bytes());
    result.extend_from_slice(&body);
    Ok(result)
}

pub fn decode_frame(bytes: &[u8]) -> Result<WireFrame, ProtocolError> {
    if bytes.len() < 4 {
        return Err(ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing frame length",
        )));
    }
    let len = u32::from_be_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    if bytes.len() != len + 4 {
        return Err(ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame length mismatch",
        )));
    }
    let frame: WireFrame = serde_cbor::from_slice(&bytes[4..])?;
    if frame.protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(frame.protocol_version));
    }
    Ok(frame)
}

pub fn write_frame<W: Write>(writer: &mut W, frame: &WireFrame) -> Result<(), ProtocolError> {
    writer.write_all(&encode_frame(frame)?)?;
    Ok(())
}
pub fn read_frame<R: Read>(reader: &mut R) -> Result<WireFrame, ProtocolError> {
    let mut len = [0; 4];
    reader.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let mut body = vec![0; len];
    reader.read_exact(&mut body)?;
    let mut bytes = len.to_be_bytes().to_vec();
    bytes.extend(body);
    decode_frame(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip() {
        let frame = WireFrame::v1(WorkerMessage::Heartbeat);
        assert_eq!(decode_frame(&encode_frame(&frame).unwrap()).unwrap(), frame);
    }
}
