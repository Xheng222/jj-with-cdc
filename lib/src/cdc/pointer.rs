#![expect(missing_docs)]

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

pub type CdcPointerBytes = Vec<u8>;

#[derive(Serialize, Deserialize, Debug)]
pub struct CdcPointer {
    manifest_hash: String, // 指向 manifest 的 hash
}

/// try_parse 的结果类型
pub enum TryParseResult {
    /// 成功解析出 CDC 指针
    Parsed(CdcPointer),
    /// 不是 CDC 指针，返回已读取的字节（用于恢复原始数据流）
    NotCdcPointer(Vec<u8>),
}

impl CdcPointer {
    const MAGIC: &[u8] = b"\x00CDC\x00";  // 二进制魔数

    pub fn new(manifest_hash: String) -> Self {
        Self {
            manifest_hash,
        }
    }

    pub fn hash(&self) -> &String {
        &self.manifest_hash
    }

    // 序列化 CDC 指针，返回 CdcPointerBytes
    pub fn serialize(self) -> CdcPointerBytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(Self::MAGIC);
        buf.extend_from_slice(self.manifest_hash.as_bytes());
        buf
    }

    /// 尝试解析 CDC 指针
    /// 
    /// # 返回值
    /// - `Ok(TryParseResult::Parsed(pointer))` - 成功解析出 CDC 指针
    /// - `Ok(TryParseResult::NotCdcPointer(bytes))` - 不是 CDC 指针，返回已读取的字节
    /// - `Err(e)` - 是 CDC 指针但解析失败，或读取错误
    pub async fn try_parse<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<TryParseResult> 
    {
        // 尝试读取魔数
        let mut magic_buf = [0; Self::MAGIC.len()];
        match reader.read(&mut magic_buf).await {
            Ok(n) => {
                // 读取到的字节数不等于魔数，不是 CDC 指针
                if magic_buf != Self::MAGIC {
                    return Ok(TryParseResult::NotCdcPointer(magic_buf[..n].to_vec()));
                }
            }
            Err(e) => {
                // 真正的读取错误
                debug!("Failed to read magic number: {}", e);
                return Err(e);
            }
        }

        // 魔数匹配，这应该是一个 CDC 指针
        let mut hash_bytes = Vec::new();
        reader.read_to_end(&mut hash_bytes).await?;
        
        // 解析 hash
        let hash = String::from_utf8(hash_bytes).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("CDC pointer contains invalid UTF-8 hash: {}", e)
            )
        })?;

        Ok(TryParseResult::Parsed(Self::new(hash)))
    }

    pub fn is_cdc_pointer(bytes: &[u8]) -> bool {
        bytes.starts_with(Self::MAGIC)
    }









}