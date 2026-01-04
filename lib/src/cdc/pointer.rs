use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct CdcPointer {
    magic: String,     // CDC 指针的魔数
    manifest_hash: String, // 指向 manifest 的 hash
}

impl CdcPointer {
    const MAGIC: &'static str = "CDC-V1";

    pub fn new(manifest_hash: String) -> Self {
        Self {
            magic: Self::MAGIC.to_string(),
            manifest_hash,
        }
    }

    // 序列化 CDC 指针，toml 格式，返回 Vec<u8>
    pub fn serialize(&self) -> Vec<u8> {
        toml::to_string(self)
            .expect("Failed to serialize CdcPointer to TOML")
            .into_bytes()
    }

    // 反序列化 CDC 指针，toml 格式，返回 Self
    pub fn deserialize(data: Vec<u8>) -> Result<Self, Box<dyn std::error::Error>> {
        let s = String::from_utf8(data)?;
        let pointer: Self = toml::from_str(&s)?;
        Ok(pointer)
    }
}