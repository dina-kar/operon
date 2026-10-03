//! Artifacts: screenshots and extracted text, written where the caller can find
//! them and never inlined into a tool result (D504 rule 7, reference over
//! value).

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::BridgeError;
use crate::page::PageState;
use crate::tool::{Extracted, Screenshot};

/// Write `bytes` under `dir` with a timestamp-prefixed name, and describe the
/// file. The name never contains page content.
pub fn write(
    dir: &Path,
    prefix: &str,
    extension: &str,
    bytes: &[u8],
) -> Result<Screenshot, BridgeError> {
    std::fs::create_dir_all(dir)?;
    let digest = Sha256::digest(bytes);
    let name = format!("{prefix}-{}.{extension}", hex(&digest[..8]));
    let path: PathBuf = dir.join(name);
    std::fs::write(&path, bytes)?;
    Ok(Screenshot {
        path,
        bytes: bytes.len(),
        sha256: hex(&digest),
    })
}

/// Turn a page state into an extraction, cut at `budget` characters.
pub fn extracted(state: &PageState, budget: usize) -> Extracted {
    let text = state.text.trim();
    let truncated = text.chars().count() > budget;
    let kept: String = text.chars().take(budget).collect();
    Extracted {
        title: state.title.clone(),
        url: crate::tool::redact_url(&state.url),
        text: kept,
        truncated,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
