//! Chunk reassembly: collects Chunk frames by sequence and yields
//! the assembled payload when complete.

use std::collections::HashMap;

struct Assembler {
    buf: Vec<u8>,
    got: usize,
    total: usize,
}

/// Reassembler collects Chunk frames into complete payloads.
pub struct Reassembler {
    chunks: HashMap<i32, Assembler>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
        }
    }

    /// Fold one chunk into the per-sequence buffer. Returns the
    /// assembled payload when all bytes are present.
    pub fn reassemble(
        &mut self,
        seq: i32,
        total: usize,
        offset: usize,
        data: &[u8],
    ) -> Option<Vec<u8>> {
        let entry = self.chunks.entry(seq).or_insert_with(|| Assembler {
            buf: vec![0; total],
            got: 0,
            total,
        });

        if entry.total != total {
            *entry = Assembler {
                buf: vec![0; total],
                got: 0,
                total,
            };
        }

        let end = offset + data.len();
        if end > entry.buf.len() {
            return None;
        }
        entry.buf[offset..end].copy_from_slice(data);
        entry.got += data.len();

        if entry.got >= entry.total {
            self.chunks.remove(&seq).map(|a| a.buf)
        } else {
            None
        }
    }
}

impl Default for Reassembler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_chunk() {
        let mut r = Reassembler::new();
        let full = b"hello".to_vec();
        let out = r.reassemble(1, 5, 0, &full).unwrap();
        assert_eq!(out, full);
    }

    #[test]
    fn multi_chunk() {
        let mut r = Reassembler::new();
        let full: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let mut result = None;
        for off in (0..full.len()).step_by(256) {
            let end = (off + 256).min(full.len());
            if let Some(out) = r.reassemble(7, full.len(), off, &full[off..end]) {
                result = Some(out);
            }
        }
        assert_eq!(result.unwrap(), full);
    }

    #[test]
    fn drops_state_on_completion() {
        let mut r = Reassembler::new();
        r.reassemble(1, 3, 0, &[1, 2, 3]);
        let out = r.reassemble(1, 3, 0, &[1, 2, 3]).unwrap();
        assert_eq!(out, vec![1, 2, 3]);
    }
}
