use super::*;

fn frame(body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_frame(&mut bytes, body).expect("encode frame");
    bytes
}

#[test]
fn only_incomplete_final_records_are_repairable_with_exact_boundaries() {
    let prefix = frame(b"first committed frame");
    let next = frame(b"second pending frame");
    for cut in [1, 4, 7, 8, 12, next.len() - 1] {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&next[..cut]);
        let scan = scan_records(&bytes, 0, |b| Ok(b.to_vec())).expect("torn final frame");
        assert_eq!(scan.records, vec![b"first committed frame".to_vec()]);
        assert_eq!(scan.valid_len, prefix.len(), "cut {cut}");
        assert_eq!(scan.torn_bytes, cut, "cut {cut}");
    }
}

#[test]
fn zero_padding_at_end_is_counted_and_damaged_prefix_cannot_hide_valid_records() {
    let prefix = frame(b"committed");
    let mut padded = prefix.clone();
    padded.extend_from_slice(&[0; 1024]);
    let scan = scan_records(&padded, 0, |b| Ok(b.to_vec())).expect("zero tail");
    assert_eq!(scan.valid_len, prefix.len());
    assert_eq!(scan.torn_bytes, 1024);

    let mut hidden = prefix;
    hidden.extend_from_slice(&[3, 0, 0]);
    hidden.extend_from_slice(&frame(b"later acknowledged record"));
    let error = scan_records(&hidden, 0, |b| Ok(b.to_vec()))
        .err()
        .expect("refuse hidden ack");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn complete_crc_and_decode_errors_are_not_torn_tails() {
    let mut corrupt = frame(b"first");
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    corrupt.extend_from_slice(&frame(b"later"));
    assert_eq!(
        scan_records(&corrupt, 0, |_| Ok(()))
            .err()
            .expect("CRC failure")
            .kind(),
        io::ErrorKind::InvalidData
    );
    let encoded = frame(b"unknown payload");
    assert_eq!(
        scan_records(&encoded, 0, |_| Err::<(), _>(invalid("unknown operation")))
            .err()
            .expect("decode failure")
            .kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn damaged_length_of_last_complete_frame_is_not_a_torn_write() {
    for suffix in [vec![], vec![0; 64], vec![32, 0, 0]] {
        let mut bytes = frame(b"last acknowledged record");
        bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&suffix);
        assert_eq!(
            scan_records(&bytes, 0, |body| Ok(body.to_vec()))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}

#[test]
fn repair_then_append_survives_another_reopen() {
    let path = std::env::temp_dir().join(format!("rr_framed_repair_{}", std::process::id()));
    let prefix = frame(b"first");
    let mut damaged = prefix.clone();
    damaged.extend_from_slice(&[3, 0, 0]);
    std::fs::write(&path, &damaged).expect("plant torn tail");
    let scan = scan_records(&damaged, 0, |b| Ok(b.to_vec())).expect("scan tail");
    repair_tail(&path, damaged.len(), scan.valid_len).expect("sync repair");
    let file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append handle");
    LogAppender::new(file)
        .append(b"second", true)
        .expect("ack second frame");
    let reopened = std::fs::read(&path).expect("reopen");
    let scan = scan_records(&reopened, 0, |b| Ok(b.to_vec())).expect("whole log");
    assert_eq!(scan.records, vec![b"first".to_vec(), b"second".to_vec()]);
    assert_eq!(scan.torn_bytes, 0);
    std::fs::remove_file(path).expect("cleanup");
}

struct PartialWriter {
    bytes: Vec<u8>,
    writes: usize,
}

impl Write for PartialWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        let room = 13 - self.bytes.len();
        if room == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "injected disk fault",
            ));
        }
        let count = room.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_partial_write_disables_later_appends() {
    let mut appender = LogAppender::new(PartialWriter {
        bytes: Vec::new(),
        writes: 0,
    });
    assert!(appender
        .append_with(b"first pending record", |_| Ok(()))
        .is_err());
    let writes = appender.writer.writes;
    let bytes = appender.writer.bytes.clone();
    assert!(appender.append_with(b"later record", |_| Ok(())).is_err());
    assert_eq!(
        appender.writer.writes, writes,
        "no writes after a partial failure"
    );
    assert_eq!(appender.writer.bytes, bytes);
}

#[test]
fn a_sync_failure_disables_later_appends_even_when_the_frame_is_complete() {
    let mut appender = LogAppender::new(Vec::new());
    assert!(appender
        .append_with(b"ambiguous write", |_| Err(io::Error::other("fsync fault")))
        .is_err());
    let bytes = appender.writer.clone();
    assert!(appender.append_with(b"later ack", |_| Ok(())).is_err());
    assert_eq!(appender.writer, bytes);
    let scan = scan_records(&bytes, 0, |b| Ok(b.to_vec())).expect("complete ambiguous frame");
    assert_eq!(scan.records.len(), 1);
}
