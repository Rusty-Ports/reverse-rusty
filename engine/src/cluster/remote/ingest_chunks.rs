//! Splitting one shard's bulk bucket into requests a shard node accepts (ADR-193).
//!
//! A bucket holds every query bound for one shard, so its encoded size grows with the
//! corpus. gRPC refuses an inbound message above the server's limit (4 MiB unless raised),
//! and one request per bucket stopped working once a bucket passed it. The coordinator
//! sends the bucket as consecutive requests of bounded size instead.

use reverse_rusty_shard_proto::encoded_len;

use super::super::proto;
use super::super::shard::ShardError;
use crate::segment::IngestReport;

/// What one item costs inside `IngestRequest.items` beyond its own bytes: the field tag and
/// a length prefix of up to five bytes.
const ITEM_FRAMING_BYTES: usize = 1 + 5;

/// Split `items` into consecutive groups whose encoded size stays within `budget`. Order is
/// preserved, so applying the groups in turn is applying the bucket. An item that exceeds
/// the budget on its own cannot be sent in any group, and fails here with its id.
pub(super) fn split_by_encoded_size(
    items: Vec<proto::AddItem>,
    budget: usize,
) -> Result<Vec<Vec<proto::AddItem>>, ShardError> {
    let mut groups: Vec<Vec<proto::AddItem>> = Vec::new();
    let mut current: Vec<proto::AddItem> = Vec::new();
    let mut current_bytes = 0usize;
    for item in items {
        let item_bytes = encoded_len(&item) + ITEM_FRAMING_BYTES;
        if item_bytes > budget {
            return Err(ShardError::Config(format!(
                "query {} encodes to {item_bytes} bytes, more than one bulk-ingest request \
                 may carry ({budget} bytes); it cannot be loaded in bulk",
                item.logical_id
            )));
        }
        if current_bytes + item_bytes > budget {
            groups.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += item_bytes;
        current.push(item);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

/// Add one request's reply to the bucket's report: the bucket's outcome is the sum of its
/// requests' outcomes.
pub(super) fn add_reply(report: &mut IngestReport, reply: &proto::IngestReply) {
    report.ingested += reply.ingested as usize;
    report.rejected_parse += reply.rejected_parse as usize;
    report.rejected_class_d += reply.rejected_class_d as usize;
}

#[cfg(test)]
mod tests {
    use super::{add_reply, split_by_encoded_size, ITEM_FRAMING_BYTES};
    use crate::cluster::proto;
    use reverse_rusty_shard_proto::encoded_len;

    fn item(id: u64, dsl_len: usize) -> proto::AddItem {
        proto::AddItem {
            logical_id: id,
            dsl: "x".repeat(dsl_len),
            version: 1,
            tags: Vec::new(),
            placement: None,
        }
    }

    fn request_bytes(items: &[proto::AddItem]) -> usize {
        encoded_len(&proto::IngestRequest {
            items: items.to_vec(),
            shard_id: u32::MAX,
        })
    }

    #[test]
    fn every_group_fits_and_the_order_is_kept() {
        let items: Vec<_> = (0..500)
            .map(|id| item(id, 40 + (id as usize % 90)))
            .collect();
        let budget = 2_000;
        let groups = split_by_encoded_size(items.clone(), budget).expect("split");
        assert!(groups.len() > 1, "the bucket needs several requests");
        for group in &groups {
            assert!(!group.is_empty());
            assert!(
                request_bytes(group) <= budget,
                "a request of {} bytes exceeds the {budget}-byte budget",
                request_bytes(group)
            );
        }
        let rejoined: Vec<u64> = groups.iter().flatten().map(|i| i.logical_id).collect();
        let original: Vec<u64> = items.iter().map(|i| i.logical_id).collect();
        assert_eq!(rejoined, original);
    }

    #[test]
    fn groups_are_filled_before_a_new_one_starts() {
        let one = encoded_len(&item(1, 100)) + ITEM_FRAMING_BYTES;
        let items: Vec<_> = (0..10).map(|id| item(id, 100)).collect();
        let groups = split_by_encoded_size(items, one * 4).expect("split");
        let sizes: Vec<usize> = groups.iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![4, 4, 2]);
    }

    #[test]
    fn a_small_bucket_is_one_request_and_an_empty_one_is_none() {
        let groups = split_by_encoded_size(vec![item(1, 10), item(2, 10)], 1_000).expect("split");
        assert_eq!(groups.len(), 1);
        assert!(split_by_encoded_size(Vec::new(), 1_000)
            .expect("split")
            .is_empty());
    }

    #[test]
    fn a_buckets_report_is_the_sum_of_its_requests() {
        let mut report = crate::segment::IngestReport {
            ingested: 0,
            rejected_parse: 0,
            rejected_class_d: 0,
        };
        for (ingested, rejected_parse, rejected_class_d) in [(10, 1, 0), (7, 0, 2), (3, 4, 5)] {
            add_reply(
                &mut report,
                &proto::IngestReply {
                    ingested,
                    rejected_parse,
                    rejected_class_d,
                },
            );
        }
        assert_eq!(
            (
                report.ingested,
                report.rejected_parse,
                report.rejected_class_d
            ),
            (20, 5, 7)
        );
    }

    #[test]
    fn an_item_larger_than_the_budget_is_refused_by_id() {
        let error = split_by_encoded_size(vec![item(1, 10), item(77, 5_000)], 1_000)
            .expect_err("the item cannot be sent");
        let message = error.to_string();
        assert!(
            message.contains("query 77") && message.contains("1000"),
            "{message}"
        );
    }
}
