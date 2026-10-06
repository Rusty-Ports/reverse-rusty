//! The messages one shard's bulk bucket is sent as (ADR-193).
//!
//! A bucket holds every query bound for one shard, so its encoded size grows with the
//! corpus. gRPC refuses an inbound message above the server's limit (4 MiB unless raised),
//! and one request per bucket stopped working once a bucket passed it. The bucket travels on
//! the `StageIngest` stream instead, as consecutive messages of bounded size. (A remote
//! resize bounds its own batches before it sends them.)

use reverse_rusty_shard_proto::encoded_len;

use super::super::proto;
use super::super::shard::ShardError;
use super::refuse_wire_tag_ids;
use crate::segment::PlacedQuery;

/// A batch as the wire carries it: raw DSL and raw tags, which the node compiles against its
/// own frozen dictionary.
pub(super) fn wire_items(items: &[PlacedQuery]) -> Vec<proto::AddItem> {
    items
        .iter()
        .map(|q| proto::AddItem {
            logical_id: q.logical,
            dsl: q.dsl.clone(),
            version: q.version,
            tags: proto::tags_to_proto(&q.tags),
            placement: Some(proto::placement_to_proto(&q.placement)),
        })
        .collect()
}

/// The messages a bucket travels as: in order, each within
/// [`crate::cluster::INGEST_REQUEST_BUDGET_BYTES`]. An empty bucket is one empty message, so
/// the node still checks that it serves the slot.
pub(super) fn bounded_requests(
    items: &[PlacedQuery],
) -> Result<Vec<Vec<proto::AddItem>>, ShardError> {
    refuse_wire_tag_ids(items)?;
    let mut requests = split_by_encoded_size(
        wire_items(items),
        crate::cluster::INGEST_REQUEST_BUDGET_BYTES,
    )?;
    if requests.is_empty() {
        requests.push(Vec::new());
    }
    Ok(requests)
}

/// What one item costs inside `IngestRequest.items` beyond its own bytes: the field tag and
/// a length prefix of up to five bytes.
const ITEM_FRAMING_BYTES: usize = 1 + 5;

/// What a request costs beyond its items: the `shard_id` field's tag and a varint of up to
/// five bytes.
const REQUEST_ENVELOPE_BYTES: usize = 1 + 5;

/// Split `items` into consecutive groups, each of which encodes, as a whole request, to at
/// most `budget` bytes. Order is preserved, so applying the groups in turn is applying the
/// bucket. An item that exceeds the budget on its own cannot be sent in any group, and fails
/// here with its id.
fn split_by_encoded_size(
    items: Vec<proto::AddItem>,
    budget: usize,
) -> Result<Vec<Vec<proto::AddItem>>, ShardError> {
    let request_bytes = budget;
    let budget = budget.saturating_sub(REQUEST_ENVELOPE_BYTES);
    let mut groups: Vec<Vec<proto::AddItem>> = Vec::new();
    let mut current: Vec<proto::AddItem> = Vec::new();
    let mut current_bytes = 0usize;
    for item in items {
        let item_bytes = encoded_len(&item) + ITEM_FRAMING_BYTES;
        if item_bytes > budget {
            return Err(ShardError::Config(format!(
                "query {} encodes to {item_bytes} bytes, more than one bulk-ingest request \
                 may carry ({request_bytes} bytes); it cannot be loaded in bulk",
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

#[cfg(test)]
mod tests {
    use super::{bounded_requests, split_by_encoded_size, ITEM_FRAMING_BYTES};
    use crate::cluster::proto;
    use crate::segment::PlacedQuery;
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
        let groups =
            split_by_encoded_size(items, one * 4 + super::REQUEST_ENVELOPE_BYTES).expect("split");
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

    fn placed(logical: u64, dsl: &str, tag_ids: Vec<crate::tagdict::TagId>) -> PlacedQuery {
        let norm = crate::normalize::Normalizer::default_vocab().expect("vocab");
        let mut dict = crate::dict::Dict::new();
        let mut lc = String::new();
        let ast = crate::dsl::parse("1994 north star").expect("parse");
        PlacedQuery {
            logical,
            // The wire carries the raw query; the compiled form stays with the coordinator.
            ex: crate::compile::extract(&ast, &norm, &mut dict, &mut lc),
            dsl: dsl.to_string(),
            version: 3,
            source_generation: None,
            tags: vec![("tier".into(), "gold".into())],
            tag_ids,
            rank: crate::rank::RankValues::default(),
            placement: crate::ownership::QueryPlacement::standalone(),
        }
    }

    #[test]
    fn a_batch_travels_as_raw_queries_in_order() {
        let batch = [
            placed(7, "+nike", Vec::new()),
            placed(9, "+sony", Vec::new()),
        ];
        let requests = bounded_requests(&batch).expect("requests");
        assert_eq!(requests.len(), 1);
        let sent: Vec<(u64, &str, u32)> = requests[0]
            .iter()
            .map(|item| (item.logical_id, item.dsl.as_str(), item.version))
            .collect();
        assert_eq!(sent, vec![(7, "+nike", 3), (9, "+sony", 3)]);
        assert!(requests[0]
            .iter()
            .all(|item| item.tags.len() == 1 && item.placement.is_some()));
    }

    #[test]
    fn an_empty_batch_is_one_empty_message() {
        let requests = bounded_requests(&[]).expect("requests");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].is_empty());
    }

    #[test]
    fn a_batch_over_the_budget_is_several_messages_that_each_fit() {
        let dsl = "x".repeat(8_000);
        let batch: Vec<PlacedQuery> = (0..1_000).map(|id| placed(id, &dsl, Vec::new())).collect();
        let requests = bounded_requests(&batch).expect("requests");
        assert!(requests.len() > 1, "8 MB cannot travel as one message");
        for request in &requests {
            assert!(request_bytes(request) <= crate::cluster::INGEST_REQUEST_BUDGET_BYTES);
        }
        let ids: Vec<u64> = requests.iter().flatten().map(|i| i.logical_id).collect();
        assert_eq!(ids, (0..1_000).collect::<Vec<u64>>());
    }

    #[test]
    fn pre_resolved_tag_ids_never_reach_the_wire() {
        let error = bounded_requests(&[placed(
            1,
            "+nike",
            vec![crate::tagdict::synthetic_tag_id("region", "emea")],
        )])
        .expect_err("refused");
        assert!(error.to_string().contains("tag ids"), "{error}");
    }

    /// The budget is for the request the node decodes, so the largest item it admits has to
    /// leave room for the request's own field.
    #[test]
    fn the_largest_item_that_fits_still_fits_as_a_whole_request() {
        let budget = 1_000;
        let (dsl_len, groups) = (0..=budget)
            .rev()
            .find_map(|dsl_len| {
                split_by_encoded_size(vec![item(1, dsl_len)], budget)
                    .ok()
                    .map(|groups| (dsl_len, groups))
            })
            .expect("some item fits");
        assert!(
            split_by_encoded_size(vec![item(1, dsl_len + 1)], budget).is_err(),
            "this is the largest item the budget admits"
        );
        let encoded = request_bytes(&groups[0]);
        assert!(
            encoded <= budget,
            "a request of {encoded} bytes exceeds the {budget}-byte budget"
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
