use super::{CollectError, LiveSourceCollector, LiveSourceRow};
use crate::cluster::proto;

fn request() -> proto::LiveSourcesRequest {
    proto::LiveSourcesRequest {
        shard_id: 2,
        dict_fingerprint: 0,
        tag_dict_fingerprint: 0,
        placement_generation: 5,
        num_shards: 4,
        max_documents: 10,
        remaining_micros: 1,
    }
}

fn frame(ids: &[u64], total: u64, complete: bool) -> proto::LiveSourcesFrame {
    proto::LiveSourcesFrame {
        documents: ids
            .iter()
            .map(|&id| proto::LiveSource {
                logical_id: id,
                dsl: format!("q{id}"),
                version: 1,
                tags: vec![proto::TagKv {
                    key: "k".into(),
                    value: id.to_string(),
                }],
            })
            .collect(),
        total_documents: total,
        complete,
        shard_id: 2,
        placement_generation: 5,
        num_shards: 4,
    }
}

fn run(frames: Vec<proto::LiveSourcesFrame>) -> Result<Vec<LiveSourceRow>, String> {
    let mut collector = LiveSourceCollector::new(&request());
    let mut rows = Vec::new();
    for f in frames {
        collector
            .push(f, |row| {
                rows.push(row);
                Ok::<(), ()>(())
            })
            .map_err(|e| match e {
                CollectError::Wire(status) => status.message().to_string(),
                CollectError::Visit(()) => "visit".to_string(),
            })?;
    }
    collector
        .finish()
        .map_err(|status| status.message().to_string())?;
    Ok(rows)
}

#[test]
fn a_complete_ordered_export_delivers_every_document() {
    let rows = run(vec![
        frame(&[1, 4], 3, false),
        frame(&[9], 3, false),
        frame(&[], 3, true),
    ])
    .expect("valid export");
    let ids: Vec<u64> = rows.iter().map(|r| r.0).collect();
    assert_eq!(ids, [1, 4, 9]);
    assert_eq!(rows[2].1, "q9");
    assert_eq!(rows[2].3, vec![("k".to_string(), "9".to_string())]);
}

#[test]
fn an_empty_slot_completes_with_zero_documents() {
    assert!(run(vec![frame(&[], 0, true)]).expect("empty").is_empty());
}

#[test]
fn every_protocol_violation_fails_loud() {
    let mut wrong_identity = frame(&[1], 1, false);
    wrong_identity.placement_generation = 6;
    let cases = vec![
        ("no completion", vec![frame(&[1], 1, false)]),
        (
            "short completion",
            vec![frame(&[1], 2, false), frame(&[], 2, true)],
        ),
        ("over count", vec![frame(&[1, 2], 1, false)]),
        (
            "count changed",
            vec![frame(&[1], 2, false), frame(&[2], 3, false)],
        ),
        ("unordered", vec![frame(&[2, 1], 2, false)]),
        (
            "duplicate across frames",
            vec![frame(&[1], 2, false), frame(&[1], 2, false)],
        ),
        ("empty data frame", vec![frame(&[], 1, false)]),
        ("over limit", vec![frame(&[1], 11, false)]),
        ("identity", vec![wrong_identity]),
        (
            "after completion",
            vec![frame(&[], 0, true), frame(&[], 0, true)],
        ),
        ("documents on completion", vec![frame(&[1], 1, true)]),
    ];
    for (name, frames) in cases {
        assert!(run(frames).is_err(), "{name} must fail");
    }
}

#[test]
fn a_visitor_refusal_stops_the_export() {
    let mut collector = LiveSourceCollector::new(&request());
    let result = collector.push(frame(&[1, 2], 2, false), |row| {
        if row.0 == 2 {
            Err("refused")
        } else {
            Ok(())
        }
    });
    assert!(matches!(result, Err(CollectError::Visit("refused"))));
}
