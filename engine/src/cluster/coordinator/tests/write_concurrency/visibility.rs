//! ADR-185 (RR-004): what an unfenced reader sees while a cluster upsert is stopped at each
//! of its shard calls. The old funnel tombstoned the id everywhere and then inserted it, so
//! a reader between those passes saw neither version.

use super::*;

/// Every read an ordinary request can make for one title, with no barrier taken.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    percolate: Vec<u64>,
    ranked: Vec<u64>,
    top_k: Result<Vec<u64>, String>,
    top_k_batch: Result<Vec<u64>, String>,
    exists: bool,
}

impl Seen {
    fn exactly(id: u64) -> Self {
        Self {
            percolate: vec![id],
            ranked: vec![id],
            top_k: Ok(vec![id]),
            top_k_batch: Ok(vec![id]),
            exists: true,
        }
    }
}

fn read_all(cluster: &ClusterEngine, title: &str, id: u64) -> Seen {
    let program = cluster
        .compile_rank_program(&crate::rank::RankProgramSpec::default())
        .expect("rank program");
    let options = crate::result::TopKOptions::default();
    Seen {
        percolate: cluster
            .percolate_with_broad(title, true)
            .expect("percolate"),
        ranked: cluster
            .percolate_filtered_ranked(title, &[], true, &crate::rank::RankSpec::default())
            .expect("ranked percolate")
            .0
            .into_iter()
            .map(|(id, _)| id)
            .collect(),
        top_k: cluster
            .try_percolate_filtered_top_k(title, &[], options, &program, None)
            .map(|found| found.hits.iter().map(|hit| hit.logical_id).collect())
            .map_err(|error| error.to_string()),
        top_k_batch: cluster
            .try_percolate_filtered_top_k_batch(&[title], &[], options, &program, None)
            .map(|found| {
                found.titles[0]
                    .hits
                    .iter()
                    .map(|hit| hit.logical_id)
                    .collect()
            })
            .map_err(|error| error.to_string()),
        exists: cluster.document_exists(id).expect("point read"),
    }
}

enum Event {
    Step(usize, WriteCall),
    Done,
}

/// Stops the upsert of one logical id before each of its shard calls, until released.
struct Stepper {
    armed: Arc<AtomicBool>,
    events: mpsc::Receiver<Event>,
    done: mpsc::Sender<Event>,
    go: mpsc::Sender<()>,
}

fn step_writes(cluster: &mut ClusterEngine, logical: u64) -> Stepper {
    let (event_tx, events) = mpsc::channel();
    let (go, go_rx) = mpsc::channel::<()>();
    let go_rx = std::sync::Mutex::new(go_rx);
    let armed = Arc::new(AtomicBool::new(false));
    instrument(cluster, {
        let armed = Arc::clone(&armed);
        let event_tx = std::sync::Mutex::new(event_tx.clone());
        Arc::new(move |position, call| {
            let ours = matches!(
                call,
                WriteCall::Insert(id) | WriteCall::Delete(id) | WriteCall::Replace(id, _)
                    if id == logical
            );
            if ours && armed.load(Ordering::SeqCst) {
                let _ = event_tx
                    .lock()
                    .expect("event sender")
                    .send(Event::Step(position, call));
                // A broken test must fail, not hang: stop waiting after a bounded time.
                go_rx
                    .lock()
                    .expect("release receiver")
                    .recv_timeout(Duration::from_secs(20))
                    .map_err(|_| ShardError::Remote("the stepper was never released".into()))?;
            }
            Ok(())
        })
    });
    Stepper {
        armed,
        events,
        done: event_tx,
        go,
    }
}

/// One observation: the shard call the writer was stopped at, whether the reader had to
/// wait for the writer to move on, and what it finally saw.
#[derive(Debug)]
struct Observation {
    /// Diagnostic only: printed by the assertion messages below.
    #[allow(dead_code)]
    position: usize,
    call: WriteCall,
    waited: bool,
    /// A point read taken while the writer was stopped. Point reads take no fence, so
    /// this samples the half-done state directly.
    exists_now: bool,
    seen: Seen,
}

/// Run `write` with the stepper armed and read at every stop. A reader that does not
/// return promptly is waiting out a fenced move; the writer is then released and the
/// reader's eventual answer recorded. Nothing is asserted until every thread has finished.
fn observe(
    cluster: &ClusterEngine,
    stepper: &Stepper,
    title: &str,
    id: u64,
    write: impl FnOnce() -> Result<(usize, AddOutcome), ShardError> + Send,
) -> (Vec<Observation>, Result<(usize, AddOutcome), ShardError>) {
    std::thread::scope(|scope| {
        stepper.armed.store(true, Ordering::SeqCst);
        let done = stepper.done.clone();
        let writer = scope.spawn(move || {
            let out = write();
            let _ = done.send(Event::Done);
            out
        });
        let mut pending = Vec::new();
        while let Ok(Event::Step(position, call)) =
            stepper.events.recv_timeout(Duration::from_secs(20))
        {
            let exists_now = cluster.document_exists(id).unwrap_or(false);
            let (tx, rx) = mpsc::channel();
            scope.spawn(move || {
                let _ = tx.send(read_all(cluster, title, id));
            });
            let prompt = rx.recv_timeout(Duration::from_millis(150)).ok();
            let _ = stepper.go.send(());
            pending.push((position, call, exists_now, prompt, rx));
        }
        stepper.armed.store(false, Ordering::SeqCst);
        let result = writer.join().expect("writer thread");
        let observations = pending
            .into_iter()
            .map(|(position, call, exists_now, prompt, rx)| {
                let waited = prompt.is_none();
                let seen = prompt.unwrap_or_else(|| {
                    rx.recv_timeout(Duration::from_secs(20))
                        .expect("a waiting reader finishes once the write does")
                });
                Observation {
                    position,
                    call,
                    waited,
                    exists_now,
                    seen,
                }
            })
            .collect();
        (observations, result)
    })
}

/// The selective placement of `dsl` on this cluster.
fn placed(cluster: &ClusterEngine, dsl: &str) -> Vec<usize> {
    let ast = crate::dsl::parse(dsl).expect("dsl");
    let mut lc = String::new();
    let ex = crate::compile::extract_readonly(&ast, &cluster.norm, &cluster.dict, &mut lc);
    match placement_of(
        &cluster.dict,
        &cluster.ring,
        &ex,
        true,
        cluster.per_shard.hot_anchor_threshold,
    ) {
        Target::Selective(shards) => shards,
        _ => panic!("{dsl} is not a selective query"),
    }
}

fn same_placement_upsert_is_never_missing(num_shards: usize) {
    let cfg = ClusterConfig {
        num_shards,
        ..Default::default()
    };
    let seed = vec![(999u64, "zzkeep zzalpha".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster");
    let stepper = step_writes(&mut cluster, 999);
    let title = "zzkeep zzalpha zzextra";
    assert_eq!(read_all(&cluster, title, 999), Seen::exactly(999));

    // A re-put of the same DSL: the routine bulk re-index case.
    let (observations, result) = observe(&cluster, &stepper, title, 999, || {
        cluster.upsert_query(999, "zzkeep zzalpha", 2)
    });
    result.expect("upsert accepted");
    assert!(!observations.is_empty(), "the upsert reached a shard");
    for observation in &observations {
        assert!(
            !observation.waited,
            "a placement-preserving upsert must not make readers wait: {observation:?}"
        );
        assert_eq!(
            observation.seen,
            Seen::exactly(999),
            "the query went missing mid-upsert: {observation:?}"
        );
        assert!(observation.exists_now, "{observation:?}");
        assert!(
            matches!(observation.call, WriteCall::Replace(999, _)),
            "a same-placement upsert is one atomic replace per placement shard: {observation:?}"
        );
    }
    assert_eq!(read_all(&cluster, title, 999), Seen::exactly(999));
}

#[test]
fn same_placement_upsert_is_never_missing_on_one_shard() {
    same_placement_upsert_is_never_missing(1);
}

#[test]
fn same_placement_upsert_is_never_missing_on_four_shards() {
    same_placement_upsert_is_never_missing(4);
}

/// An upsert that moves the query to another shard: a title matching both versions sees
/// the query exactly once through every read path, and never an ownership error — readers
/// that would have caught the move half-done wait for it instead.
#[test]
fn a_moving_upsert_is_seen_exactly_once() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let tokens: Vec<String> = (0..64).map(|i| format!("zzmove{i}")).collect();
    let home = placed(&cluster, &tokens[0]);
    let away = tokens
        .iter()
        .find(|token| placed(&cluster, token) != home)
        .expect("some token routes to another shard");
    let (old, new) = (tokens[0].as_str(), away.as_str());
    let title = format!("{old} {new}");
    cluster.upsert_query(999, old, 1).expect("seed");
    let stepper = step_writes(&mut cluster, 999);
    assert_eq!(read_all(&cluster, &title, 999), Seen::exactly(999));

    let (observations, result) = observe(&cluster, &stepper, &title, 999, || {
        cluster.upsert_query(999, new, 2)
    });
    result.expect("moving upsert accepted");
    assert!(
        observations
            .iter()
            .any(|o| matches!(o.call, WriteCall::Delete(999))),
        "precondition: the upsert moved the query off its old shard: {observations:?}"
    );
    for observation in &observations {
        assert_eq!(
            observation.seen,
            Seen::exactly(999),
            "a reader saw the move half-done: {observation:?}"
        );
        assert!(
            observation.exists_now,
            "an unfenced point read found no version mid-move: {observation:?}"
        );
    }
    assert!(
        observations.iter().any(|o| o.waited),
        "readers overlapping the fenced rewrite wait for it: {observations:?}"
    );
    assert_eq!(read_all(&cluster, &title, 999), Seen::exactly(999));
    assert_eq!(cluster.percolate(old).expect("old only"), Vec::<u64>::new());
}

/// A move whose old and new placements overlap (an any-of query that keeps one branch).
#[test]
fn a_partially_overlapping_move_is_seen_exactly_once() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let tokens: Vec<String> = (0..64).map(|i| format!("zzlap{i}")).collect();
    let shard_of = |token: &String| placed(&cluster, token)[0];
    let a = &tokens[0];
    let b = tokens
        .iter()
        .find(|t| shard_of(t) != shard_of(a))
        .expect("second shard");
    let c = tokens
        .iter()
        .find(|t| shard_of(t) != shard_of(a) && shard_of(t) != shard_of(b))
        .expect("third shard");
    let (old, new) = (format!("({a},{b})"), format!("({b},{c})"));
    assert_ne!(placed(&cluster, &old), placed(&cluster, &new));
    let title = format!("{a} {b} {c}");
    cluster.upsert_query(999, &old, 1).expect("seed");
    let stepper = step_writes(&mut cluster, 999);

    let (observations, result) = observe(&cluster, &stepper, &title, 999, || {
        cluster.upsert_query(999, &new, 2)
    });
    result.expect("moving upsert accepted");
    for observation in &observations {
        assert_eq!(
            observation.seen,
            Seen::exactly(999),
            "a reader saw the move half-done: {observation:?}"
        );
        assert!(
            observation.exists_now,
            "an unfenced point read found no version mid-move: {observation:?}"
        );
    }
    assert_eq!(read_all(&cluster, &title, 999), Seen::exactly(999));
}

/// A read with a deadline that arrives while a move is rewriting shards fails with its own
/// deadline instead of being held until the move finishes.
#[test]
fn a_deadline_bounded_read_is_not_held_past_its_deadline_by_a_move() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let tokens: Vec<String> = (0..64).map(|i| format!("zzdead{i}")).collect();
    let home = placed(&cluster, &tokens[0]);
    let away = tokens
        .iter()
        .find(|token| placed(&cluster, token) != home)
        .expect("some token routes to another shard");
    let (old, new) = (tokens[0].as_str(), away.as_str());
    let title = format!("{old} {new}");
    cluster.upsert_query(999, old, 1).expect("seed");
    let stepper = step_writes(&mut cluster, 999);
    let program = cluster
        .compile_rank_program(&crate::rank::RankProgramSpec::default())
        .expect("rank program");

    let (timed, result) = std::thread::scope(|scope| {
        stepper.armed.store(true, Ordering::SeqCst);
        let done = stepper.done.clone();
        let cluster = &cluster;
        let writer = scope.spawn(move || {
            let out = cluster.upsert_query(999, new, 2);
            let _ = done.send(Event::Done);
            out
        });
        let mut timed = None;
        while let Ok(Event::Step(_, call)) = stepper.events.recv_timeout(Duration::from_secs(20)) {
            // The first step inside the fence: the conditional replace declined before it.
            let fenced = matches!(
                call,
                WriteCall::Delete(_)
                    | WriteCall::Replace(_, crate::cluster::shard::ReplaceMode::Unconditional)
            );
            if fenced && timed.is_none() {
                let started = Instant::now();
                let read = cluster.try_percolate_filtered_top_k(
                    &title,
                    &[],
                    crate::result::TopKOptions::default(),
                    &program,
                    Some(started + Duration::from_millis(40)),
                );
                timed = Some((
                    matches!(read, Err(ClusterRankedError::DeadlineExceeded)),
                    started.elapsed(),
                ));
            }
            let _ = stepper.go.send(());
        }
        stepper.armed.store(false, Ordering::SeqCst);
        (timed, writer.join().expect("writer thread"))
    });
    result.expect("moving upsert accepted");
    let (deadline_exceeded, waited) = timed.expect("the move reached a fenced step");
    assert!(
        deadline_exceeded,
        "the read must fail with its own deadline"
    );
    assert!(
        waited < Duration::from_secs(5),
        "the read returned at its deadline, not when the move ended: {waited:?}"
    );
    assert_eq!(read_all(&cluster, &title, 999), Seen::exactly(999));
}
