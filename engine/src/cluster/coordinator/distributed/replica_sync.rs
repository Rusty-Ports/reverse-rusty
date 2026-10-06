//! Proving a remote replica against its primary before the coordinator serves (ADR-195).
//!
//! Whether reads may fail over to a replica is a flag the coordinator keeps in memory. A
//! coordinator that has just connected knows nothing about what a replica missed under an
//! earlier coordinator, or whether it came back on an empty volume, so it must not presume the
//! flag: it compares what each copy holds and trusts only an exact match.

use crate::cluster::clog::LogPos;
use crate::cluster::remote::RemoteShard;
use crate::cluster::replica::{catch_up_replica, ReplicaProof};
use crate::cluster::shard::{Shard, ShardError};
use crate::dict::Dict;
use crate::normalize::Normalizer;

/// A copy's order-independent live-set fingerprint and live count (ADR-097).
type ContentFingerprint = (u64, u64, u64);

/// Bound on the tail drain after a recovery. Nothing writes while the coordinator connects,
/// so the first pass finds the tail empty; the bound is a safety cap.
const TAIL_DRAIN_PASSES: usize = 8;

/// One position's primary as its replicas are checked against it. The primary's fingerprint
/// is read once, on first use, and kept: every replica is compared with the same value.
pub(super) struct PrimaryContent<'a> {
    primary: &'a RemoteShard,
    endpoint: &'a str,
    fingerprint: Option<Result<ContentFingerprint, String>>,
}

impl<'a> PrimaryContent<'a> {
    pub(super) fn new(primary: &'a RemoteShard, endpoint: &'a str) -> Self {
        Self {
            primary,
            endpoint,
            fingerprint: None,
        }
    }

    fn fingerprint(&mut self) -> Result<ContentFingerprint, String> {
        let primary = self.primary;
        self.fingerprint
            .get_or_insert_with(|| {
                primary
                    .content_fingerprint()
                    .map_err(|error| format!("the primary could not attest what it holds: {error}"))
            })
            .clone()
    }

    /// Whether `replica` holds exactly what the primary holds. Any doubt is a refusal: a copy
    /// that cannot attest its content, or attests different content, is not proven.
    pub(super) fn prove(&mut self, replica: &RemoteShard) -> ReplicaProof {
        let held = replica
            .content_fingerprint()
            .map_err(|error| format!("the replica could not attest what it holds: {error}"));
        decide(self.fingerprint(), held)
    }

    /// Replace `replica`'s content with the primary's and prove it again. Only for an operator
    /// who has declared the primaries authoritative: recovery discards whatever the replica
    /// held, and when the primary is the copy that lost data, that was the surviving one.
    pub(super) fn recover_then_prove(
        &mut self,
        replica: &RemoteShard,
        norm: &Normalizer,
        dict: &Dict,
    ) -> ReplicaProof {
        self.recover(replica, norm, dict)
            .map_err(|error| format!("recovery from the primary failed: {error}"))?;
        // The primary was sealed for the copy, which does not change what it holds.
        self.prove(replica)
    }

    fn recover(
        &self,
        replica: &RemoteShard,
        norm: &Normalizer,
        dict: &Dict,
    ) -> Result<(), ShardError> {
        // Pin the primary's un-sealed tail so the copy's seal cannot trim it (ADR-040).
        let (lease, _pinned) = self.primary.acquire_retention_lease()?;
        let recovered = (|| {
            let (_segments, _queries, sealed_at) =
                replica.recover_from(self.endpoint, dict.fingerprint())?;
            let mut drained_to = LogPos(sealed_at);
            for _ in 0..TAIL_DRAIN_PASSES {
                let next = catch_up_replica(replica, self.primary, norm, dict, drained_to)?;
                self.primary.renew_retention_lease(lease, next)?;
                if next == drained_to {
                    break;
                }
                drained_to = next;
            }
            Ok(())
        })();
        // A lease left behind only makes the primary keep translog until its next seal.
        let released = self.primary.release_retention_lease(lease);
        recovered.and(released)
    }
}

/// A replica is proven only when both copies attested their content and the two agree.
fn decide(
    primary: Result<ContentFingerprint, String>,
    replica: Result<ContentFingerprint, String>,
) -> ReplicaProof {
    compare(primary?, replica?)
}

fn compare(primary: ContentFingerprint, replica: ContentFingerprint) -> ReplicaProof {
    if primary == replica {
        return Ok(());
    }
    Err(format!(
        "the replica holds {} live queries (fingerprint {:016x}{:016x}) and its primary holds {} \
         ({:016x}{:016x})",
        replica.2, replica.1, replica.0, primary.2, primary.1, primary.0
    ))
}

#[cfg(test)]
mod tests {
    use super::{compare, decide};

    /// A copy that cannot say what it holds proves nothing, whichever copy it is, and
    /// whatever the other one says.
    #[test]
    fn a_copy_that_cannot_attest_its_content_is_not_a_proof() {
        let held = || Ok((1, 2, 3));
        let silent = || Err("no fingerprint".to_string());
        assert!(decide(held(), held()).is_ok());
        assert_eq!(decide(held(), silent()), Err("no fingerprint".to_string()));
        assert_eq!(decide(silent(), held()), Err("no fingerprint".to_string()));
        assert!(decide(silent(), silent()).is_err());
    }

    #[test]
    fn only_an_exact_match_is_a_proof() {
        assert!(compare((1, 2, 3), (1, 2, 3)).is_ok());
        for different in [(9, 2, 3), (1, 9, 3), (1, 2, 9)] {
            let reason = compare((1, 2, 3), different).expect_err("the copies differ");
            assert!(reason.contains("primary holds 3"), "{reason}");
        }
    }

    #[test]
    fn two_empty_copies_are_equal() {
        assert!(compare((0, 0, 0), (0, 0, 0)).is_ok());
    }
}
