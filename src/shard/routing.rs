//! Stable key routing schemes.
//!
//! `ModuloV1` is the on-disk contract shipped by manifest format 1. `LinearV1` grows one
//! shard at a time: a growth step splits exactly one old bucket and changes no other
//! key's route. The hash itself is shared and versioned separately because changing either
//! its constants or key encoding is a data migration.

use super::ShardId;

/// Stable FNV-1a used by every routing scheme.
///
/// This function must never change in place. Introduce a new hash scheme and migrate data
/// if different behavior is ever required.
pub fn hash_key(key: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for byte in key {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// The legacy immutable-count router.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ModuloV1 {
    shard_count: u32,
}

impl ModuloV1 {
    pub fn new(shard_count: u32) -> crate::Result<Self> {
        if shard_count == 0 {
            return Err(crate::Error::ShardConfig(
                "modulo routing requires at least one shard".into(),
            ));
        }
        Ok(Self { shard_count })
    }

    pub const fn shard_count(self) -> u32 {
        self.shard_count
    }

    pub fn route_hash(self, hash: u64) -> ShardId {
        ShardId((hash % self.shard_count as u64) as u32)
    }

    pub fn route(self, key: &[u8]) -> ShardId {
        self.route_hash(hash_key(key))
    }
}

/// Mutable routing metadata for incremental linear hashing.
///
/// Active shards are `2^level + split_pointer`. `split_pointer` is always below
/// `2^level`; its bucket is the next one a growth operation must split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LinearV1 {
    level: u8,
    split_pointer: u32,
}

impl LinearV1 {
    /// The smallest layout: one active shard, whose next growth creates shard 1.
    pub const ONE: Self = Self {
        level: 0,
        split_pointer: 0,
    };

    pub fn new(level: u8, split_pointer: u32) -> crate::Result<Self> {
        let base = base_for(level)?;
        if split_pointer >= base {
            return Err(crate::Error::ShardConfig(format!(
                "linear routing split_pointer {split_pointer} is outside 0..{base} at level {level}"
            )));
        }
        Ok(Self {
            level,
            split_pointer,
        })
    }

    /// Canonical linear state for exactly `active_shards`.
    pub fn for_shard_count(active_shards: u32) -> crate::Result<Self> {
        if active_shards == 0 {
            return Err(crate::Error::ShardConfig(
                "linear routing requires at least one shard".into(),
            ));
        }
        let level = (31 - active_shards.leading_zeros()) as u8;
        let base = 1u32 << level;
        Self::new(level, active_shards - base)
    }

    pub const fn level(self) -> u8 {
        self.level
    }

    pub const fn split_pointer(self) -> u32 {
        self.split_pointer
    }

    pub fn base(self) -> u32 {
        // Validated by constructors; level 31 is representable as u32.
        1u32 << self.level
    }

    pub fn shard_count(self) -> u32 {
        self.base() + self.split_pointer
    }

    /// The only old shard whose keys may move in the next growth step.
    pub const fn split_source(self) -> ShardId {
        ShardId(self.split_pointer)
    }

    /// The new shard created by the next growth step.
    pub fn split_destination(self) -> ShardId {
        ShardId(self.base() + self.split_pointer)
    }

    /// Routing state after one successfully committed split.
    pub fn grown(self) -> crate::Result<Self> {
        let base = self.base();
        let next = self.split_pointer + 1;
        if next == base {
            let level = self.level.checked_add(1).ok_or_else(|| {
                crate::Error::ShardConfig("linear routing exhausted its level space".into())
            })?;
            Self::new(level, 0)
        } else {
            Self::new(self.level, next)
        }
    }

    pub fn route_hash(self, hash: u64) -> ShardId {
        let base = self.base() as u64;
        let first = (hash % base) as u32;
        if first < self.split_pointer {
            ShardId((hash % (base * 2)) as u32)
        } else {
            ShardId(first)
        }
    }

    pub fn route(self, key: &[u8]) -> ShardId {
        self.route_hash(hash_key(key))
    }
}

fn base_for(level: u8) -> crate::Result<u32> {
    1u32.checked_shl(level as u32).ok_or_else(|| {
        crate::Error::ShardConfig(format!(
            "linear routing level {level} exceeds the u32 shard-id space"
        ))
    })
}

/// Versioned router stored by cluster metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Routing {
    ModuloV1(ModuloV1),
    LinearV1(LinearV1),
}

impl Routing {
    pub fn route(self, key: &[u8]) -> ShardId {
        match self {
            Self::ModuloV1(routing) => routing.route(key),
            Self::LinearV1(routing) => routing.route(key),
        }
    }

    pub fn shard_count(self) -> u32 {
        match self {
            Self::ModuloV1(routing) => routing.shard_count(),
            Self::LinearV1(routing) => routing.shard_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn fnv_contract_does_not_move() {
        assert_eq!(hash_key(b"user:1"), 0xf7fd_9aaa_7508_1ceb);
        assert_eq!(hash_key(b"user:2"), 0xf7fd_9baa_7508_1e9e);
        assert_eq!(hash_key(b""), 0xcbf2_9ce4_8422_2325);
    }

    #[test]
    fn modulo_matches_the_legacy_contract() {
        let routing = ModuloV1::new(64).unwrap();
        assert_eq!(routing.route(b"user:1"), ShardId(43));
        assert_eq!(routing.route(b"user:2"), ShardId(30));
        assert_eq!(routing.route(b""), ShardId(37));
    }

    #[test]
    fn shard_counts_have_one_canonical_linear_state() {
        for count in 1..=10_000 {
            let routing = LinearV1::for_shard_count(count).unwrap();
            assert_eq!(routing.shard_count(), count);
            assert!(routing.split_pointer() < routing.base());
        }
    }

    #[test]
    fn growth_moves_keys_only_from_the_split_source_to_destination() {
        let mut routing = LinearV1::ONE;
        for _ in 1..1_024 {
            let next = routing.grown().unwrap();
            let source = routing.split_source();
            let destination = routing.split_destination();
            for hash in sample_hashes() {
                let before = routing.route_hash(hash);
                let after = next.route_hash(hash);
                if before != after {
                    assert_eq!(before, source, "hash {hash} moved from a third shard");
                    assert_eq!(
                        after, destination,
                        "hash {hash} moved somewhere but the new shard"
                    );
                }
            }
            routing = next;
        }
    }

    #[test]
    fn every_active_shard_is_reachable() {
        for count in 1..=1_024 {
            let routing = LinearV1::for_shard_count(count).unwrap();
            let reached: BTreeSet<u32> = (0..(count as u64 * 4))
                .map(|hash| routing.route_hash(hash).0)
                .collect();
            assert_eq!(
                reached,
                (0..count).collect(),
                "unreachable shard with {count} active"
            );
        }
    }

    #[test]
    fn powers_of_two_are_legacy_compatible() {
        for count in [1, 2, 4, 16, 64, 256, 1_024] {
            let modulo = ModuloV1::new(count).unwrap();
            let linear = LinearV1::for_shard_count(count).unwrap();
            assert_eq!(linear.split_pointer(), 0);
            for hash in sample_hashes() {
                assert_eq!(modulo.route_hash(hash), linear.route_hash(hash));
            }
        }
    }

    #[test]
    fn invalid_states_are_refused() {
        assert!(ModuloV1::new(0).is_err());
        assert!(LinearV1::for_shard_count(0).is_err());
        assert!(LinearV1::new(3, 8).is_err());
        assert!(LinearV1::new(32, 0).is_err());
    }

    fn sample_hashes() -> impl Iterator<Item = u64> {
        (0u64..8_192).map(|n| {
            n.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .rotate_left((n % 64) as u32)
        })
    }
}
