//! A tiny LRU keyed by [`ShardId`].
//!
//! Capacities here are small (single digits to low tens), so an O(n) scan to find the
//! least-recently-used entry is cheaper than the bookkeeping of an intrusive list — and a
//! great deal easier to be sure is correct.

use std::collections::HashMap;

use super::ShardId;

/// What a [`Lru::get_or_open`] call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Event {
    pub opened: bool,
    pub evicted: u64,
}

pub struct Lru<V> {
    capacity: usize,
    tick: u64,
    entries: HashMap<ShardId, (u64, V)>,
    evictions: u64,
    opens: u64,
}

impl<V> Lru<V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "an LRU with no capacity can hold nothing");
        Self {
            capacity,
            tick: 0,
            entries: HashMap::new(),
            evictions: 0,
            opens: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    pub fn opens(&self) -> u64 {
        self.opens
    }

    pub fn contains(&self, id: ShardId) -> bool {
        self.entries.contains_key(&id)
    }

    /// Fetch the value for `id`, creating it with `open` if absent.
    ///
    /// Making room happens **before** `open` runs, so peak occupancy never exceeds the
    /// capacity — the point of the cache is bounding resident memory, and briefly holding
    /// `capacity + 1` connections would defeat it.
    /// Also reports what it did, because the caller cannot read the counters afterwards
    /// while holding the returned mutable borrow.
    pub fn get_or_open<F, E>(&mut self, id: ShardId, open: F) -> Result<(&mut V, Event), E>
    where
        F: FnOnce() -> Result<V, E>,
    {
        self.tick += 1;
        let now = self.tick;

        if self.entries.contains_key(&id) {
            let slot = self.entries.get_mut(&id).expect("checked above");
            slot.0 = now;
            return Ok((&mut slot.1, Event::default()));
        }

        let mut event = Event::default();
        while self.entries.len() >= self.capacity {
            let Some(victim) = self.coldest() else { break };
            self.entries.remove(&victim);
            self.evictions += 1;
            event.evicted += 1;
        }

        let value = open()?;
        self.opens += 1;
        event.opened = true;
        Ok((&mut self.entries.entry(id).or_insert((now, value)).1, event))
    }

    fn coldest(&self) -> Option<ShardId> {
        self.entries
            .iter()
            .min_by_key(|(_, (tick, _))| *tick)
            .map(|(id, _)| *id)
    }

    /// Drop a shard's entry, closing whatever it held.
    ///
    /// Used to hand a shard's file over to the replication path: a connection left open
    /// across raw page writes serves stale data with no error, so the handover has to close
    /// it rather than merely stop using it.
    pub fn remove(&mut self, id: ShardId) -> Option<V> {
        self.entries.remove(&id).map(|(_, v)| v)
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = (ShardId, &mut V)> {
        self.entries.iter_mut().map(|(id, (_, v))| (*id, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_ok(v: u32) -> impl FnOnce() -> Result<u32, ()> {
        move || Ok(v)
    }

    #[test]
    fn evicts_the_least_recently_used() {
        let mut lru: Lru<u32> = Lru::new(2);
        lru.get_or_open(ShardId(1), open_ok(1)).unwrap();
        lru.get_or_open(ShardId(2), open_ok(2)).unwrap();
        // Touch 1 so 2 becomes coldest.
        lru.get_or_open(ShardId(1), open_ok(99)).unwrap();
        lru.get_or_open(ShardId(3), open_ok(3)).unwrap();

        assert!(lru.contains(ShardId(1)));
        assert!(
            !lru.contains(ShardId(2)),
            "2 was coldest and should be gone"
        );
        assert!(lru.contains(ShardId(3)));
        assert_eq!(lru.evictions(), 1);
    }

    #[test]
    fn never_exceeds_capacity() {
        let mut lru: Lru<u32> = Lru::new(4);
        for i in 0..50 {
            lru.get_or_open(ShardId(i), open_ok(i)).unwrap();
            assert!(lru.len() <= 4, "capacity exceeded at {i}");
        }
        assert_eq!(lru.len(), 4);
        assert_eq!(lru.opens(), 50);
        assert_eq!(lru.evictions(), 46);
    }

    #[test]
    fn a_failed_open_is_not_cached() {
        let mut lru: Lru<u32> = Lru::new(2);
        let r: Result<(&mut u32, Event), &str> = lru.get_or_open(ShardId(1), || Err("boom"));
        assert!(r.is_err());
        assert!(!lru.contains(ShardId(1)));
        assert_eq!(lru.opens(), 0);
    }

    #[test]
    fn a_cache_hit_does_not_reopen() {
        let mut lru: Lru<u32> = Lru::new(2);
        lru.get_or_open(ShardId(1), open_ok(7)).unwrap();
        let (v, event) = lru
            .get_or_open(ShardId(1), || -> Result<u32, ()> {
                panic!("must not reopen a cached shard")
            })
            .unwrap();
        assert_eq!(*v, 7);
        assert!(!event.opened, "a cache hit must not report an open");
        assert_eq!(lru.opens(), 1);
    }
}
