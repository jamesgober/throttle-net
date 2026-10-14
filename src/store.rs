//! The sharded, bounded-memory per-key bucket store behind
//! [`PerKey`](crate::PerKey).
//!
//! Each key's state is a [`BucketCore`] (better-bucket's lock-free engine with
//! caller-supplied time) plus two small eviction marks. Keys are spread across
//! shards by hash, each shard behind its own `RwLock`: an existing key's
//! acquire takes only a shard *read* lock plus the bucket's own atomic, so
//! unrelated keys — and even concurrent acquires of one key — never serialise.
//! Only first-seeing a key takes the shard write lock, briefly.
//!
//! # Layout
//!
//! A shard keeps its entries densely in a `Vec` of slots and finds them through
//! a `hashbrown::HashTable` of slot indices, so the key is hashed once per
//! operation and stored once. Disjoint bits of that one hash pick the shard and
//! the table bucket. Shards are padded to their own cache lines so neighbouring
//! shards' locks never share one.
//!
//! # Eviction: bounded work per insert
//!
//! Memory is bounded by eviction that is lazy, per shard, and does a fixed
//! amount of work, under the write lock an insert already holds:
//!
//! - **Capacity — CLOCK (second chance), recovered keys first.** Every access
//!   sets the entry's *referenced* bit; a new key starts with it clear. When a
//!   full shard needs room, a hand sweeps at most [`CLOCK_SCAN`] slots looking
//!   for an entry that can go **losslessly**: one whose bucket is full (a full
//!   bucket recreated later is the same bucket, and a recreated key starts no
//!   fuller than its configured initial fill). Among those, one whose
//!   referenced bit was already clear is preferred; bits are cleared as the
//!   hand passes.
//! - **The debt ledger.** If no scanned entry has recovered, a *limited* entry
//!   (unreferenced, if one was scanned) is evicted, but its debt — the instant
//!   its bucket would be full again — is first written to the shard's ledger:
//!   16 bytes keyed by the key's hash instead of the whole entry. A key
//!   re-created while its debt is in the ledger starts with that debt, so
//!   forgetting its entry hands it nothing. The ledger holds as many debts as
//!   the shard holds keys; settled debts are dropped as the inserts pass. When
//!   the ledger is full of unsettled debts too, [`WhenFull`] decides: refuse
//!   the new key with the wait until room is expected (the default — the
//!   limit always holds), or forget the debt that settles soonest.
//! - **Idle TTL — incremental sweep.** Each insert also examines the next
//!   [`TTL_SWEEP`] slots from a second cursor and reclaims any entry idle past
//!   the TTL **whose bucket has recovered**, so idle state drains continuously
//!   rather than in one stop-the-world pass. An idle key still in debt (for
//!   example behind its own reservations) waits until it has recovered.
//!
//! Removal is a swap-remove plus two table updates: O(1). The remaining
//! shard-wide pass is amortized growth (the slot vector doubling, the index
//! rehashing to clear tombstones), so capped shards are kept small — the shard
//! count is raised until no shard's share of the cap exceeds
//! [`MAX_KEYS_PER_SHARD`] — and that pass stays short.

use core::hash::Hash;
use core::time::Duration;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use ahash::RandomState;
use better_bucket::{BucketConfig, BucketCore};
use hashbrown::HashTable;

use crate::eviction::{Eviction, WhenFull};

/// The hash bits that pick a shard. `hashbrown` takes a bucket index from the
/// low bits and a 7-bit tag from the top seven (57..64), so bits 40..56 never
/// correlate with either: keys that share a shard still spread evenly across
/// its table.
const SHARD_SHIFT: u32 = 40;

/// The most shards a [`PerKey`](crate::PerKey) store creates. A larger
/// request (including `usize::MAX`) is clamped to it rather than overflowing
/// or exhausting memory.
///
/// # Examples
///
/// ```
/// use throttle_net::{Eviction, MAX_SHARDS, PerKey};
///
/// assert_eq!(MAX_SHARDS, 65_536);
/// let limiter: PerKey<u64> = PerKey::per_second(1)
///     .with_eviction(Eviction::unbounded())
///     .with_shards(usize::MAX);
/// assert_eq!(limiter.shard_count(), MAX_SHARDS);
/// ```
pub const MAX_SHARDS: usize = 1 << 16;

/// The most keys a capped shard may hold. A shard's index and slot vector
/// occasionally grow or rehash in one pass under its write lock, so the store
/// raises its shard count until every shard's share of the cap fits here; that
/// pass then touches at most this many keys.
pub(crate) const MAX_KEYS_PER_SHARD: usize = 1 << 14;

/// The most slots the CLOCK hand examines to evict one entry.
pub(crate) const CLOCK_SCAN: usize = 32;

/// The slots the idle sweep examines per insert.
pub(crate) const TTL_SWEEP: usize = 8;

/// The ledger entries examined per insert, for a settled debt to drop or
/// replace.
pub(crate) const LEDGER_SCAN: usize = 8;

/// The shortest wait suggested to a key refused because its shard is full of
/// unrecovered keys, so a retry loop cannot spin.
const MIN_REFUSAL_WAIT: Duration = Duration::from_millis(1);

/// The most slots one shard can index (slot indices are `u32`).
const MAX_SLOTS: usize = u32::MAX as usize;

/// Per-key state: the bucket plus the eviction marks.
struct Entry {
    bucket: BucketCore,
    /// CLOCK reference bit: set on every access, cleared by the sweeping hand.
    referenced: AtomicBool,
    /// Milliseconds since the store's epoch at the last access. Maintained only
    /// when an idle TTL is configured.
    last_seen_ms: AtomicU64,
}

impl Entry {
    fn new(bucket: BucketCore, now_ms: u64) -> Self {
        Self {
            bucket,
            // A new key starts unreferenced: a key seen once (a flood of unique
            // keys) is the first to go; a key accessed again earns a second
            // chance.
            referenced: AtomicBool::new(false),
            last_seen_ms: AtomicU64::new(now_ms),
        }
    }

    /// Marks the entry used. Both marks are written only when they change, so
    /// a hot key's cache line is not dirtied on every access.
    #[inline]
    fn touch(&self, now_ms: u64, track_idle: bool) {
        if !self.referenced.load(Ordering::Relaxed) {
            self.referenced.store(true, Ordering::Relaxed);
        }
        if track_idle && self.last_seen_ms.load(Ordering::Relaxed) < now_ms {
            self.last_seen_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// Whether the entry has been idle for at least `ttl_ms`.
    #[inline]
    fn is_expired(&self, now_ms: u64, ttl_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_seen_ms.load(Ordering::Relaxed)) >= ttl_ms
    }

    /// Whether evicting the entry loses nothing: its bucket is full, which a
    /// later re-creation reproduces exactly (or more strictly, when the
    /// configured initial fill is below the capacity). Being idle is not
    /// enough: a bucket holding reservations is in debt however long ago it
    /// was last touched (TN2-007).
    #[inline]
    fn is_lossless(&self, now: Duration) -> bool {
        self.bucket.available_at(now) >= self.bucket.capacity()
    }

    /// How long until the bucket is full again (`Duration::MAX` if never).
    #[inline]
    fn recovers_in(&self, now: Duration) -> Duration {
        let capacity = self.bucket.capacity();
        self.bucket
            .time_until_at(capacity, now)
            .unwrap_or(Duration::MAX)
    }
}

/// A forgotten limited key's debt: when its bucket would be full again.
#[derive(Clone, Copy)]
struct Debt {
    hash: u64,
    settles_at: Duration,
}

/// The debts of limited keys evicted to make room, keyed by key hash. A key
/// re-created while its debt is here starts with that debt.
struct Ledger {
    index: HashTable<u32>,
    debts: Vec<Debt>,
    /// The next entry examined for a settled debt.
    cursor: usize,
}

impl Ledger {
    fn new() -> Self {
        Self {
            index: HashTable::new(),
            debts: Vec::new(),
            cursor: 0,
        }
    }

    fn position(&self, hash: u64) -> Option<usize> {
        let debts = &self.debts;
        self.index
            .find(hash, |&pos| {
                debts
                    .get(pos as usize)
                    .is_some_and(|debt| debt.hash == hash)
            })
            .map(|&pos| pos as usize)
    }

    /// Removes the debt at `pos` by swap-remove. O(1).
    fn remove_at(&mut self, pos: usize) -> Option<Debt> {
        let last = self.debts.len().checked_sub(1)?;
        let hash = self.debts.get(pos)?.hash;
        if let Ok(found) = self.index.find_entry(hash, |&p| p as usize == pos) {
            let (_pos, _vacant) = found.remove();
        }
        if pos != last {
            if let Some(moved_hash) = self.debts.get(last).map(|debt| debt.hash) {
                if let Some(target) = self.index.find_mut(moved_hash, |&p| p as usize == last) {
                    *target = u32::try_from(pos).unwrap_or(u32::MAX);
                }
            }
        }
        Some(self.debts.swap_remove(pos))
    }

    /// Takes the debt recorded for `hash`, if any.
    fn take(&mut self, hash: u64) -> Option<Duration> {
        let pos = self.position(hash)?;
        self.remove_at(pos).map(|debt| debt.settles_at)
    }

    /// Records a debt, making room among the next [`LEDGER_SCAN`] entries when
    /// the ledger already holds `cap`: a settled debt is dropped; failing that,
    /// with `forget` set, the debt settling soonest is forgotten. Otherwise the
    /// debt is not recorded and the soonest settle time scanned is returned.
    fn record(
        &mut self,
        debt: Debt,
        now: Duration,
        cap: usize,
        forget: bool,
    ) -> Result<(), Duration> {
        if let Some(pos) = self.position(debt.hash) {
            // A hash collision with a debt already held: keep the later one.
            if let Some(held) = self.debts.get_mut(pos) {
                held.settles_at = held.settles_at.max(debt.settles_at);
            }
            return Ok(());
        }
        if self.debts.len() >= cap {
            let len = self.debts.len();
            let mut soonest: Option<(usize, Duration)> = None;
            for _ in 0..LEDGER_SCAN.min(len) {
                if self.cursor >= len {
                    self.cursor = 0;
                }
                if let Some(held) = self.debts.get(self.cursor) {
                    if held.settles_at <= now {
                        soonest = Some((self.cursor, held.settles_at));
                        break;
                    }
                    if soonest.is_none_or(|(_, at)| held.settles_at < at) {
                        soonest = Some((self.cursor, held.settles_at));
                    }
                }
                self.cursor += 1;
            }
            match soonest {
                Some((pos, at)) if at <= now || forget => {
                    let _dropped = self.remove_at(pos);
                }
                Some((_, at)) => return Err(at.saturating_sub(now)),
                None => return Err(Duration::MAX),
            }
        }
        let pos = u32::try_from(self.debts.len()).unwrap_or(u32::MAX);
        let Self { index, debts, .. } = self;
        debts.push(debt);
        let _bucket = index.insert_unique(debt.hash, pos, |&p| {
            debts.get(p as usize).map_or(0, |held| held.hash)
        });
        Ok(())
    }

    /// Drops settled debts among the next `budget` entries.
    fn sweep(&mut self, now: Duration, budget: usize) {
        for _ in 0..budget {
            let len = self.debts.len();
            if len == 0 {
                break;
            }
            if self.cursor >= len {
                self.cursor = 0;
            }
            if self
                .debts
                .get(self.cursor)
                .is_some_and(|debt| debt.settles_at <= now)
            {
                let _settled = self.remove_at(self.cursor);
            } else {
                self.cursor += 1;
            }
        }
    }
}

/// Puts a freshly created bucket into the debt a forgotten entry left: as
/// though it had been kept, it will be full again at `settles_at`. Rounds the
/// missing units up, so a restored key is never fuller than the original.
fn restore_debt(bucket: &BucketCore, config: BucketConfig, now: Duration, settles_at: Duration) {
    let remaining = settles_at.saturating_sub(now);
    if remaining.is_zero() {
        return;
    }
    let period = config.refill_period().as_nanos().max(1);
    let missing = (remaining.as_nanos() * u128::from(config.refill_amount())).div_ceil(period);
    let capacity = config.capacity();
    let available = bucket.available_at(now);
    // What should be left now: `capacity - missing`, or debt beyond empty.
    let keep = u128::from(capacity).saturating_sub(missing);
    let keep = u32::try_from(keep).unwrap_or(capacity);
    if available > keep {
        let _taken = bucket.acquire_up_to_at(available - keep, now);
    }
    let mut debt = missing.saturating_sub(u128::from(capacity));
    while debt > 0 && capacity > 0 {
        let chunk = u32::try_from(debt.min(u128::from(capacity))).unwrap_or(capacity);
        if bucket.reserve_at(chunk, Duration::MAX, now).is_none() {
            break;
        }
        debt -= u128::from(chunk);
    }
}

/// One stored key: its hash (kept so a moved slot can be re-indexed without
/// rehashing), the key, and its entry.
struct Slot<K> {
    hash: u64,
    key: K,
    entry: Entry,
}

/// A shard's contents: dense slots, an index from key hash to slot, the two
/// eviction cursors, and the debt ledger.
struct ShardMap<K> {
    index: HashTable<u32>,
    slots: Vec<Slot<K>>,
    /// CLOCK hand: the next slot the capacity sweep examines.
    hand: usize,
    /// The next slot the idle sweep examines.
    sweep: usize,
    /// Debts of limited keys evicted to make room.
    ledger: Ledger,
}

impl<K: Eq> ShardMap<K> {
    fn new() -> Self {
        Self {
            index: HashTable::new(),
            slots: Vec::new(),
            hand: 0,
            sweep: 0,
            ledger: Ledger::new(),
        }
    }

    /// The slot position holding `key`, if any.
    #[inline]
    fn position(&self, hash: u64, key: &K) -> Option<usize> {
        let slots = &self.slots;
        self.index
            .find(hash, |&pos| {
                slots
                    .get(pos as usize)
                    .is_some_and(|slot| slot.hash == hash && slot.key == *key)
            })
            .map(|&pos| pos as usize)
    }

    /// The entry for `key`, if any.
    #[inline]
    fn get(&self, hash: u64, key: &K) -> Option<&Entry> {
        self.position(hash, key)
            .and_then(|pos| self.slots.get(pos))
            .map(|slot| &slot.entry)
    }

    /// Appends a new key. The caller has checked it is absent and made room.
    fn insert(&mut self, hash: u64, key: K, entry: Entry) {
        let Self { index, slots, .. } = self;
        // The per-shard cap keeps the slot count at most `MAX_SLOTS`, so the
        // position always fits a `u32`.
        let pos = u32::try_from(slots.len()).unwrap_or(u32::MAX);
        slots.push(Slot { hash, key, entry });
        let _bucket = index.insert_unique(hash, pos, |&p| {
            slots.get(p as usize).map_or(0, |slot| slot.hash)
        });
    }

    /// Removes the slot at `pos` by swap-remove, re-indexing the slot that
    /// moves into its place. O(1).
    fn remove_at(&mut self, pos: usize) -> Option<Slot<K>> {
        let last = self.slots.len().checked_sub(1)?;
        let hash = self.slots.get(pos)?.hash;
        if let Ok(found) = self.index.find_entry(hash, |&p| p as usize == pos) {
            let (_pos, _vacant) = found.remove();
        }
        if pos != last {
            if let Some(moved_hash) = self.slots.get(last).map(|slot| slot.hash) {
                if let Some(target) = self.index.find_mut(moved_hash, |&p| p as usize == last) {
                    // `pos < last <= MAX_SLOTS`, so it fits.
                    *target = u32::try_from(pos).unwrap_or(u32::MAX);
                }
            }
        }
        Some(self.slots.swap_remove(pos))
    }

    /// Reclaims idle-expired, recovered entries among the next `budget` slots
    /// from the sweep cursor. Returns how many were reclaimed.
    fn sweep_idle(&mut self, now: Duration, now_ms: u64, ttl_ms: u64, budget: usize) -> usize {
        let mut reclaimed = 0;
        for _ in 0..budget {
            let len = self.slots.len();
            if len == 0 {
                break;
            }
            if self.sweep >= len {
                self.sweep = 0;
            }
            let expired = self.slots.get(self.sweep).is_some_and(|slot| {
                slot.entry.is_expired(now_ms, ttl_ms) && slot.entry.is_lossless(now)
            });
            if expired {
                // The swap moved an unexamined slot here; look at it next.
                let _reclaimed = self.remove_at(self.sweep);
                reclaimed += 1;
            } else {
                self.sweep += 1;
            }
        }
        reclaimed
    }

    /// Evicts one entry within [`CLOCK_SCAN`] of the hand.
    ///
    /// A recovered entry goes first — an unreferenced one if there is one,
    /// else the first recovered one seen. If none of the scanned entries has
    /// recovered, a limited one (unreferenced, if one was scanned) goes, but
    /// only once its debt is in the ledger. When the ledger has no room either
    /// (and `forget` is off), evicts nothing and returns the wait until room is
    /// expected.
    fn evict_one(
        &mut self,
        now: Duration,
        ledger_cap: usize,
        forget: bool,
    ) -> Result<(), Duration> {
        let len = self.slots.len();
        if len == 0 {
            return Ok(());
        }
        let mut victim = None;
        let mut first_recovered = None;
        let mut limited: Option<(usize, bool, Duration)> = None;
        let mut soonest = Duration::MAX;
        for _ in 0..CLOCK_SCAN.min(len) {
            if self.hand >= len {
                self.hand = 0;
            }
            if let Some(slot) = self.slots.get(self.hand) {
                let entry = &slot.entry;
                // Second chance: an entry seen since the hand last passed keeps
                // its place this time round.
                let referenced = entry.referenced.swap(false, Ordering::Relaxed);
                if entry.is_lossless(now) {
                    if !referenced {
                        victim = Some(self.hand);
                        break;
                    }
                    if first_recovered.is_none() {
                        first_recovered = Some(self.hand);
                    }
                } else {
                    let recovers_in = entry.recovers_in(now);
                    soonest = soonest.min(recovers_in);
                    // Prefer moving an unreferenced (not recently used) key.
                    if limited.is_none_or(|(_, was_referenced, _)| was_referenced && !referenced) {
                        limited = Some((self.hand, referenced, recovers_in));
                    }
                }
            }
            self.hand += 1;
        }
        let pos = if let Some(pos) = victim.or(first_recovered) {
            pos
        } else {
            let Some((pos, _, recovers_in)) = limited else {
                return Err(soonest.max(MIN_REFUSAL_WAIT));
            };
            let Some(hash) = self.slots.get(pos).map(|slot| slot.hash) else {
                return Err(soonest.max(MIN_REFUSAL_WAIT));
            };
            let debt = Debt {
                hash,
                settles_at: now.saturating_add(recovers_in),
            };
            if let Err(ledger_wait) = self.ledger.record(debt, now, ledger_cap, forget) {
                return Err(soonest.min(ledger_wait).max(MIN_REFUSAL_WAIT));
            }
            pos
        };
        let _evicted = self.remove_at(pos);
        // The swap moved the newest slot into `pos`; step past it so a key that
        // just arrived gets a full revolution before the hand returns.
        self.hand = pos + 1;
        Ok(())
    }
}

/// One shard: an independently locked slice of the key space, on its own cache
/// lines so neighbouring shards never falsely share.
#[repr(align(128))]
struct Shard<K> {
    map: RwLock<ShardMap<K>>,
}

/// The sharded per-key store.
pub(crate) struct Store<K> {
    shards: Box<[Shard<K>]>,
    /// `shard_count - 1`; the count is a power of two, so this masks the shard
    /// bits of a hash to an index without a division.
    shard_mask: u64,
    /// One random-seeded hasher for the whole store (collision-attack
    /// resistant); its single hash drives both the shard and the table.
    hasher: RandomState,
    /// Maximum live keys per shard (at most [`MAX_SLOTS`]).
    per_shard_cap: usize,
    /// Effective idle expiry in milliseconds, or `None` to never expire.
    idle_ttl_ms: Option<u64>,
    /// Whether a full ledger forgets a debt rather than refuse a new key.
    forget_debt: bool,
}

impl<K: Eq + Hash> Store<K> {
    /// Builds a store with `shards` shards (clamped to `1..=MAX_SHARDS`, raised
    /// so no capped shard holds more than `MAX_KEYS_PER_SHARD` keys, then
    /// rounded up to a power of two). An idle TTL shorter than `min_idle` is
    /// raised to it.
    pub(crate) fn new(shards: usize, eviction: Eviction, min_idle: Duration) -> Self {
        let for_cap = eviction
            .max_keys()
            .map_or(1, |max| max.div_ceil(MAX_KEYS_PER_SHARD));
        // `MAX_SHARDS` is a power of two, so rounding a clamped value up never
        // passes it (and never overflows).
        let shard_count = shards.max(for_cap).clamp(1, MAX_SHARDS).next_power_of_two();
        // Spread the cap evenly; at least one key per shard so a tiny cap with
        // many shards still admits keys.
        let per_shard_cap = eviction.max_keys().map_or(MAX_SLOTS, |max| {
            max.div_ceil(shard_count).clamp(1, MAX_SLOTS)
        });
        // Idle time is measured between floored millisecond stamps, so a
        // measured gap can exceed the true one by up to a millisecond. Rounding
        // the TTL up and adding one millisecond means a key is reclaimed only
        // once it has truly been idle for the whole TTL.
        let idle_ttl_ms = eviction.idle_ttl().map(|ttl| {
            let millis = ttl.max(min_idle).as_nanos().div_ceil(1_000_000);
            u64::try_from(millis).unwrap_or(u64::MAX).saturating_add(1)
        });
        let shards = (0..shard_count)
            .map(|_| Shard {
                map: RwLock::new(ShardMap::new()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            shard_mask: shard_count as u64 - 1,
            hasher: RandomState::new(),
            per_shard_cap,
            idle_ttl_ms,
            forget_debt: matches!(eviction.when_full(), WhenFull::ForgetDebt),
        }
    }

    /// The number of shards.
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    fn shard(&self, hash: u64) -> &Shard<K> {
        // The mask keeps the index below the shard count.
        let index = ((hash >> SHARD_SHIFT) & self.shard_mask) as usize;
        &self.shards[index]
    }

    /// Runs `op` on `key`'s bucket, creating the bucket from `config` (with its
    /// initial fill as of `now`) on first sight, and making room for it under
    /// the eviction policy.
    ///
    /// # Errors
    ///
    /// When `key` is new and its shard is full of keys that have not
    /// recovered, the key is refused (nothing is created and `op` does not
    /// run) with the wait until room is expected.
    #[inline]
    pub(crate) fn with_or_insert<R>(
        &self,
        key: &K,
        now: Duration,
        config: BucketConfig,
        op: impl FnOnce(&BucketCore) -> R,
    ) -> Result<R, Duration>
    where
        K: Clone,
    {
        let hash = self.hasher.hash_one(key);
        let shard = self.shard(hash);
        let track_idle = self.idle_ttl_ms.is_some();
        let now_ms = millis(now);

        // Fast path: a shared read lock is enough for an existing key.
        {
            let map = read(&shard.map);
            if let Some(entry) = map.get(hash, key) {
                entry.touch(now_ms, track_idle);
                return Ok(op(&entry.bucket));
            }
        }

        // Slow path: first-seen key. Take the write lock, re-check (another
        // thread may have inserted it in the gap), make room, insert.
        let mut map = write(&shard.map);
        if let Some(entry) = map.get(hash, key) {
            entry.touch(now_ms, track_idle);
            return Ok(op(&entry.bucket));
        }
        // A key forgotten while limited comes back in its debt. Take the debt
        // first, so the key's own ledger slot is free for the room it needs.
        let owed = map.ledger.take(hash);
        if let Err(wait) = self.make_room(&mut map, now, now_ms) {
            if let Some(settles_at) = owed {
                let _kept = map
                    .ledger
                    .record(Debt { hash, settles_at }, now, usize::MAX, false);
            }
            return Err(wait);
        }
        let entry = Entry::new(BucketCore::new(config, now), now_ms);
        if let Some(settles_at) = owed {
            restore_debt(&entry.bucket, config, now, settles_at);
        }
        let result = op(&entry.bucket);
        map.insert(hash, key.clone(), entry);
        Ok(result)
    }

    /// Runs `op` on `key`'s bucket if the key has state, without creating it
    /// and without refreshing its eviction marks.
    #[inline]
    pub(crate) fn with_existing<R>(&self, key: &K, op: impl FnOnce(&BucketCore) -> R) -> Option<R> {
        let hash = self.hasher.hash_one(key);
        let map = read(&self.shard(hash).map);
        map.get(hash, key).map(|entry| op(&entry.bucket))
    }

    /// Makes room in a shard about to receive a new key: an incremental idle
    /// sweep (and ledger sweep), then one eviction if the shard is still full.
    /// Bounded work.
    fn make_room(&self, map: &mut ShardMap<K>, now: Duration, now_ms: u64) -> Result<(), Duration> {
        if let Some(ttl_ms) = self.idle_ttl_ms {
            let _reclaimed = map.sweep_idle(now, now_ms, ttl_ms, TTL_SWEEP);
        }
        map.ledger.sweep(now, LEDGER_SCAN);
        if map.slots.len() >= self.per_shard_cap {
            map.evict_one(now, self.per_shard_cap, self.forget_debt)?;
        }
        Ok(())
    }

    /// Forgets `key` and any debt it left in the ledger. Returns whether it
    /// had state.
    pub(crate) fn remove(&self, key: &K) -> bool {
        let hash = self.hasher.hash_one(key);
        let mut map = write(&self.shard(hash).map);
        let debt = map.ledger.take(hash).is_some();
        match map.position(hash, key) {
            Some(pos) => map.remove_at(pos).is_some() || debt,
            None => debt,
        }
    }

    /// The number of forgotten limited keys whose debt is still remembered
    /// (test support).
    #[cfg(test)]
    pub(crate) fn ledger_len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| read(&shard.map).ledger.debts.len())
            .sum()
    }

    /// Forgets every key.
    pub(crate) fn clear(&self) {
        for shard in &*self.shards {
            let mut map = write(&shard.map);
            *map = ShardMap::new();
        }
    }

    /// The number of keys with state (a momentary snapshot).
    pub(crate) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| read(&shard.map).slots.len())
            .sum()
    }

    /// Whether no key has state (a momentary snapshot).
    pub(crate) fn is_empty(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| read(&shard.map).slots.is_empty())
    }

    /// The effective idle TTL in milliseconds (test support).
    #[cfg(test)]
    pub(crate) fn idle_ttl_ms(&self) -> Option<u64> {
        self.idle_ttl_ms
    }

    /// The per-shard key cap (test support).
    #[cfg(test)]
    pub(crate) fn per_shard_cap(&self) -> usize {
        self.per_shard_cap
    }
}

/// Whole milliseconds in `now`, saturating.
#[inline]
fn millis(now: Duration) -> u64 {
    u64::try_from(now.as_millis()).unwrap_or(u64::MAX)
}

/// Recovers a read guard even if a previous holder panicked: a poisoned shard
/// keeps limiting rather than propagating a panic into every caller. Nothing
/// under the lock can be left half-updated by a panic (every mutation is a
/// single push or swap-remove), so the data is consistent.
fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Recovers a write guard even if a previous holder panicked. See [`read`].
fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{CLOCK_SCAN, MAX_KEYS_PER_SHARD, MAX_SHARDS, Store, TTL_SWEEP};
    use crate::eviction::Eviction;
    use better_bucket::BucketConfig;
    use core::time::Duration;

    fn config(rate: u32) -> BucketConfig {
        BucketConfig::new(rate, rate, Duration::from_secs(1), rate).unwrap()
    }

    /// Takes `cost` for `key`; a key refused by a full shard counts as denied.
    fn take(store: &Store<u64>, key: u64, now: Duration, cost: u32) -> bool {
        store
            .with_or_insert(&key, now, config(10), |b| b.try_acquire_at(cost, now))
            .unwrap_or(false)
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn test_shard_count_is_clamped_raised_and_rounded() {
        let unbounded = Eviction::unbounded();
        assert_eq!(
            Store::<u64>::new(5, unbounded, Duration::ZERO).shard_count(),
            8
        );
        assert_eq!(
            Store::<u64>::new(0, unbounded, Duration::ZERO).shard_count(),
            1
        );
        assert_eq!(
            Store::<u64>::new(usize::MAX, unbounded, Duration::ZERO).shard_count(),
            MAX_SHARDS
        );
        // A million-key cap needs at least 64 shards of 16,384.
        let big = Store::<u64>::new(1, Eviction::capacity(1 << 20), Duration::ZERO);
        assert_eq!(big.shard_count(), 64);
        assert!(big.per_shard_cap() <= MAX_KEYS_PER_SHARD);
    }

    #[test]
    fn test_insert_creates_one_entry_and_reuses_it() {
        let store = Store::new(4, Eviction::unbounded(), Duration::ZERO);
        assert!(store.is_empty());
        assert!(take(&store, 1, ms(0), 4));
        assert!(take(&store, 1, ms(0), 4));
        assert!(!take(&store, 1, ms(0), 4), "the same bucket was reused");
        assert_eq!(store.len(), 1);
        assert_eq!(store.with_existing(&1, |b| b.available_at(ms(0))), Some(2));
        assert_eq!(store.with_existing(&2, |b| b.available_at(ms(0))), None);
    }

    #[test]
    fn test_capacity_bounds_a_unique_key_flood() {
        let store = Store::new(8, Eviction::capacity(100), Duration::ZERO);
        for key in 0..10_000u64 {
            let _ = take(&store, key, ms(0), 1);
        }
        assert!(store.len() <= store.per_shard_cap() * store.shard_count());
    }

    #[test]
    fn test_hot_limited_key_survives_a_flood() {
        // One shard of 64 keys. The hot key is drained (still limited) and
        // touched every round; flood keys are seen once and are not full either
        // (each took a token). The hot key keeps earning its second chance, so
        // the flood never evicts it — and never resets its allowance.
        let store = Store::new(1, Eviction::capacity(64), Duration::ZERO);
        assert!(take(&store, u64::MAX, ms(0), 10)); // hot key, drained
        for key in 0..5_000u64 {
            assert!(
                !take(&store, u64::MAX, ms(0), 1),
                "the hot key stays limited"
            );
            let _ = take(&store, key, ms(0), 1);
        }
        assert_eq!(
            store.with_existing(&u64::MAX, |b| b.available_at(ms(0))),
            Some(0)
        );
    }

    #[test]
    fn test_full_buckets_are_evicted_before_limited_ones() {
        let store = Store::new(1, Eviction::capacity(2), Duration::ZERO);
        assert!(take(&store, 1, ms(0), 10)); // limited
        assert!(take(&store, 2, ms(0), 0)); // created full (cost 0)
        // Key 1 is limited and unreferenced; key 2 is full. Inserting key 3
        // must evict key 2, losslessly.
        assert!(take(&store, 3, ms(0), 1));
        assert_eq!(store.with_existing(&1, |b| b.available_at(ms(0))), Some(0));
        assert_eq!(store.with_existing(&2, |_| ()), None);
    }

    #[test]
    fn test_ttl_sweep_is_incremental_and_eventually_complete() {
        // A small cap keeps the store at one shard, so every insert sweeps it.
        let store = Store::new(
            1,
            Eviction::idle(ms(100)).with_capacity(1_000),
            Duration::ZERO,
        );
        for key in 0..100u64 {
            assert!(take(&store, key, ms(0), 1));
        }
        // Long after the TTL, each insert reclaims at most TTL_SWEEP keys.
        let later = ms(10_000);
        assert!(take(&store, 1_000, later, 1));
        assert!(store.len() >= 100 + 1 - TTL_SWEEP);
        for key in 1_001..1_100u64 {
            assert!(take(&store, key, later, 1));
        }
        // Every one of the original keys is gone by now.
        assert!((0..100u64).all(|key| store.with_existing(&key, |_| ()).is_none()));
    }

    #[test]
    fn test_short_ttl_is_raised_to_the_refill_time() {
        let store = Store::<u64>::new(1, Eviction::idle(ms(10)), Duration::from_secs(60));
        assert_eq!(store.idle_ttl_ms(), Some(60_001));
    }

    #[test]
    fn test_clock_scan_is_bounded_when_every_key_is_hot() {
        let cap = CLOCK_SCAN * 4;
        let store = Store::new(1, Eviction::capacity(cap), Duration::ZERO);
        for key in 0..cap as u64 {
            assert!(take(&store, key, ms(0), 1));
        }
        for key in 0..cap as u64 {
            assert!(take(&store, key, ms(0), 1), "touch every key");
        }
        // Every key is still limited: newcomers move limited keys' debts to the
        // ledger, until the ledger is full too.
        for key in 10_000..10_000 + cap as u64 {
            assert!(take(&store, key, ms(0), 1), "room through the ledger");
        }
        assert_eq!(store.len(), cap);
        assert_eq!(store.ledger_len(), cap);
        // Now the newcomer is refused (bounded work, nothing evicted) with the
        // wait until a key or a debt recovers.
        let refused = store.with_or_insert(&9_999, ms(0), config(10), |_| ());
        assert_eq!(refused, Err(Duration::from_millis(100)));
        assert_eq!(store.len(), cap);
        // Once they have recovered, the newcomer gets a slot.
        assert!(take(&store, 9_999, ms(200), 1));
        assert_eq!(store.len(), cap);
    }

    #[test]
    fn test_a_forgotten_limited_key_comes_back_in_debt() {
        let store = Store::new(1, Eviction::capacity(1), Duration::ZERO);
        assert!(take(&store, 1, ms(0), 10)); // drained: full again at 1s
        assert!(take(&store, 2, ms(0), 1)); // evicts key 1 into the ledger
        assert_eq!(store.with_existing(&1, |_| ()), None);
        assert_eq!(store.ledger_len(), 1);
        // Back at 500ms: it owes what it would have owed had it been kept.
        assert!(take(&store, 1, ms(500), 5));
        assert!(!take(&store, 1, ms(500), 1), "no fresh allowance");
        // Key 2 had recovered by then, so it went without a trace.
        assert_eq!(store.ledger_len(), 0);
    }

    #[test]
    fn test_debt_beyond_empty_is_restored_as_a_reservation() {
        let store = Store::new(1, Eviction::capacity(1), Duration::ZERO);
        // Drain key 1 and queue 20 more tokens behind it: full again at 3s.
        assert!(take(&store, 1, ms(0), 10));
        let reserved = store
            .with_or_insert(&1, ms(0), config(10), |b| {
                (
                    b.reserve_at(10, Duration::MAX, ms(0)),
                    b.reserve_at(10, Duration::MAX, ms(0)),
                )
            })
            .unwrap();
        assert_eq!(reserved, (Some(ms(1_000)), Some(ms(2_000))));
        assert!(take(&store, 2, ms(1_100), 1)); // forgets key 1 (in debt)
        // At 1.1s the original key would have nothing until its reservations
        // were paid off at 2s; the restored one agrees.
        assert!(!take(&store, 1, ms(1_100), 1));
        let wait = store
            .with_existing(&1, |b| b.time_until_at(1, ms(1_100)))
            .flatten();
        assert!(wait >= Some(ms(900)), "{wait:?}");
    }

    #[test]
    fn test_forget_debt_policy_never_refuses() {
        let store = Store::new(
            1,
            Eviction::capacity(4).with_when_full(crate::eviction::WhenFull::ForgetDebt),
            Duration::ZERO,
        );
        for key in 0..100u64 {
            assert!(take(&store, key, ms(0), 10), "admitted {key}");
        }
        assert_eq!(store.len(), 4);
        assert_eq!(store.ledger_len(), 4);
    }

    #[test]
    fn test_settled_debts_are_swept_and_removal_clears_a_debt() {
        let store = Store::new(1, Eviction::capacity(1), Duration::ZERO);
        assert!(take(&store, 1, ms(0), 10));
        assert!(take(&store, 2, ms(0), 10)); // key 1's debt to the ledger
        assert_eq!(store.ledger_len(), 1);
        assert!(store.remove(&1), "the remembered debt counts as state");
        assert_eq!(store.ledger_len(), 0);
        assert!(take(&store, 3, ms(0), 10)); // key 2's debt to the ledger
        assert_eq!(store.ledger_len(), 1);
        assert!(take(&store, 4, ms(5_000), 1)); // key 2 settled long ago
        assert_eq!(store.ledger_len(), 0, "settled debts are dropped");
    }

    /// TN2-007: an idle key whose bucket is still in reservation debt is not
    /// reclaimed by the TTL sweep (the 1.1 candidate recreated it full).
    #[test]
    fn test_ttl_never_reclaims_a_key_in_debt() {
        let store = Store::new(
            1,
            Eviction::new(16, Duration::from_millis(1)),
            Duration::from_secs(1),
        );
        assert!(take(&store, 0, ms(0), 10));
        let _ = store.with_or_insert(&0, ms(0), config(10), |b| {
            (
                b.reserve_at(10, Duration::MAX, ms(0)),
                b.reserve_at(10, Duration::MAX, ms(0)),
            )
        });
        // Idle past the (raised) TTL, but its reservations run until 2s.
        assert!(take(&store, 1, ms(1_100), 1));
        assert!(
            store.with_existing(&0, |_| ()).is_some(),
            "kept while in debt"
        );
        assert!(!take(&store, 0, ms(1_100), 1));
        // Once recovered and idle, it goes.
        assert!(take(&store, 2, ms(10_000), 1));
        assert!(store.with_existing(&0, |_| ()).is_none());
    }

    #[test]
    fn test_a_unique_key_flood_never_resets_a_limited_key() {
        // rate-net 1.1's capacity eviction dropped unrecovered keys, so a flood
        // of new keys could evict a drained key and hand it a fresh allowance.
        // Here only recovered keys are ever evicted.
        let store = Store::new(1, Eviction::capacity(8), Duration::ZERO);
        assert!(take(&store, 0, ms(0), 10)); // drained
        for key in 1..10_000u64 {
            let _ = take(&store, key, ms(0), 10);
        }
        assert!(store.len() <= 8);
        assert!(store.ledger_len() <= 8);
        // Whether key 0 kept its slot or only its debt, it is still limited.
        assert!(!take(&store, 0, ms(0), 1));
        assert!(!take(&store, 0, ms(500), 6));
    }

    #[test]
    fn test_insert_at_capacity_is_constant_time() {
        // Regression (TN-003): every insert at the cap scanned the whole shard
        // under its write lock (~1.8ms per insert at the default cap). With a
        // bounded scan, 200k inserts into a full 16k-key shard stay fast even
        // in an unoptimised debug build.
        let store = Store::new(1, Eviction::capacity(MAX_KEYS_PER_SHARD), Duration::ZERO);
        let start = std::time::Instant::now();
        for key in 0..200_000u64 {
            let _ = take(&store, key, ms(0), 1);
        }
        assert!(store.len() <= MAX_KEYS_PER_SHARD);
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn test_remove_and_clear() {
        let store = Store::new(2, Eviction::unbounded(), Duration::ZERO);
        for key in 0..10u64 {
            assert!(take(&store, key, ms(0), 1));
        }
        assert!(store.remove(&3));
        assert!(!store.remove(&3));
        assert_eq!(store.len(), 9);
        // Every remaining key is still reachable after the swap-removes.
        for key in (0..10u64).filter(|k| *k != 3) {
            assert!(
                store.with_existing(&key, |_| ()).is_some(),
                "lost key {key}"
            );
        }
        store.clear();
        assert!(store.is_empty());
    }
}
