/**
 * @file idempotency.ts
 * In-process idempotency key store with enhanced observability and admin controls
 * (Issues #457 & #466).
 *
 * New in this revision:
 *  - Per-key metadata: createdAt, lastAccessedAt, replayCount, status
 *  - Observability counters: hits, conflicts, evictions
 *  - inspectKeys(prefix?)  – list keys with their metadata
 *  - deleteKey(key)        – targeted single-key eviction
 *  - clear()               – global flush (restricted to admin callers)
 *  - getMetrics()          – snapshot of hits / conflicts / evictions
 */

import NodeCache from 'node-cache';

// ─── Public Types ─────────────────────────────────────────────────────────────

export interface IdempotentOperationResult<T> {
  statusCode: number;
  body: T;
}

/** Metadata attached to every idempotency key entry. */
export interface IdempotencyKeyMetadata {
  /** ISO-8601 timestamp when the key was first stored. */
  createdAt: string;
  /** ISO-8601 timestamp of the most recent access (read or write). */
  lastAccessedAt: string;
  /** Number of times this key has been replayed (returned cached result). */
  replayCount: number;
  /** Current state of the entry. */
  status: 'pending' | 'completed';
}

/** Summary returned by GET /admin/idempotency/keys. */
export interface IdempotencyKeyInfo {
  key: string;
  metadata: IdempotencyKeyMetadata;
}

/** Snapshot of store-wide observability counters. */
export interface IdempotencyMetrics {
  hits: number;
  conflicts: number;
  evictions: number;
  activeKeys: number;
  pendingKeys: number;
}

// ─── Internal Types ───────────────────────────────────────────────────────────

interface StoredResponse<T> extends IdempotentOperationResult<T> {
  fingerprint: string;
  metadata: IdempotencyKeyMetadata;
}

interface PendingOperation<T> {
  fingerprint: string;
  promise: Promise<StoredResponse<T>>;
  metadata: IdempotencyKeyMetadata;
}

// ─── Errors ───────────────────────────────────────────────────────────────────

export class IdempotencyConflictError extends Error {
  constructor(message = 'Idempotency key already used for a different request body') {
    super(message);
    this.name = 'IdempotencyConflictError';
  }
}

// ─── Store ────────────────────────────────────────────────────────────────────

export class IdempotencyStore {
  private readonly completedResponses: NodeCache;
  private readonly pendingResponses = new Map<string, PendingOperation<unknown>>();

  // Observability counters
  private _hits = 0;
  private _conflicts = 0;
  private _evictions = 0;

  constructor(private readonly ttlMs = 24 * 60 * 60 * 1000) {
    const ttlSeconds = Math.max(1, Math.ceil(this.ttlMs / 1000));
    this.completedResponses = new NodeCache({
      stdTTL: ttlSeconds,
      checkperiod: ttlSeconds,
    });

    // Count automatic TTL evictions for observability
    this.completedResponses.on('expired', (_key: string, _value: unknown) => {
      this._evictions++;
    });
  }

  // ─── Core execute ──────────────────────────────────────────────────────────

  async execute<T>(
    key: string,
    fingerprint: string,
    operation: () => Promise<IdempotentOperationResult<T>>
  ): Promise<{ result: IdempotentOperationResult<T>; replayed: boolean }> {
    const now = new Date().toISOString();

    // 1. Already completed
    const completed = this.completedResponses.get<StoredResponse<T>>(key);
    if (completed) {
      if (completed.fingerprint !== fingerprint) {
        this._conflicts++;
        throw new IdempotencyConflictError();
      }

      // Update metadata in place
      this._hits++;
      completed.metadata.lastAccessedAt = now;
      completed.metadata.replayCount++;
      // Re-set with the remaining TTL (node-cache resets TTL on set; use 0 to keep current)
      this.completedResponses.set(key, completed);

      return {
        result: { statusCode: completed.statusCode, body: completed.body },
        replayed: true,
      };
    }

    // 2. Currently in-flight
    const pendingOperation = this.pendingResponses.get(key) as PendingOperation<T> | undefined;
    if (pendingOperation) {
      if (pendingOperation.fingerprint !== fingerprint) {
        this._conflicts++;
        throw new IdempotencyConflictError();
      }

      this._hits++;
      pendingOperation.metadata.lastAccessedAt = now;
      pendingOperation.metadata.replayCount++;

      const replayed = await pendingOperation.promise;
      return {
        result: { statusCode: replayed.statusCode, body: replayed.body },
        replayed: true,
      };
    }

    // 3. First execution — create entry with initial metadata
    const metadata: IdempotencyKeyMetadata = {
      createdAt: now,
      lastAccessedAt: now,
      replayCount: 0,
      status: 'pending',
    };

    const operationPromise = (async () => {
      const result = await operation();
      const stored: StoredResponse<T> = {
        ...result,
        fingerprint,
        metadata: { ...metadata, status: 'completed', lastAccessedAt: new Date().toISOString() },
      };
      this.completedResponses.set(key, stored, this.ttlMs / 1000);
      return stored;
    })();

    this.pendingResponses.set(key, { fingerprint, promise: operationPromise, metadata });

    try {
      const stored = await operationPromise;
      return {
        result: { statusCode: stored.statusCode, body: stored.body },
        replayed: false,
      };
    } finally {
      this.pendingResponses.delete(key);
    }
  }

  // ─── Inspection ────────────────────────────────────────────────────────────

  /**
   * Returns metadata for all known keys (completed + pending).
   * When `prefix` is supplied only keys that begin with that string are included.
   */
  inspectKeys(prefix?: string): IdempotencyKeyInfo[] {
    const results: IdempotencyKeyInfo[] = [];

    // Completed keys
    for (const key of this.completedResponses.keys()) {
      if (prefix && !key.startsWith(prefix)) continue;
      const entry = this.completedResponses.get<StoredResponse<unknown>>(key);
      if (entry) {
        results.push({ key, metadata: { ...entry.metadata } });
      }
    }

    // Pending keys (not yet in completedResponses)
    for (const [key, pending] of this.pendingResponses.entries()) {
      if (prefix && !key.startsWith(prefix)) continue;
      // Avoid duplicating a key that completed between the two loops
      if (!results.some((r) => r.key === key)) {
        results.push({ key, metadata: { ...pending.metadata } });
      }
    }

    return results;
  }

  // ─── Targeted deletion ─────────────────────────────────────────────────────

  /**
   * Removes a single idempotency key from both the completed and pending stores.
   * Returns `true` if the key existed, `false` if it was not found.
   */
  deleteKey(key: string): boolean {
    const deletedCompleted = this.completedResponses.del(key) > 0;
    const deletedPending = this.pendingResponses.delete(key);

    if (deletedCompleted || deletedPending) {
      this._evictions++;
      return true;
    }
    return false;
  }

  // ─── Global clear (admin only) ─────────────────────────────────────────────

  /**
   * Flushes the entire store.
   * This is a destructive operation — restrict to admin-authenticated callers.
   */
  clear(): void {
    const completedCount = this.completedResponses.keys().length;
    const pendingCount = this.pendingResponses.size;
    this._evictions += completedCount + pendingCount;

    this.completedResponses.flushAll();
    this.pendingResponses.clear();
  }

  // ─── Observability ─────────────────────────────────────────────────────────

  /** Returns a snapshot of store-wide counters. */
  getMetrics(): IdempotencyMetrics {
    return {
      hits: this._hits,
      conflicts: this._conflicts,
      evictions: this._evictions,
      activeKeys: this.completedResponses.keys().length,
      pendingKeys: this.pendingResponses.size,
    };
  }
}

// ─── Singleton ────────────────────────────────────────────────────────────────

export const idempotencyStore = new IdempotencyStore(
  parseInt(process.env.IDEMPOTENCY_KEY_TTL_MS || '86400000', 10)
);

// ─── Fingerprint helper ───────────────────────────────────────────────────────

export function buildIdempotencyFingerprint(payload: unknown): string {
  return stableStringify(payload);
}

function stableStringify(value: unknown): string {
  if (value === null) return 'null';
  if (value instanceof Date) return JSON.stringify(value.toISOString());
  if (typeof value !== 'object') return JSON.stringify(value);

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }

  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  const serialized = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`);
  return `{${serialized.join(',')}}`;
}
