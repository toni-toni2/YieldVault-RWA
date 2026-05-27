import type { Request, Response, NextFunction } from 'express';
import { cacheHitCount, cacheMissCount, cacheEvictionCount } from '../metrics';

const MAX_ENTRIES = parseInt(process.env.CACHE_MAX_ENTRIES || '1000', 10);

interface CacheEntry {
  data: unknown;
  expiresAt: number;
  /** Timestamp of last access for LRU eviction */
  lastAccessed: number;
}

/**
 * LRU cache implementation with TTL support.
 * Uses Map for O(1) operations and maintains insertion/access order.
 */
class LRUCache<K, V> {
  private cache: Map<K, V>;
  private maxEntries: number;

  constructor(maxEntries: number) {
    this.cache = new Map<K, V>();
    this.maxEntries = maxEntries;
  }

  get(key: K): V | undefined {
    const value = this.cache.get(key);
    if (value !== undefined) {
      // Move to end (most recently used)
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }

  set(key: K, value: V): void {
    // If key exists, update it
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }

    // Add new entry
    this.cache.set(key, value);

    // Evict if over capacity
    if (this.cache.size > this.maxEntries) {
      // Remove first entry (least recently used)
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) {
        this.cache.delete(firstKey);
        cacheEvictionCount.inc();
      }
    }
  }

  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  clear(): void {
    this.cache.clear();
  }

  size(): number {
    return this.cache.size;
  }

  keys(): IterableIterator<K> {
    return this.cache.keys();
  }

  values(): IterableIterator<V> {
    return this.cache.values();
  }
}

const responseCache = new LRUCache<string, CacheEntry>(MAX_ENTRIES);

export interface CacheOptions {
  ttl: number; // milliseconds
}

function normalizeCacheKey(req: Request): string {
  const baseKey = `${req.method}:${req.path}`;
  const queryEntries = Object.entries(req.query)
    .filter(([, value]) => value !== undefined)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) =>
      Array.isArray(value)
        ? `${key}=${value.slice().sort().join(',')}`
        : `${key}=${value}`,
    );

  return queryEntries.length ? `${baseKey}?${queryEntries.join('&')}` : baseKey;
}

export function cacheMiddleware(options: CacheOptions) {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (req.method !== 'GET') {
      next();
      return;
    }

    const cacheKey = normalizeCacheKey(req);
    const cached = responseCache.get(cacheKey);

    if (cached && cached.expiresAt > Date.now()) {
      // Update last accessed timestamp for LRU ordering
      cached.lastAccessed = Date.now();
      res.setHeader('X-Cache-Hit', 'true');
      cacheHitCount.inc({ method: req.method, route: req.path });
      res.json(cached.data);
      return;
    }

    const originalJson = res.json.bind(res);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res.json = function (data: any) {
      const successResponse = res.statusCode >= 200 && res.statusCode < 300;
      if (successResponse) {
        responseCache.set(cacheKey, {
          data,
          expiresAt: Date.now() + options.ttl,
          lastAccessed: Date.now(),
        });
        cacheMissCount.inc({ method: req.method, route: req.path });
        res.setHeader(
          'Cache-Control',
          `public, max-age=${Math.ceil(options.ttl / 1000)}`,
        );
        res.setHeader('X-Cache-Hit', 'false');
      }
      return originalJson(data);
    } as typeof res.json;

    next();
  };
}

export function invalidateCache(pattern?: string): void {
  if (!pattern) {
    responseCache.clear();
    return;
  }

  const regex = new RegExp(pattern);
  for (const key of responseCache.keys()) {
    if (regex.test(key)) {
      responseCache.delete(key);
    }
  }
}

export function getCacheStats(): { size: number; entries: string[]; maxEntries: number } {
  return {
    size: responseCache.size(),
    maxEntries: MAX_ENTRIES,
    entries: Array.from(responseCache.keys()),
  };
}
