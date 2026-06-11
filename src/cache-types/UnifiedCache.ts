// ============================================================================
// UnifiedCache.ts - Unified Cache Manager
// ============================================================================
// 
// Purpose:
// Provides a unified interface for both memory and Redis caching with
// automatic fallback. Handles Mongoose query result caching with support
// for smart cache invalidation.
//
// Architecture:
// - Primary cache layer (Redis or Memory) based on configuration
// - Fallback to MemoryCache if Redis is unavailable
// - Proper async initialization and health checking
// - Support for both Node.js and Bun runtimes
//
// Key Features:
// - Pattern-based cache key generation (query/pipeline/options hashing)
// - Smart query invalidation using indexes
// - Memory pressure monitoring and eviction
// - Redis connection pooling with health checks
// - Comprehensive statistics and debugging
//
// ============================================================================

import CryptoUtil from '../CryptoUtil';
import { CacheConfig, CacheStats, DEFAULT_CONFIG } from '../config';
import { MemoryCache } from './MemoryCache';
import { RedisAdapter } from '../adapters/RedisAdapter';
import PipelineHashGenerator from '../PipelineHashGenerator';
import { MemoryMonitor } from '../MemoryMonitor';

/**
 * UnifiedCache - Main cache orchestration class
 *
 * Manages the lifecycle and operations of a tiered caching system with:
 * 1. Redis as the primary distributed cache (if configured)
 * 2. In-memory cache as a fallback or local optimization layer
 *
 * This class handles initialization, health monitoring, and graceful
 * degradation when Redis is unavailable.
 */
export class UnifiedCache {
    private config: Required<CacheConfig>;
    private memoryCache: MemoryCache;
    private redisAdapter: RedisAdapter | null = null;

    // Redis state management
    private useRedis: boolean = false;
    private redisInitialized: boolean = false;
    private redisInitializing: boolean = false;
    private redisInitPromise: Promise<boolean> | null = null;

    /**
     * Constructor
     *
     * Initializes the unified cache with provided configuration.
     * Merges user config with defaults and determines cache strategy:
     * - If Redis config is provided → attempts Redis initialization
     * - Falls back to memory cache if Redis unavailable
     *
     * @param config - Partial CacheConfig to override defaults
     *
     * Note: Redis initialization is async and begins in the background.
     * Use ensureRedisReady() before operations requiring Redis to be live.
     */
    constructor(config: CacheConfig = {}) {
        this.config = { ...DEFAULT_CONFIG, ...config } as Required<CacheConfig>;

        // Always initialize memory cache as fallback (works in all runtimes)
        this.memoryCache = new MemoryCache(this.config);

        // Determine if we should attempt Redis based on config presence
        // Redis is enabled if config.redis is provided (not default)
        this.useRedis = this.isRedisConfigured();

        if (this.useRedis) {
            // Start Redis initialization in the background (non-blocking)
            this.redisInitPromise = this.initializeRedis();
        }

        if (this.config.debug) {
            const strategy = this.useRedis ? 'Redis (with memory fallback)' : 'Memory only';
            console.log(`[UnifiedCache] Initialized with strategy: ${strategy}`);
            console.log(`[UnifiedCache] Memory Threshold: ${this.config.memoryThreshold}%`);
            console.log(`[UnifiedCache] Crypto implementation: ${CryptoUtil.getImplementation()}`);
        }
    }

    /**
     * Check if Redis is configured (not using default)
     * User-provided Redis config indicates intention to use Redis
     */
    private isRedisConfigured(): boolean {
        // Redis is considered "configured" if user explicitly provided redis settings
        // OR if non-default host/port are specified
        return Boolean(
            this.config.redis?.host !== DEFAULT_CONFIG.redis.host ||
            this.config.redis?.port !== DEFAULT_CONFIG.redis.port
        );
    }

    /**
     * Initialize Redis connection with retry logic
     *
     * Attempts to establish Redis connection with exponential backoff.
     * On failure after max attempts, gracefully falls back to memory cache.
     *
     * Process:
     * 1. Check if already initialized (return cached result)
     * 2. Check if initialization in progress (wait for it)
     * 3. Attempt connection with retries (MAX_INIT_ATTEMPTS)
     * 4. On success: mark as initialized, return true
     * 5. On failure: fall back to memory cache, return false
     *
     * Thread-safe: Multiple concurrent calls will wait for first initialization
     *
     * @returns {Promise<boolean>} true if Redis is ready, false if using memory cache
     */
    private async initializeRedis(): Promise<boolean> {
        if (this.redisInitialized) {
            return true;
        }

        if (this.redisInitializing) {
            // Wait for existing initialization attempt
            while (this.redisInitializing) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
            return this.redisInitialized;
        }

        this.redisInitializing = true;

        const MAX_INIT_ATTEMPTS = 3;
        const RETRY_DELAY = 2000; // 2 seconds between retries

        for (let attempt = 1; attempt <= MAX_INIT_ATTEMPTS; attempt++) {
            try {
                if (this.config.debug) {
                    console.log(`[Redis] Connection attempt ${attempt}/${MAX_INIT_ATTEMPTS}...`);
                }

                // Create adapter instance if needed
                if (!this.redisAdapter) {
                    this.redisAdapter = new RedisAdapter(this.config);

                    // Listen for Redis connection events
                    this.redisAdapter.on('error', (error) => {
                        if (this.config.debug) {
                            console.warn(`[Redis] Error: ${error.message}`);
                        }
                    });

                    this.redisAdapter.on('disconnect', () => {
                        if (this.config.debug) {
                            console.warn('[Redis] Disconnected - attempting reconnection...');
                        }
                    });

                    this.redisAdapter.on('reconnect', () => {
                        if (this.config.debug) {
                            console.log('[Redis] Reconnected successfully');
                        }
                    });
                }

                // Attempt connection (RedisAdapter.connect() waits for actual TCP handshake)
                const connected = await this.redisAdapter.connect();

                if (connected) {
                    this.redisInitialized = true;
                    this.redisInitializing = false;

                    if (this.config.debug) {
                        console.log('[Redis] Cache active and ready');
                    }

                    return true;
                }

                // Connection failed, retry if not last attempt
                if (attempt < MAX_INIT_ATTEMPTS) {
                    if (this.config.debug) {
                        console.warn(
                            `[Redis] Connection attempt ${attempt} failed, ` +
                            `retrying in ${RETRY_DELAY}ms...`
                        );
                    }
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                } else {
                    throw new Error('Connection failed on final attempt');
                }

            } catch (error: any) {
                if (this.config.debug) {
                    console.error(
                        `[Redis] Attempt ${attempt} error: ${error.message}`
                    );
                }

                if (attempt >= MAX_INIT_ATTEMPTS) {
                    // All attempts exhausted - clean up and fall back
                    this.redisInitializing = false;
                    this.redisInitialized = false;
                    this.useRedis = false;

                    if (this.redisAdapter) {
                        await this.redisAdapter.disconnect().catch(() => { });
                        this.redisAdapter = null;
                    }

                    if (this.config.debug) {
                        console.error(
                            '[Redis] Initialization failed after all attempts. ' +
                            'Falling back to in-memory cache.'
                        );
                    }

                    return false;
                }
            }
        }

        this.redisInitializing = false;
        return false;
    }

    /**
     * Ensure Redis is ready before performing operations
     *
     * Waits for Redis initialization promise if currently initializing.
     * Returns immediately if already initialized or not using Redis.
     *
     * @param timeoutMs - Maximum time to wait for initialization (default 10 seconds)
     * @returns true if Redis is ready, false if using memory cache or timed out
     */
    private async ensureRedisReady(timeoutMs: number = 10000): Promise<boolean> {
        if (!this.useRedis) {
            return false;
        }

        if (this.redisInitialized && this.redisAdapter) {
            return true;
        }

        // Wait for initialization promise with timeout
        if (this.redisInitPromise) {
            try {
                const result = await Promise.race([
                    this.redisInitPromise,
                    new Promise<boolean>((resolve) =>
                        setTimeout(() => resolve(false), timeoutMs)
                    )
                ]);
                return result;
            } catch {
                return false;
            }
        }

        return false;
    }

    /**
     * Get active cache adapter
     *
     * Returns the appropriate cache implementation based on current state:
     * - RedisAdapter if Redis is initialized and ready
     * - MemoryCache as fallback
     *
     * @returns Active cache implementation (never null)
     */
    private getActiveCache(): MemoryCache | RedisAdapter {
        if (this.useRedis && this.redisAdapter && this.redisInitialized) {
            return this.redisAdapter;
        }
        return this.memoryCache;
    }

    /**
     * Set a cache entry
     *
     * Stores a value in the active cache layer (Redis or Memory).
     * If Redis is configured but not yet initialized, waits for it.
     * Falls back to memory cache if Redis operation fails.
     *
     * @param key - Cache key
     * @param value - Data to cache (will be serialized)
     * @param ttl - Time-to-live in seconds (uses config default if omitted)
     * @returns true if set successful, false on error
     */
    public async set(key: string, value: any, ttl?: number, isLean?: boolean): Promise<boolean> {
        // Ensure Redis is ready before first use (non-blocking wait)
        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                const result = await cache.set(key, value, ttl, isLean);

                // Only fall back if Redis is truly disconnected
                if (!result) {
                    const stats = this.redisAdapter?.getStats();
                    if (!stats?.connected) {
                        if (this.config.debug) {
                            console.warn('[Cache] Redis unavailable for SET, using memory cache');
                        }
                        return this.memoryCache.set(key, value, ttl, isLean);
                    }
                }

                return result;
            } else {
                return cache.set(key, value, ttl, isLean);
            }
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] SET error:', error.message);
            }

            // Final fallback to memory cache
            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.set(key, value, ttl, isLean);
            }

            return false;
        }
    }

    /**
     * Synchronous get for memory-only pathways
     */
    public getSync<T = any>(key: string): T | null {
        if (this.useRedis && this.redisInitialized) return null;
        return this.memoryCache.get<T>(key);
    }

    /**
     * Get a cache entry
     *
     * Retrieves a value from the active cache layer with type inference.
     * Falls back to memory cache if Redis unavailable.
     *
     * @param key - Cache key
     * @returns Cached value or null if not found/expired
     */
    public async get<T = any>(key: string): Promise<T | null> {
        // Ensure Redis is ready before first use
        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                const result = await cache.get<T>(key);

                // Only fall back if Redis is truly disconnected AND result is null
                if (result === null) {
                    const stats = this.redisAdapter?.getStats();
                    if (!stats?.connected) {
                        if (this.config.debug) {
                            console.warn('[Cache] Redis unavailable for GET, checking memory cache');
                        }
                        return this.memoryCache.get<T>(key);
                    }
                }

                return result;
            } else {
                return cache.get<T>(key);
            }
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] GET error:', error.message);
            }

            // Final fallback to memory cache
            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.get<T>(key);
            }

            return null;
        }
    }

    /**
     * Check if key exists in cache
     *
     * @param key - Cache key
     * @returns true if key exists and not expired, false otherwise
     */
    public async has(key: string): Promise<boolean> {
        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                return await cache.has(key);
            } else {
                return cache.has(key);
            }
        } catch (error) {
            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.has(key);
            }
            return false;
        }
    }

    /**
     * Delete a cache entry
     *
     * @param key - Cache key
     * @returns true if deleted successfully, false if not found or error
     */
    public async delete(key: string): Promise<boolean> {
        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                return await cache.delete(key);
            } else {
                return cache.delete(key);
            }
        } catch (error) {
            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.delete(key);
            }
            return false;
        }
    }

    /**
     * Get multiple cache entries
     *
     * Retrieves multiple keys in a single operation.
     * More efficient than repeated get() calls.
     *
     * @param keys - Array of cache keys
     * @returns Map of key -> value pairs (missing keys excluded)
     */
    public async mget(keys: string[]): Promise<Map<string, any>> {
        if (keys.length === 0) {
            return new Map();
        }

        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                return await cache.mget(keys);
            } else {
                return cache.mget(keys);
            }
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] MGET error:', error.message);
            }

            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.mget(keys);
            }

            return new Map();
        }
    }

    /**
     * Set multiple cache entries
     *
     * Stores multiple key-value pairs in a single batch operation.
     * More efficient than repeated set() calls.
     *
     * @param entries - Map of key -> {value, ttl?} pairs
     * @returns Number of successfully set entries
     */
    public async mset(entries: Map<string, { value: any; ttl?: number; isLean?: boolean }>): Promise<number> {
        if (entries.size === 0) {
            return 0;
        }

        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                return await cache.mset(entries);
            } else {
                return cache.mset(entries);
            }
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] MSET error:', error.message);
            }

            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.mset(entries);
            }

            return 0;
        }
    }

    /**
     * Delete entries matching a pattern
     *
     * Performs pattern-based deletion. Patterns support:
     * - * as wildcard (matches any characters)
     * - ? as single character
     *
     * @param pattern - Glob-style pattern (e.g., "User:*", "*:find:*")
     * @returns Number of deleted entries
     */
    public async deletePattern(pattern: string): Promise<number> {
        if (this.useRedis && !this.redisInitialized) {
            await this.ensureRedisReady(5000);
        }

        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                return await cache.deletePattern(pattern);
            } else {
                return cache.deletePattern(pattern);
            }
        } catch (error) {
            if (this.useRedis && this.memoryCache) {
                return this.memoryCache.deletePattern(pattern);
            }
            return 0;
        }
    }

    /**
     * Clear entire cache
     *
     * Removes all entries from both Redis and memory caches.
     * Safe operation - errors in one layer don't prevent clearing the other.
     */
    public async clear(): Promise<void> {
        try {
            if (this.useRedis && this.redisAdapter && this.redisInitialized) {
                await this.redisAdapter.clear();
            }
            this.memoryCache.clear();

            if (this.config.debug) {
                console.log('[Cache] Cache cleared');
            }
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] CLEAR error:', error.message);
            }
        }
    }

    /**
     * Generate cache key from Mongoose query parameters
     *
     * Creates deterministic cache keys by hashing:
     * - Model name and operation
     * - Query/pipeline stage objects
     * - Options (sort, limit, skip, select, populate)
     *
     * Supports both query objects and aggregation pipelines.
     * Optionally uses crypto hashing for consistent short keys.
     *
     * @param modelName - Mongoose model name (e.g., "User")
     * @param op - Operation type (e.g., "find", "findOne", "aggregate")
     * @param query - Query object or aggregation pipeline
     * @param options - Query options (sort, limit, skip, etc.)
     * @returns Cache key string
     *
     * Example:
     *   generateKey("User", "find", {age: {$gt: 18}}, {limit: 10})
     *   // => "User:find:o:sort=1:q:abc123..."
     */
    public generateKey(modelName: string, op: string, query: any, options?: any): string {
        const parts = [modelName, op];

        // Hash relevant options if present
        if (options) {
            const optionKeys = ['sort', 'limit', 'skip', 'select', 'populate'];
            let optionsStr = '';

            for (const key of optionKeys) {
                if (options[key] !== undefined) {
                    optionsStr += `${key}:${typeof options[key] === 'object' ? 'obj' : options[key]}|`;
                }
            }

            if (optionsStr) {
                parts.push(`o:${this.fastHash(optionsStr)}`);
            }
        }

        // Hash query or pipeline
        if (query && typeof query === 'object') {
            if (Array.isArray(query)) {
                // FAST PIPELINE HASH: Use PipelineHashGenerator.generateFastHash which is already optimized
                parts.push(`p:${PipelineHashGenerator.generateFastHash(query)}`);
            } else {
                // FAST QUERY HASH: Avoid deep JSON.stringify for complex queries
                // We use a shallow signature + length for speed, then hash only if necessary
                if (query._id) {
                    parts.push(`q:${String(query._id)}`);
                } else {
                    parts.push(`q:${this.fastHash(query)}`);
                }
            }
        } else if (query !== undefined) {
            parts.push(`q:${String(query)}`);
        } else {
            parts.push('q:all');
        }

        const key = parts.join(':');

        // Use hash if key is too long or hashing is configured
        if (key.length > 120 || this.config.useCryptoHash) {
            return `${modelName}:${op}:h${this.fastHash(key)}`;
        }

        return key;
    }


    /**
     * Fast runtime-agnostic hash for cache keys
     *
     * Uses CryptoUtil for cross-runtime compatibility:
     * - Node.js: Native crypto.createHash('md5') - very fast
     * - Bun: Deterministic simple hash - excellent distribution
     * - WebCrypto: Fast simple hash fallback
     *
     * Takes first 12 characters for balance of uniqueness and length.
     *
     * @param data - Object or string to hash
     * @returns 12-character hash string
     */
    private fastHash(data: any): string {
        if (typeof data === 'string' && data.length < 128) {
            // NANO-HASH for common short strings: avoids crypto overhead (~20μs vs ~300μs)
            let h = 0;
            for (let i = 0; i < data.length; i++) {
                h = Math.imul(31, h) + data.charCodeAt(i) | 0;
            }
            return (h >>> 0).toString(16);
        }

        // Use CryptoUtil for runtime-agnostic hashing
        // Works seamlessly in Node.js (native MD5) and Bun (simple hash)
        return CryptoUtil.hash(data, 12);
    }

    /**
     * Add entry to indexes for smart invalidation
     *
     * Maintains indexes that map queries to cache keys, enabling:
     * - Query-based cache invalidation
     * - Fine-grained updates without clearing entire model cache
     *
     * Only active if enableSmartInvalidation is true.
     *
     * @param cacheKey - The generated cache key
     * @param modelName - Model being cached
     * @param query - Query object used to generate the key
     */
    public addToIndexes(cacheKey: string, modelName: string, query: any): void {
        if (!this.config.enableSmartInvalidation) {
            return;
        }

        if (this.memoryCache) {
            this.memoryCache.addToIndexes(cacheKey, modelName, query);
        }
    }

    /**
     * Invalidate cache entries by query
     *
     * Marks cache entries as invalid based on query pattern matching.
     * Used after update/delete operations to keep cache consistent.
     *
     * Note: updateData parameter was previously used for smart invalidation
     * but is now handled internally by the query matching logic.
     *
     * @param modelName - Model name (e.g., "User")
     * @param updateQuery - Query used to find documents being updated
     * @returns Number of invalidated entries
     *
     * Example:
     *   invalidateByQuery("User", {department: "Engineering"})
     *   // Invalidates all User cache entries matching that query
     */
    public async invalidateByQuery(modelName: string, updateQuery: any): Promise<number> {
        try {
            let invalidatedCount = 0;

            if (this.useRedis && this.redisAdapter && this.redisInitialized) {
                // Pattern-based invalidation for Redis
                const pattern = `${modelName}:*`;
                invalidatedCount = await this.redisAdapter.deletePattern(pattern);
            } else if (this.memoryCache) {
                // Smart query-based invalidation for memory cache
                invalidatedCount = this.memoryCache.invalidateByQuery(modelName, updateQuery);
            }

            if (this.config.debug && invalidatedCount > 0) {
                console.log(
                    `[Cache] Invalidated ${invalidatedCount} entries for ` +
                    `${modelName} (query: ${JSON.stringify(updateQuery)})`
                );
            }

            return invalidatedCount;
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] Invalidation error:', error.message);
            }
            return 0;
        }
    }

    /**
     * Invalidate all cache entries for a model
     *
     * Flushes all cached data for a specific Mongoose model.
     * Used when schema changes or major model updates occur.
     *
     * @param modelName - Model name to invalidate (e.g., "User")
     * @returns Number of invalidated entries
     *
     * Example:
     *   invalidateModel("User")
     *   // Clears all cached User queries
     */
    public async invalidateModel(modelName: string): Promise<number> {
        try {
            const pattern = `${modelName}:*`;
            let invalidatedCount = 0;

            if (this.useRedis && this.redisAdapter && this.redisInitialized) {
                invalidatedCount = await this.redisAdapter.deletePattern(pattern);
            } else if (this.memoryCache) {
                invalidatedCount = this.memoryCache.invalidateModel(modelName);
            }

            if (this.config.debug && invalidatedCount > 0) {
                console.log(
                    `[Cache] Invalidated ${invalidatedCount} entries for model ${modelName}`
                );
            }

            return invalidatedCount;
        } catch (error: any) {
            if (this.config.debug) {
                console.error('[Cache] Model invalidation error:', error.message);
            }
            return 0;
        }
    }

    /**
     * Get comprehensive cache statistics
     *
     * Returns detailed metrics about cache performance and resource usage.
     * Data includes:
     * - Hit/miss ratio and cache effectiveness
     * - Memory utilization and pressure state
     * - Backend status (Redis/Memory)
     * - Configuration values
     * - Process memory information (Node.js/Bun compatible)
     *
     * Thread-safe: Works during initialization without blocking
     * Runtime compatible: Works with both Node.js and Bun
     *
     * @returns CacheStats object conforming to CacheStats interface
     */
    public async getStats(): Promise<CacheStats> {
        // Wait for Redis to be ready if it's initializing (non-blocking)
        if (this.useRedis && !this.redisInitialized && this.redisInitializing) {
            await this.ensureRedisReady(5000);
        }

        // Check if we're actually using Redis and it's initialized
        if (this.useRedis && this.redisAdapter && this.redisInitialized) {
            try {
                const redisStats = this.redisAdapter.getStats();
                const memoryInfo = await this.redisAdapter.getMemoryInfo();
                const processMemory = MemoryMonitor.getMemoryReport();

                // Build comprehensive stats object for Redis backend
                return {
                    // Cache identification
                    cacheType: 'redis',

                    // Redis connection status
                    redisConnected: redisStats.connected,
                    redisMemoryUsageMB: +(memoryInfo.used / 1048576).toFixed(2),
                    redisMaxMemoryMB: memoryInfo.max > 0 ? +(memoryInfo.max / 1048576).toFixed(2) : 30,

                    // Hit/miss metrics (from RedisAdapter)
                    hits: redisStats.hits || 0,
                    misses: redisStats.misses || 0,
                    hitRate: +(redisStats.hitRate || 0).toFixed(2),

                    // Cache size metrics
                    keys: 0, // Would require DBSIZE call - deferred for performance
                    cachedDataMB: +(memoryInfo.used / 1048576).toFixed(2),
                    avgItemSizeMB: 0,
                    memoryUtilization: memoryInfo.max > 0 ? +((memoryInfo.used / memoryInfo.max) * 100).toFixed(2) : 0,

                    // Eviction and invalidation
                    evictions: 0, // Not tracked separately in Redis adapter
                    invalidations: 0, // Tracked at query level
                    underMemoryPressure: redisStats.underMemoryPressure || false,

                    // Configuration snapshot
                    maxKeys: this.config.maxKeys,
                    maxItemSizeMB: this.config.maxItemSizeMB,
                    ttlSeconds: this.config.ttl,
                    smartInvalidation: this.config.enableSmartInvalidation,

                    // Process memory (via Unified MemoryMonitor)
                    heapUsedMB: +(processMemory.heapUsed / 1048576).toFixed(2),
                    heapTotalMB: +(processMemory.heapTotal / 1048576).toFixed(2),
                    rssMemoryMB: +(processMemory.rss / 1048576).toFixed(2),
                } as CacheStats;

            } catch (error: any) {
                if (this.config.debug) {
                    console.error('[Cache] Error getting Redis stats:', error.message);
                }

                // Return error state - DON'T fall back to memory stats
                // This preserves data integrity and signals to caller that Redis is down
                return {
                    cacheType: 'redis',
                    redisConnected: false,
                    hits: 0,
                    misses: 0,
                    hitRate: 0,
                    keys: 0,
                    cachedDataMB: 0,
                    avgItemSizeMB: 0,
                    maxKeys: this.config.maxKeys,
                    maxItemSizeMB: this.config.maxItemSizeMB,
                    ttlSeconds: this.config.ttl,
                    evictions: 0,
                    memoryUtilization: 0,
                    underMemoryPressure: false,
                    smartInvalidation: this.config.enableSmartInvalidation,
                } as CacheStats;
            }
        }

        // Return memory cache stats (and add process memory info)
        // This path is taken when:
        // - Redis is not configured
        // - Redis initialization failed
        // - Redis is temporarily unavailable
        const memoryStats = this.memoryCache.getStats();

        // Supplement with process memory info for consistency
        const processMemory = MemoryMonitor.getMemoryReport();

        return {
            ...memoryStats,
            heapUsedMB: +(processMemory.heapUsed / 1048576).toFixed(2),
            heapTotalMB: +(processMemory.heapTotal / 1048576).toFixed(2),
            rssMemoryMB: +(processMemory.rss / 1048576).toFixed(2),
        } as CacheStats;
    }

    /**
     * Disconnect from cache backends
     *
     * Properly closes Redis connection and cleans up resources.
     * Memory cache is kept alive for potential reconnection.
     *
     * Use reconnect() to reestablish connection.
     */
    public async disconnect(): Promise<void> {
        if (this.redisAdapter) {
            await this.redisAdapter.disconnect();
            this.redisAdapter = null;
            this.redisInitialized = false;
        }

        if (this.config.debug) {
            console.log('[Cache] Disconnected');
        }
    }

    /**
     * Reconnect to Redis cache
     *
     * Attempts to reconnect to Redis if configured and previously connected.
     * No-op if using memory-only cache.
     *
     * Useful for recovering from temporary connection loss or
     * after manual disconnect().
     */
    public async reconnect(): Promise<void> {
        try {
            if (this.useRedis && this.redisAdapter && this.redisInitialized) {
                await this.redisAdapter.reconnect('manual');
            }
        } catch (error) {
            if (this.config.debug) {
                console.warn('[Cache] Reconnection failed:', error);
            }
        }
    }

    /**
     * Health check - ping cache backends
     *
     * Tests connectivity to active cache backend.
     * Returns true if responsive, false if there's an issue.
     *
     * Useful for monitoring cache health in application lifecycle.
     *
     * @returns true if cache is responsive, false otherwise
     */
    public async ping(): Promise<boolean> {
        try {
            if (this.useRedis && this.redisAdapter && this.redisInitialized) {
                return await this.redisAdapter.ping();
            }
            // Memory cache is always responsive
            return true;
        } catch (error) {
            if (this.config.debug) {
                console.warn('[Cache] Ping failed:', error);
            }
            return false;
        }
    }

    /**
     * Get raw serialized cache entry
     *
     * Retrieves the raw entry as stored (serialized).
     * Used for debugging and cache inspection, not for normal use.
     *
     * @param key - Cache key
     * @returns Raw entry object or null if not found
     *
     * Note: Accessing internal cache representation - use with caution
     */
    public async getRaw(key: string): Promise<any> {
        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                // Access Redis client directly for raw value
                const data = await (cache as any).client?.get(key);
                if (data) {
                    return JSON.parse(data);
                }
                return null;
            } else {
                // Access memory cache directly
                return (cache as any).cache.get(key);
            }
        } catch (error) {
            if (this.config.debug) {
                console.error('[Cache] getRaw error:', error);
            }
            return null;
        }
    }

    /**
     * Get all keys matching a pattern
     *
     * Retrieves keys from cache matching a glob-style pattern.
     * Patterns support:
     * - * as wildcard (matches any characters)
     * - ? as single character wildcard
     *
     * @param pattern - Pattern to match (e.g., "User:*", "*:find:*")
     * @returns Array of matching key names
     *
     * Performance note:
     * - Redis: O(N) where N is total keys (uses KEYS command)
     * - Memory: O(N) with regex matching (lighter weight)
     *
     * Caution: On large key sets, this can be slow. Use patterns to limit scope.
     */
    public async getKeysByPattern(pattern: string): Promise<string[]> {
        try {
            const cache = this.getActiveCache();

            if (cache instanceof RedisAdapter) {
                // Use Redis KEYS with proper prefix
                const fullPattern = this.config.redis.keyPrefix + pattern.replace(/^\*/, '');
                const keys = await (cache as any).client?.keys(fullPattern);
                return keys || [];
            } else {
                // Use regex matching for memory cache
                const regex = this.patternToRegex(pattern);
                const matchingKeys: string[] = [];

                for (const [key] of (cache as any).cache.entries()) {
                    if (regex.test(key)) {
                        matchingKeys.push(key);
                    }
                }

                return matchingKeys;
            }
        } catch (error) {
            if (this.config.debug) {
                console.error('[Cache] getKeysByPattern error:', error);
            }
            return [];
        }
    }

    /**
     * Convert glob pattern to regex
     *
     * Converts glob-style patterns (*, ?) to regular expressions.
     * Used for pattern matching in memory cache.
     *
     * @param pattern - Glob pattern
     * @returns RegExp for matching
     */
    private patternToRegex(pattern: string): RegExp {
        const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
        const regexPattern = escaped.replace(/\*/g, '.*').replace(/\?/g, '.');
        return new RegExp(`^${regexPattern}$`);
    }

    /**
     * Check if Redis cache is initialized and ready
     *
     * @returns true if Redis is initialized and connected, false otherwise
     */
    public isRedisReady(): boolean {
        return this.useRedis && this.redisInitialized && this.redisAdapter !== null;
    }

    /**
     * Get current cache backend type
     *
     * @returns 'redis' if using Redis, 'memory' if using memory cache
     */
    public getCacheType(): 'redis' | 'memory' {
        return this.isRedisReady() ? 'redis' : 'memory';
    }
}