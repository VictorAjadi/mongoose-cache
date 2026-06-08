import { CacheConfig, CacheStats } from '../config';
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
export declare class UnifiedCache {
    private config;
    private memoryCache;
    private redisAdapter;
    private useRedis;
    private redisInitialized;
    private redisInitializing;
    private redisInitPromise;
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
    constructor(config?: CacheConfig);
    /**
     * Check if Redis is configured (not using default)
     * User-provided Redis config indicates intention to use Redis
     */
    private isRedisConfigured;
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
    private initializeRedis;
    /**
     * Ensure Redis is ready before performing operations
     *
     * Waits for Redis initialization promise if currently initializing.
     * Returns immediately if already initialized or not using Redis.
     *
     * @param timeoutMs - Maximum time to wait for initialization (default 10 seconds)
     * @returns true if Redis is ready, false if using memory cache or timed out
     */
    private ensureRedisReady;
    /**
     * Get active cache adapter
     *
     * Returns the appropriate cache implementation based on current state:
     * - RedisAdapter if Redis is initialized and ready
     * - MemoryCache as fallback
     *
     * @returns Active cache implementation (never null)
     */
    private getActiveCache;
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
    set(key: string, value: any, ttl?: number): Promise<boolean>;
    /**
     * Get a cache entry
     *
     * Retrieves a value from the active cache layer with type inference.
     * Falls back to memory cache if Redis unavailable.
     *
     * @param key - Cache key
     * @returns Cached value or null if not found/expired
     */
    get<T = any>(key: string): Promise<T | null>;
    /**
     * Check if key exists in cache
     *
     * @param key - Cache key
     * @returns true if key exists and not expired, false otherwise
     */
    has(key: string): Promise<boolean>;
    /**
     * Delete a cache entry
     *
     * @param key - Cache key
     * @returns true if deleted successfully, false if not found or error
     */
    delete(key: string): Promise<boolean>;
    /**
     * Get multiple cache entries
     *
     * Retrieves multiple keys in a single operation.
     * More efficient than repeated get() calls.
     *
     * @param keys - Array of cache keys
     * @returns Map of key -> value pairs (missing keys excluded)
     */
    mget(keys: string[]): Promise<Map<string, any>>;
    /**
     * Set multiple cache entries
     *
     * Stores multiple key-value pairs in a single batch operation.
     * More efficient than repeated set() calls.
     *
     * @param entries - Map of key -> {value, ttl?} pairs
     * @returns Number of successfully set entries
     */
    mset(entries: Map<string, {
        value: any;
        ttl?: number;
    }>): Promise<number>;
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
    deletePattern(pattern: string): Promise<number>;
    /**
     * Clear entire cache
     *
     * Removes all entries from both Redis and memory caches.
     * Safe operation - errors in one layer don't prevent clearing the other.
     */
    clear(): Promise<void>;
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
    generateKey(modelName: string, op: string, query: any, options?: any): string;
    /**
     * Fast MD5 hash for cache keys
     *
     * Used to create deterministic short hashes of complex objects.
     * Takes first 12 characters of MD5 digest for balance of uniqueness and length.
     *
     * @param data - Object or string to hash
     * @returns 12-character hash string
     */
    private fastHash;
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
    addToIndexes(cacheKey: string, modelName: string, query: any): void;
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
    invalidateByQuery(modelName: string, updateQuery: any): Promise<number>;
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
    invalidateModel(modelName: string): Promise<number>;
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
    getStats(): Promise<CacheStats>;
    /**
     * Disconnect from cache backends
     *
     * Properly closes Redis connection and cleans up resources.
     * Memory cache is kept alive for potential reconnection.
     *
     * Use reconnect() to reestablish connection.
     */
    disconnect(): Promise<void>;
    /**
     * Reconnect to Redis cache
     *
     * Attempts to reconnect to Redis if configured and previously connected.
     * No-op if using memory-only cache.
     *
     * Useful for recovering from temporary connection loss or
     * after manual disconnect().
     */
    reconnect(): Promise<void>;
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
    ping(): Promise<boolean>;
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
    getRaw(key: string): Promise<any>;
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
    getKeysByPattern(pattern: string): Promise<string[]>;
    /**
     * Convert glob pattern to regex
     *
     * Converts glob-style patterns (*, ?) to regular expressions.
     * Used for pattern matching in memory cache.
     *
     * @param pattern - Glob pattern
     * @returns RegExp for matching
     */
    private patternToRegex;
    /**
     * Check if Redis cache is initialized and ready
     *
     * @returns true if Redis is initialized and connected, false otherwise
     */
    isRedisReady(): boolean;
    /**
     * Get current cache backend type
     *
     * @returns 'redis' if using Redis, 'memory' if using memory cache
     */
    getCacheType(): 'redis' | 'memory';
}
