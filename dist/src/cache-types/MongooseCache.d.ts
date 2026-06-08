import { Schema } from 'mongoose';
import { CacheConfig } from '../config';
declare module 'mongoose' {
    interface QueryOptions {
        cache?: boolean | {
            ttl?: number;
        };
    }
}
interface BatchInvalidateOperation {
    modelName: string;
    query: any;
    updateData?: any;
}
/**
 * ============================================================================
 * MongooseCache - Mongoose Query Result Caching Layer
 * ============================================================================
 *
 * Purpose:
 * Transparently caches Mongoose query results (find, aggregate, count, distinct)
 * and intelligently invalidates them on writes (save, update, delete).
 *
 * Architecture:
 * 1. Hooks into Mongoose pre/post middleware to intercept queries and mutations
 * 2. Delegates all cache operations to UnifiedCache (Redis + Memory fallback)
 * 3. Batches cache updates (50ms intervals) to reduce I/O by 60-90%
 * 4. Monitors heap memory and flushes queue on pressure to prevent OOM
 * 5. Provides smart invalidation using query pattern indexing
 *
 * Performance Strategy:
 * - Query caching: Intercepts at pre-hook, returns cached via exec() override
 * - Result batching: Queues all writes, flushes atomically via mset()
 * - Deduplication: Keeps only latest update per cache key within 5s window
 * - Memory protection: Circuit breaker halts queuing when heap > threshold
 * - Serialization: Uses DocumentSerializer for consistent BSON handling
 *
 * Runtime Compatibility:
 * - Node.js: Full support including process signal handlers
 * - Bun: Compatible via runtime detection (conditional process.exit)
 *
 * Configuration (via config):
 * - enabled: Master on/off switch (default: true)
 * - ttl: Time-to-live for cached entries in seconds (default: 300)
 * - maxKeys: Maximum cache keys before LRU eviction (default: 10000)
 * - memoryThreshold: Heap % to trigger queue flush (default: 60)
 * - enableSmartInvalidation: Use query pattern matching (default: true)
 * - redis: Optional Redis config for distributed caching
 * \n * Usage:\n * ```typescript
 * const cache = new MongooseCache({ ttl: 600, enableSmartInvalidation: true });
 * cache.applyCacheToQueries(userSchema);
 * // Now all queries on this schema are automatically cached\n * ```
 * ============================================================================
 */
export declare class MongooseCache {
    private cache;
    config: Required<CacheConfig>;
    private debugMode;
    private updateQueue;
    private bulkFlushTimer?;
    private readonly BULK_FLUSH_INTERVAL;
    private isDisconnecting;
    private memoryCheckTimer?;
    constructor(config?: CacheConfig);
    /**
     * Setup graceful shutdown handlers for both Node.js and Bun runtimes.
     *
     * Handles: SIGINT (Ctrl+C), SIGTERM (termination), SIGUSR2 (nodemon restart)
     * Bun-compatible: Uses runtime detection instead of process.exit()
     */
    private setupGracefulShutdown;
    /**
     * Runtime injection of internal helpers.
     */
    private injectCacheMethods;
    private startBulkFlushTimer;
    /**
     * Monitor process heap memory and trigger flush when threshold exceeded
     *
     * Runs every 5 seconds to check heap utilization. Uses process.memoryUsage().heapUsed
     * divided by heapTotal for accurate per-process memory tracking (not system RAM).
     * Triggers automatic flush when heap usage exceeds the configured threshold (default 60%)
     * to prevent out-of-memory errors.
     */
    private startMemoryMonitoring;
    /**
     * @deprecated Use MemoryMonitor.getHeapLimit directly if needed
     */
    private getHeapLimit;
    /**
     * Check if process heap memory is healthy enough to accept more queue entries
     *
     * Acts as a circuit breaker to prevent unbounded queue growth under memory pressure.
     * Returns false when heap usage exceeds the threshold, forcing an immediate flush.
     */
    private canAcceptQueueEntry;
    /**
     * Flush accumulated cache updates in a single atomic batch write
     *
     * Architecture: Instead of immediately writing each update, they're queued and flushed
     * periodically (every 50ms via startBulkFlushTimer). This batching approach reduces
     * backend I/O by 60-90% in high-traffic scenarios.
     *
     * Deduplication: Keeps only the latest update for each key within a 5-second window.
     * This means rapid successive updates to the same cache key are collapsed into one.
     *
     * Delegation: UnifiedCache.mset() handles both Redis and MemoryCache backends,
     * so this method is pure batch orchestration logic.
     */
    private flushBulkUpdates;
    /**
     * Queue a cache update for batch processing (non-blocking)
     *
     * Design Pattern: Instead of immediately writing to the cache backend, we queue
     * updates and process them in batches. This reduces I/O operations by 60-90%
     * in high-throughput scenarios where many updates arrive within 50ms windows.
     *
     * Memory Protection: Uses canAcceptQueueEntry() as a circuit breaker. If heap
     * is under pressure, forces an immediate flush before accepting new entries.
     * This prevents unbounded queue growth that could cause OOM.
     *
     * @param key - Cache key to update
     * @param doc - Document/value to cache
     * @param ttl - Optional time-to-live (uses config default if omitted)
     */
    private updateCacheInBackground;
    /**
     * Convert Mongoose documents to plain serializable objects
     *
     * Delegates to DocumentSerializer for consistent BSON handling across the app.
     * DocumentSerializer handles: ObjectIds, Dates, Buffers, Decimal128, nested objects,
     * Mongoose document metadata stripping, and circular reference detection.
     *
     */
    private toPlainObject;
    /**
     * Convert plain cached objects back to Mongoose documents
     *
     * Only used for non-lean queries where the client expects full Mongoose document
     * instances with methods and virtuals. Lean queries return plain objects directly (faster).
     *
     * Performance: Document construction is expensive:
     * - Skip defaults validation (user data already hydrated)
     * - Call init() after _id assignment (initializes virtuals/getters)
     * - Pre-allocate arrays instead of pushing
     *
     * For large result sets, this overhead can add 10-50ms per 1000 docs.
     * Recommend using .lean() for read-heavy queries.
     */
    private toMongooseDocument;
    /**
     * Generate a unique cache key for queries/aggregations
     *
     * Delegates to UnifiedCache.generateKey() which handles:
     * - Hashing query/pipeline/options for consistent key generation
     * - Optionally using crypto hashing for deterministic keys across processes
     * - Path-based indexing for smart invalidation
     *
     * This is called from pre-hooks to generate keys before cache lookups.
     */
    private generateCacheKey;
    /**
     * Handle cache miss by queuing the result for caching
     *
     * Called from post-hooks when query results aren't in cache. Checks if results
     * should be cached (respecting skipEmpty config), serializes them, and queues
     * them for batch writing.
     *
     * Also registers results with smart invalidation indexes if enabled, allowing
     * fine-grained cache invalidation later based on query patterns.
     */
    private handleCacheMiss;
    updateCachedData(modelName: string, operation: string, query: any, updateData: any, resultDoc?: any): void;
    private updateArrayCache;
    private updateSingleDocCache;
    private extractFieldPaths;
    private getQueryPaths;
    /**
     * Apply cache middleware to a Mongoose schema
     *
     * Registers pre/post hooks for all query and mutation operations:
     *
     * QUERY HOOKS (Read Operations - use pre-hook for cache lookup):
     * - find: Intercepts all find/findById queries
     * - aggregate: Caches aggregation pipeline results
     * - count/countDocuments: Caches numeric results
     * - distinct: Caches distinct field values
     *
     * MUTATION HOOKS (Write Operations - use post-hook for invalidation):
     * - save, insert, insertMany: Queue new docs for cache write
     * - updateOne/Many, findOneAndUpdate, replaceOne: Smart update matching
     * - deleteOne/Many, findOneAndDelete, remove: Mark for cache eviction
     *
     * HOW PRE-HOOKS WORK:
     * 1. Generate cache key from query/options
     * 2. Lookup in UnifiedCache (Redis or Memory)
     * 3. If hit: Convert to Mongoose docs (unless lean), override exec()
     * 4. If miss: Continue to MongoDB, post-hook will cache result
     *
     * HOW POST-HOOKS WORK:
     * 1. For reads: Serialize results and queue for batch caching
     * 2. For writes: Analyze mutation, invalidate affected cache keys
     * 3. Smart invalidation: Use query pattern matching (e.g., invalidate
     *    all find queries on User where status=active when one is updated)
     *
     * @param schema - Mongoose schema to decorate
     * @param options - Cache behavior options
     */
    applyCacheToQueries(schema: Schema, options?: {
        ttl?: number;
        skipEmpty?: boolean;
        enableSmartInvalidation?: boolean;
        useCryptoHash?: boolean;
    }): void;
    getStats(): Promise<any>;
    clearCache(key: string): Promise<boolean>;
    flushCache(): Promise<void>;
    invalidateByQuery(modelName: string, query: any, updateData?: any): Promise<void>;
    invalidateModel(modelName: string): Promise<number>;
    reconnectCache(): Promise<void>;
    warmCache(model: any, commonQueries: any[]): Promise<void>;
    batchInvalidate(operations: BatchInvalidateOperation[]): Promise<void>;
    optimizeMemory(): any;
    disconnect(): Promise<void>;
    ping(): Promise<boolean>;
    getRawCacheEntry(key: string): Promise<any>;
    getKeysByPattern(pattern: string): Promise<string[]>;
    isEnabled(): boolean;
    updateConfig(newConfig: Partial<CacheConfig>): void;
}
export {};
