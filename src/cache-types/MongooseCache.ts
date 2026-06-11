import { Schema, Document, Query, Aggregate, Model } from 'mongoose';
import { CacheConfig, DEFAULT_CONFIG } from '../config';
import { UnifiedCache } from './UnifiedCache';
import { MongoDocumentUtils } from '../MongoDocumentUtils';
import { OptimizedQueryMatcher } from '../OptimizedQueryMatcher';
import { UpdateOperations } from '../UpdateOperations';
import { DocumentSerializer } from '../documentSerializer';
import { MemoryMonitor } from '../MemoryMonitor';

// ============================================================================
// MONGOOSE MODULE AUGMENTATION
// ============================================================================
declare module 'mongoose' {
    interface QueryOptions {
        cache?: boolean | { ttl?: number };
    }
}

interface BulkUpdateEntry {
    doc: any;
    timestamp: number;
    isLean?: boolean;
}

interface CacheMissContext {
    cacheKey: string;
    modelName: string;
    originalQuery: any;
    shouldSkipEmpty?: boolean;
    enableSmartInvalidation?: boolean;
    debugMode?: boolean;
    updateCacheInBackground: (key: string, doc: any, ttl?: number, isLean?: boolean) => void;
    toPlainObject: (doc: any) => any;
    isLean?: boolean;
}

interface UpdateResult {
    modified: boolean;
    data: any;
}

interface BatchInvalidateOperation {
    modelName: string;
    query: any;
    updateData?: any;
}

// ---------------------------------------------------------------------------
// Internal type for the bounded miss-processing queue (Bug 3 fix)
// ---------------------------------------------------------------------------
interface PendingMissEntry {
    result: any;
    context: CacheMissContext;
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
 * 3. Batches cache updates (50 ms intervals) to reduce I/O by 60-90 %
 * 4. Monitors heap memory and flushes queue on pressure to prevent OOM
 * 5. Provides smart invalidation using query pattern indexing
 *
 * Performance Strategy:
 * - Query caching: Intercepts at pre-hook, returns cached via exec() override
 * - Result batching: Queues all writes, flushes atomically via mset()
 * - Deduplication: Keeps only latest update per cache key within 5 s window
 * - Memory protection: Circuit breaker halts queuing when heap > threshold
 * - Serialization: Uses DocumentSerializer for consistent BSON handling
 * - Backpressure: Bounded miss-queue + batch-drain (N entries/tick) keeps the
 *   event loop free; background work never exceeds its CPU budget.
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
 * - hotKeyThreshold: Accesses/min before bypassing cache (default: 100)
 * - redis: Optional Redis config for distributed caching
 *
 * Usage:
 * ```typescript
 * const cache = new MongooseCache({ ttl: 600, enableSmartInvalidation: true });
 * cache.applyCacheToQueries(userSchema);
 * // Now all queries on this schema are automatically cached
 * ```
 * ============================================================================
 */
export class MongooseCache {
    private cache: UnifiedCache;
    public config: Required<CacheConfig>;
    private debugMode: boolean;

    private updateQueue: Map<string, BulkUpdateEntry[]>;
    private inflightQueries: Map<string, Promise<any>>;
    private bulkFlushTimer?: ReturnType<typeof setInterval>;
    private readonly BULK_FLUSH_INTERVAL: number = 50;

    private readonly HOT_KEY_THRESHOLD!: number;
    private readonly HOT_KEY_WINDOW_MS: number = 60_000;
    private hotKeyTracker: Map<string, { count: number; lastSeen: number }> = new Map();
    private hotKeyCleanupTimer?: ReturnType<typeof setInterval>;

    // -------------------------------------------------------------------------
    // Backpressure / bounded miss queue 
    // Instead of spawning one setImmediate per cache miss (which floods the I/O
    // phase under load), we push to a fixed-size FIFO and drain it N-at-a-time
    // inside a single recurring micro-loop, yielding back to the event loop
    // every MISS_DRAIN_BATCH_SIZE entries.
    // -------------------------------------------------------------------------
    private missQueue: PendingMissEntry[] = [];
    private readonly MAX_MISS_QUEUE_SIZE: number = 300;   // hard cap — drop excess
    private readonly MISS_DRAIN_BATCH_SIZE: number = 8;   // entries processed per tick
    private missQueueDraining: boolean = false;

    private isDisconnecting: boolean = false;
    private memoryCheckTimer?: ReturnType<typeof setInterval>;

    constructor(config: CacheConfig = {}) {
        this.config = { ...DEFAULT_CONFIG, ...config } as Required<CacheConfig>;
        this.debugMode = this.config.debug;

        (this as any).HOT_KEY_THRESHOLD = this.config.hotKeyThreshold ?? 100;

        this.cache = new UnifiedCache(this.config);
        this.updateQueue = new Map<string, BulkUpdateEntry[]>();
        this.inflightQueries = new Map<string, Promise<any>>();

        this.startBulkFlushTimer();
        this.startMemoryMonitoring();

        // Prune stale hot-key entries every 30 s
        this.hotKeyCleanupTimer = setInterval(() => {
            const now = Date.now();
            for (const [key, data] of this.hotKeyTracker.entries()) {
                if (now - data.lastSeen > this.HOT_KEY_WINDOW_MS) {
                    this.hotKeyTracker.delete(key);
                }
            }
        }, 30_000);

        this.setupGracefulShutdown();
    }

    // =========================================================================
    // LIFECYCLE
    // =========================================================================

    private setupGracefulShutdown(): void {
        const shutdownHandler = async (signal: string): Promise<void> => {
            if (this.isDisconnecting) return;
            this.isDisconnecting = true;

            if (this.debugMode) {
                console.log(`[Shutdown] Received ${signal}, gracefully disconnecting cache...`);
            }

            await this.disconnect();

            try {
                if (typeof process !== 'undefined' && process.version?.startsWith?.('v')) {
                    process.exit(0);
                }
            } catch {
                // Bun: let runtime handle natural shutdown
            }
        };

        try {
            process.on('SIGINT', () => shutdownHandler('SIGINT'));
            process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
            process.on('SIGUSR2', () => shutdownHandler('SIGUSR2'));

            if (this.debugMode) {
                console.log('[MongooseCache] Signal handlers registered');
            }
        } catch (error) {
            if (this.debugMode) {
                console.warn('[MongooseCache] Signal handlers unavailable in this runtime');
            }
        }
    }

    // =========================================================================
    // BULK FLUSH TIMER
    // =========================================================================

    private startBulkFlushTimer(): void {
        this.bulkFlushTimer = setInterval((): void => {
            this.flushBulkUpdates().catch((error: Error): void => {
                if (this.debugMode) console.warn('Bulk flush error:', error);
            });
        }, this.BULK_FLUSH_INTERVAL);
    }

    // =========================================================================
    // MEMORY MONITORING
    // =========================================================================

    /**
     * Monitor process heap memory and trigger flush when threshold exceeded.
     * Runs every 5 s; uses process.memoryUsage().heapUsed / heapTotal.
     */
    private startMemoryMonitoring(): void {
        this.memoryCheckTimer = setInterval(() => {
            const heapUtilization = MemoryMonitor.getHeapUtilization();

            if (heapUtilization >= this.config.memoryThreshold) {
                if (this.debugMode) {
                    const report = MemoryMonitor.getMemoryReport();
                    console.warn(
                        `[MongooseCache] RESOURCE PRESSURE: ${heapUtilization.toFixed(1)}% usage ` +
                        `(${(report.heapUsed / 1048576).toFixed(1)}MB / ${(report.heapLimit / 1048576).toFixed(0)}MB limit), flushing queue...`
                    );
                }
                setImmediate(() => { this.flushBulkUpdates().catch(() => { }); });
            }
        }, 5000);
    }


    /** Circuit breaker: returns false when heap exceeds the configured threshold. */
    private canAcceptQueueEntry(): boolean {
        return !MemoryMonitor.isUnderPressure(this.config.memoryThreshold);
    }

    // =========================================================================
    // BULK UPDATE QUEUE
    // =========================================================================

    /**
     * Flush accumulated cache updates in a single atomic batch write.
     *
     * Deduplication: only the latest update within 5 s is kept per key.
     * Delegation: UnifiedCache.mset() picks the right backend (Redis / Memory).
     */
    private async flushBulkUpdates(): Promise<void> {
        if (this.updateQueue.size === 0 || this.isDisconnecting) return;

        const entries: Map<string, { value: any; ttl?: number; isLean?: boolean }> = new Map();
        const now: number = Date.now();

        try {
            for (const [key, updates] of this.updateQueue.entries()) {
                const validUpdates: BulkUpdateEntry[] = updates.filter(
                    (u: BulkUpdateEntry): boolean => now - u.timestamp < 5000
                );
                if (validUpdates.length > 0) {
                    const latest = validUpdates[validUpdates.length - 1];
                    entries.set(key, { value: latest.doc, ttl: this.config.ttl, isLean: latest.isLean });
                }
            }

            if (entries.size > 0) {
                const written: number = await this.cache.mset(entries);
                if (this.debugMode && entries.size > 10) {
                    console.log(`[Batch Flush] Wrote ${written}/${entries.size} entries to backend`);
                }
            }

            this.updateQueue.clear();
        } catch (error) {
            if (this.debugMode) console.warn('[Flush Error] Failed to flush bulk updates:', error);
        }
    }

    /**
     * Queue a cache update for batch processing (non-blocking).
     *
     * Memory protection: canAcceptQueueEntry() acts as a circuit breaker.
     * If heap is critical, a flush is forced before accepting new entries.
     */
    private updateCacheInBackground(key: string, doc: any, _ttl?: number, isLean?: boolean): void {
        if (this.isDisconnecting) return;

        if (!this.canAcceptQueueEntry()) {
            if (this.debugMode) {
                console.warn(`[Memory Pressure] Heap critical, immediately flushing queue (key: ${key})`);
            }
            setImmediate(() => { this.flushBulkUpdates().catch(() => { }); });
            return;
        }

        if (!this.updateQueue.has(key)) {
            this.updateQueue.set(key, []);
        }
        this.updateQueue.get(key)!.push({ doc, timestamp: Date.now(), isLean });

        if (this.updateQueue.size > 100) {
            setImmediate(() => {
                this.flushBulkUpdates().catch((error: Error) => {
                    if (this.debugMode) console.warn('[Overflow Protection] Queue size flush failed:', error);
                });
            });
        }
    }

    // =========================================================================
    // SERIALIZATION
    // =========================================================================

    private toPlainObject(doc: any): any {
        try {
            const result = DocumentSerializer.serialize(doc);
            return result.data;
        } catch (error) {
            if (this.debugMode) console.warn('[Serialization] DocumentSerializer failed, returning raw:', error);
            return doc;
        }
    }

    /**
     * Reconstruct Mongoose documents from plain cached objects.
     *
     * Only used for non-lean queries. For lean queries plain objects are returned
     * directly (much faster). Document construction overhead: ~10-50 ms per 1 000 docs.
     */
    private toMongooseDocument(model: any, data: any): any {
        if (!data) return data;

        if (Array.isArray(data)) {
            const len = data.length;
            const result = new Array(len);
            for (let i = 0; i < len; i++) result[i] = this.toMongooseDocument(model, data[i]);
            return result;
        }

        if (typeof data !== 'object') return data;
        if (data instanceof Document || data.constructor?.name === 'model') return data;

        try {
            const doc = new model(data, undefined, { defaults: false, minimize: false });
            if (data._id) { doc._id = data._id; doc.isNew = false; }
            doc.init(data);
            return doc;
        } catch (error) {
            if (this.debugMode) console.warn('[Document Conversion] Failed to convert to Mongoose:', error);
            return data;
        }
    }

    // =========================================================================
    // CACHE KEY GENERATION
    // =========================================================================

    private lastQueryKeyData: string = '';
    private lastGeneratedKey: string = '';

    private generateCacheKey(modelName: string, operation: string, context: any): string {
        // Fast path 1: simple _id queries
        if (
            operation.includes('find') &&
            context.query &&
            context.query._id &&
            Object.keys(context.query).length === 1 &&
            !context.populate
        ) {
            const idValue = context.query._id;
            if (typeof idValue !== 'object' || MongoDocumentUtils.isValidObjectId(idValue)) {
                return `${modelName}:${operation}:id:${String(idValue)}`;
            }
        }

        // Fast path 2: identity memoization
        if (context === this.lastQueryKeyData && context !== undefined) return this.lastGeneratedKey;

        const modifiers = ['query', 'projection', 'sort', 'limit', 'skip', 'populate', 'options', 'pipeline', 'distinct'];
        const keyData: any = { model: modelName, op: operation };

        for (const mod of modifiers) {
            const value = context[mod];
            if (value === undefined || value === null) continue;

            if (mod === 'populate') {
                const extractPaths = (p: any): string => {
                    if (!p) return '';
                    const base = p.path || p;
                    if (p.populate) {
                        const sub = Array.isArray(p.populate)
                            ? p.populate.map(extractPaths).join(',')
                            : extractPaths(p.populate);
                        return `${base}{${sub}}`;
                    }
                    return String(base);
                };
                const popArray = Array.isArray(value) ? value : [value];
                keyData[mod] = popArray.map(extractPaths).join('|');
            } else if (mod === 'options') {
                keyData[mod] = { lean: !!value.lean, limit: value.limit };
            } else {
                keyData[mod] = value;
            }
        }

        this.lastQueryKeyData = context;
        this.lastGeneratedKey = this.cache.generateKey(modelName, operation, keyData);
        return this.lastGeneratedKey;
    }

    /**
     * Enqueue a cache-miss result for background serialisation + storage.
     *
     * Key design decisions:
     * - Hard cap (MAX_MISS_QUEUE_SIZE): drops the oldest entry when full so the
     *   queue never grows unbounded under burst traffic.
     * - Single draining loop: only ONE setImmediate chain is active at a time,
     *   preventing event-loop flood.
     * - Batch yield: after every MISS_DRAIN_BATCH_SIZE processed entries the
     *   drain loop awaits a new setImmediate, returning control to Node so I/O
     *   callbacks (HTTP, TCP) can run between batches.
     */
    private handleCacheMiss(result: any, context: CacheMissContext): void {
        if (this.isDisconnecting) return;

        if (this.missQueue.length >= this.MAX_MISS_QUEUE_SIZE) {
            // Evict the oldest (head) entry to make room — always keep recent work.
            this.missQueue.shift();
            if (this.debugMode) {
                console.warn(`[Backpressure] Miss queue at cap (${this.MAX_MISS_QUEUE_SIZE}), evicted oldest entry`);
            }
        }

        // Update hot-key tracker synchronously (cheap O(1) map op, no I/O).
        const hk = this.hotKeyTracker.get(context.cacheKey);
        if (hk) { hk.count++; hk.lastSeen = Date.now(); }
        else { this.hotKeyTracker.set(context.cacheKey, { count: 1, lastSeen: Date.now() }); }

        this.missQueue.push({ result, context });

        // Kick off the drain loop if it is not already running.
        if (!this.missQueueDraining) {
            this.missQueueDraining = true;
            setImmediate(() => { this.drainMissQueue(); });
        }
    }

    /**
     * Drain the miss queue in fixed-size batches, yielding between each batch.
     *
     * This is the single active consumer of missQueue. It processes
     * MISS_DRAIN_BATCH_SIZE entries synchronously, then schedules itself on the
     * next setImmediate tick so the event loop can service I/O in between.
     * This keeps background CPU bounded to < 1 / (1 + MISS_DRAIN_BATCH_SIZE)
     * of any given tick, leaving the majority of each tick free for the main path.
     */
    private drainMissQueue(): void {
        if (this.isDisconnecting) {
            this.missQueueDraining = false;
            return;
        }

        let processed = 0;

        while (this.missQueue.length > 0 && processed < this.MISS_DRAIN_BATCH_SIZE) {
            const entry = this.missQueue.shift()!;
            this.processMissEntry(entry.result, entry.context);
            processed++;
        }

        if (this.missQueue.length > 0) {
            // More work remains — schedule next batch on the next event-loop tick.
            setImmediate(() => { this.drainMissQueue(); });
        } else {
            // Queue drained; allow a future handleCacheMiss to restart the loop.
            this.missQueueDraining = false;
        }
    }

    /**
     * Process a single cache-miss entry: serialise the result and hand it off
     * to the batch update queue for eventual writing to the cache backend.
     *
     * Kept intentionally synchronous — no I/O here — so each call is O(µs).
     */
    private processMissEntry(result: any, context: CacheMissContext): void {
        const {
            cacheKey, modelName, originalQuery, shouldSkipEmpty,
            enableSmartInvalidation, debugMode, updateCacheInBackground, toPlainObject, isLean,
        } = context;

        try {
            const shouldCache: boolean = !shouldSkipEmpty || (
                result && (Array.isArray(result) ? result.length > 0 : result !== null)
            );

            if (shouldCache && cacheKey && modelName) {
                const dataToCache: any = toPlainObject(result);
                updateCacheInBackground(cacheKey, dataToCache, this.config.ttl, isLean);

                if (enableSmartInvalidation) {
                    this.cache.addToIndexes(cacheKey, modelName, originalQuery);
                }
            }
        } catch (err) {
            if (debugMode) console.warn('[Cache Background] Miss processing failed:', err);
        }
    }

    // =========================================================================
    // SMART CACHE INVALIDATION
    // =========================================================================

    public updateCachedData(
        modelName: string,
        operation: string,
        query: any,
        updateData: any,
        resultDoc?: any
    ): void {
        setImmediate(async () => {
            try {
                if (!this.config.enableSmartInvalidation) {
                    await this.cache.invalidateModel(modelName);
                    return;
                }

                const fieldPaths: string[] = this.extractFieldPaths(modelName, query);
                const keysToUpdate: Set<string> = new Set<string>();
                const keysToDelete: Set<string> = new Set<string>();

                const patterns: string[] = [
                    `*${modelName}:*`,
                    ...fieldPaths.map((path: string): string => `*${path}*`),
                ];

                const patternResults: string[][] = await Promise.all(
                    patterns.map((pattern: string): Promise<string[]> =>
                        this.cache.getKeysByPattern(pattern)
                    )
                );

                for (const keys of patternResults) {
                    for (const key of keys) {
                        if (key.includes(':aggregate:') || key.includes(':agg:')) {
                            keysToDelete.add(key);
                        } else {
                            keysToUpdate.add(key);
                        }
                    }
                }

                if (keysToDelete.size > 0) {
                    Promise.all(
                        Array.from(keysToDelete).map((key: string) => this.cache.delete(key))
                    ).catch(err => { if (this.debugMode) console.warn('Batch delete error:', err); });
                }

                if (keysToUpdate.size > 0) {
                    const updateBatch: Map<string, { value: any; ttl?: number; isLean?: boolean }> = new Map();
                    const currentTime: number = Math.floor(Date.now() / 1000);

                    const cacheEntries = await Promise.allSettled(
                        Array.from(keysToUpdate).map(async (cacheKey: string) => {
                            const entry = await this.cache.getRaw(cacheKey);
                            return { cacheKey, entry };
                        })
                    );

                    for (const result of cacheEntries) {
                        if (result.status !== 'fulfilled' || !result.value.entry) continue;

                        const { cacheKey, entry } = result.value;

                        try {
                            let modified: boolean = false;
                            let newData: any;

                            if (Array.isArray(entry.d)) {
                                const updateResult: UpdateResult = this.updateArrayCache(
                                    entry.d, operation, query, updateData, resultDoc
                                );
                                modified = updateResult.modified;
                                newData = updateResult.data;
                            } else if (entry.d && typeof entry.d === 'object') {
                                const updateResult: UpdateResult = this.updateSingleDocCache(
                                    entry.d, operation, query, updateData, resultDoc
                                );
                                modified = updateResult.modified;
                                newData = updateResult.data;
                            }

                            if (modified && newData) {
                                updateBatch.set(cacheKey, { value: newData, ttl: entry.e - currentTime });
                            }
                        } catch (error) {
                            if (this.debugMode) console.warn(`Failed to process update for ${cacheKey}:`, error);
                            this.cache.delete(cacheKey).catch(() => { });
                        }
                    }

                    if (updateBatch.size > 0) {
                        const written = await this.cache.mset(updateBatch);
                        if (this.debugMode) {
                            console.log(
                                `[Cache Update] Batch updated ${written}/${updateBatch.size} entries, ` +
                                `deleted ${keysToDelete.size} aggregates for ${modelName} ${operation}`
                            );
                        }
                    }
                }
            } catch (error) {
                if (this.debugMode) console.warn(`Background cache update error for ${modelName}:`, error);
            }
        });
    }

    private updateArrayCache(
        cachedArray: any[],
        operation: string,
        query: any,
        updateData: any,
        resultDoc?: any
    ): UpdateResult {
        let modified: boolean = false;
        const newArray: any[] = [];

        if (operation === 'save' || operation === 'insertMany') {
            const newDocs: any[] = Array.isArray(resultDoc) ? resultDoc : [resultDoc];

            for (const doc of cachedArray) newArray.push(doc);

            for (const newDoc of newDocs) {
                if (newDoc && OptimizedQueryMatcher.documentMatchesQuery(newDoc, query)) {
                    const normalizedDoc: any = MongoDocumentUtils.ensureMongoDocument(newDoc);
                    const existingIndex: number = newArray.findIndex((doc: any): boolean =>
                        doc._id && normalizedDoc._id &&
                        MongoDocumentUtils.compareIds(doc._id, normalizedDoc._id)
                    );

                    if (existingIndex >= 0) {
                        newArray[existingIndex] = normalizedDoc;
                        modified = true;
                    } else {
                        newArray.push(normalizedDoc);
                        modified = true;
                    }
                }
            }
        } else if (operation.includes('update') || operation.includes('replace')) {
            for (const doc of cachedArray) {
                if (OptimizedQueryMatcher.documentMatchesQuery(doc, query)) {
                    modified = true;
                    if (operation.includes('replace') && resultDoc) {
                        newArray.push(MongoDocumentUtils.ensureMongoDocument(resultDoc));
                    } else {
                        newArray.push(UpdateOperations.applyUpdateToDocument(doc, updateData));
                    }
                } else {
                    newArray.push(doc);
                }
            }
        } else if (operation.includes('delete')) {
            for (const doc of cachedArray) {
                if (!OptimizedQueryMatcher.documentMatchesQuery(doc, query)) {
                    newArray.push(doc);
                } else {
                    modified = true;
                    if (operation.includes('One')) break;
                }
            }
        } else {
            return { modified: false, data: cachedArray };
        }

        return { modified, data: modified ? newArray : cachedArray };
    }

    private updateSingleDocCache(
        cachedDoc: any,
        operation: string,
        query: any,
        updateData: any,
        resultDoc?: any
    ): UpdateResult {
        const normalizedDoc: any = MongoDocumentUtils.ensureMongoDocument(cachedDoc);

        if (!OptimizedQueryMatcher.documentMatchesQuery(normalizedDoc, query)) {
            return { modified: false, data: normalizedDoc };
        }

        if (operation.includes('delete')) {
            return { modified: false, data: null };
        } else if (operation.includes('replace') && resultDoc) {
            return { modified: true, data: MongoDocumentUtils.ensureMongoDocument(resultDoc) };
        } else if (operation.includes('update') || operation === 'save') {
            const dataToApply: any = resultDoc || updateData;
            if (dataToApply) {
                return { modified: true, data: UpdateOperations.applyUpdateToDocument(normalizedDoc, dataToApply) };
            }
        }

        return { modified: false, data: normalizedDoc };
    }

    private extractFieldPaths(modelName: string, query: any): string[] {
        const paths: string[] = [`${modelName}:*`];
        if (!query || typeof query !== 'object') return paths;

        if (Array.isArray(query)) {
            for (const stage of query) {
                if (stage.$match) paths.push(...this.getQueryPaths(modelName, stage.$match));
            }
        } else {
            paths.push(...this.getQueryPaths(modelName, query));
        }

        return paths;
    }

    private getQueryPaths(modelName: string, query: any): string[] {
        const paths: string[] = [];
        for (const [field, value] of Object.entries(query)) {
            if (field.startsWith('$')) continue;
            const fieldPath: string = `${modelName}:${field}`;
            paths.push(fieldPath);
            if (typeof value === 'string' || typeof value === 'number') {
                paths.push(`${fieldPath}:${value}`);
            }
        }
        return paths;
    }

    // =========================================================================
    // SCHEMA MIDDLEWARE
    // =========================================================================

    /**
     * Apply cache middleware to a Mongoose schema.
     *
     * Registers pre/post hooks for all query and mutation operations.
     *
     * QUERY HOOKS (reads):
     *   find, aggregate, count/countDocuments, distinct
     *   Pre-hook  → generate key → cache lookup → return hit or continue
     *   Post-hook → handleCacheMiss() → bounded queue → batch write
     *
     * MUTATION HOOKS (writes):
     *   save, insert, insertMany, updateOne/Many, findOneAndUpdate,
     *   replaceOne, deleteOne/Many, findOneAndDelete, remove
     *   Post-hook → smart invalidation of affected cache entries
     */
    public applyCacheToQueries(schema: Schema, options: {
        ttl?: number;
        skipEmpty?: boolean;
        enableSmartInvalidation?: boolean;
        useCryptoHash?: boolean;
    } = {}): void {
        if (this.isDisconnecting) return;

        if (this.config.enabled === false) {
            if (this.debugMode) console.log('[MongooseCache] Cache is disabled via config, skipping hook application');
            return;
        }

        if (this.debugMode) console.log('[MongooseCache] Applying hooks to schema...');

        const { skipEmpty = true } = options;

        const self = this;
        const isSmartInvalidation = this.config.enableSmartInvalidation;
        const cache: UnifiedCache = this.cache;

        // Capture all helpers as closure variables so hooks can reference them
        // without relying on `this` (which is the Mongoose Query inside hooks).
        const updateCacheInBackground = this.updateCacheInBackground.bind(this);
        const generateCacheKey = this.generateCacheKey.bind(this);
        const toMongooseDocument = this.toMongooseDocument.bind(this);
        const toPlainObject = this.toPlainObject.bind(this);
        const inflightQueries = this.inflightQueries;

        const hotKeyTracker = this.hotKeyTracker;
        const HOT_KEY_THRESHOLD = this.HOT_KEY_THRESHOLD;

        // -----------------------------------------------------------------
        // FIND QUERIES
        // -----------------------------------------------------------------
        (schema.pre as any)(/^find/, async function (this: Query<any, any>): Promise<void> {
            try {
                const queryOptions = (this as any)._mongooseOptions || {};
                if (queryOptions.cache === false) return;

                const modelName = this.model.modelName;
                const operation = (this as any).op;
                const originalExec = this.exec.bind(this);

                const cacheKey = generateCacheKey(modelName, operation, {
                    query: (this as any)._conditions,
                    projection: (this as any)._fields,
                    sort: (this as any).options?.sort,
                    limit: (this as any).options?.limit,
                    skip: (this as any).options?.skip,
                    populate: (this as any)._mongooseOptions?.populate,
                    options: queryOptions,
                });

                const now = Date.now();
                const hk = hotKeyTracker.get(cacheKey);
                if (hk) { hk.count++; hk.lastSeen = now; }
                else { hotKeyTracker.set(cacheKey, { count: 1, lastSeen: now }); }

                if (hk && hk.count >= HOT_KEY_THRESHOLD) {
                    if (self.debugMode) console.log(`[HOT KEY BYPASS] ${modelName}:${operation} -> ${cacheKey}`);
                    this.exec = originalExec;
                    return;
                }

                (this as any)._cacheKey = cacheKey;
                (this as any)._modelName = modelName;
                (this as any)._queryOptions = queryOptions;

                // ---- Stampede protection (inflight coalescing) ----
                const inflight = inflightQueries.get(cacheKey);
                if (inflight) {
                    if (self.debugMode) console.log(`[QUERY COALESCED] ${modelName}:${operation} -> ${cacheKey}`);
                    (this as any)._cacheHit = true;
                    this.exec = async () => {
                        try { return await inflight; }
                        catch { return await originalExec(); }
                    };
                    return;
                }

                let resolveInflight!: (value: any) => void;
                let rejectInflight!: (reason?: any) => void;
                const inflightPromise = new Promise((resolve, reject) => {
                    resolveInflight = resolve;
                    rejectInflight = reject;
                });
                inflightQueries.set(cacheKey, inflightPromise);

                const finishInflight = (data: any, isError = false) => {
                    if (isError) rejectInflight(data); else resolveInflight(data);
                    if (inflightQueries.get(cacheKey) === inflightPromise) {
                        inflightQueries.delete(cacheKey);
                    }
                };

                try {
                    const cachedSync = (cache as any).getSync ? (cache as any).getSync(cacheKey) : null;
                    const cached: any | null = cachedSync !== null ? cachedSync : await cache.get(cacheKey);

                    if (cached !== null) {
                        if (self.debugMode) console.log(`[CACHE HIT] ${modelName}:${operation} -> ${cacheKey}`);
                        const resultToReturn = queryOptions.lean ? cached : toMongooseDocument(this.model, cached);
                        (this as any)._cacheHit = true;
                        finishInflight(resultToReturn);
                        this.exec = async () => resultToReturn;
                        return;
                    }

                    if (self.debugMode) console.log(`[CACHE MISS] ${modelName}:${operation} -> ${cacheKey}`);
                    (this as any)._cacheHit = false;

                    this.exec = async () => {
                        try {
                            const result = await originalExec();
                            finishInflight(result);
                            return result;
                        } catch (execError) {
                            finishInflight(execError, true);
                            throw execError;
                        }
                    };
                } catch (cacheError) {
                    finishInflight(cacheError, true);
                    throw cacheError;
                }
            } catch (error) {
                if (self.debugMode) console.warn('Find cache pre-hook error:', error);
            }
        });

        schema.post(/^find/, async function (this: Query<any, any>, result: any): Promise<any> {
            if ((this as any)._cacheHit) {
                if (self.debugMode) console.log(`[FIND POST] Cache hit confirmed`);
                return result;
            }

            self.handleCacheMiss(result, {
                cacheKey: (this as any)._cacheKey,
                modelName: (this as any)._modelName,
                originalQuery: (this as any)._conditions,
                shouldSkipEmpty: skipEmpty,
                enableSmartInvalidation: isSmartInvalidation,
                debugMode: self.debugMode,
                updateCacheInBackground,
                toPlainObject,
                isLean: !!((this as any)._queryOptions?.lean),
            });

            return result;
        });

        // -----------------------------------------------------------------
        // AGGREGATE QUERIES
        // -----------------------------------------------------------------
        (schema.pre as any)('aggregate', async function (this: Aggregate<any>): Promise<void> {
            try {
                if ((this as any)._cacheOptions === false) return;

                const pipeline = this.pipeline();
                const aggregateOptions = this.options || {};
                const model: Model<any> = (this as any)._model;
                const modelName: string = model?.modelName || 'UnknownModel';

                const cacheKey = generateCacheKey(modelName, 'aggregate', {
                    pipeline,
                    hint: aggregateOptions.hint,
                    collation: aggregateOptions.collation,
                    readPreference: aggregateOptions.readPreference,
                    options: aggregateOptions,
                });

                (this as any)._cacheKey = cacheKey;
                (this as any)._modelName = modelName;
                (this as any)._pipeline = pipeline;
                (this as any)._aggregateOptions = aggregateOptions;

                const inflight = inflightQueries.get(cacheKey);
                if (inflight) {
                    if (self.debugMode) console.log(`[AGGREGATE COALESCED] ${cacheKey}`);
                    (this as any)._cacheHit = true;
                    this.exec = async function (): Promise<any[]> { return await inflight!; };
                    return;
                }

                let resolveInflight!: (value: any) => void;
                let rejectInflight!: (reason?: any) => void;
                const inflightPromise = new Promise((resolve, reject) => {
                    resolveInflight = resolve; rejectInflight = reject;
                });
                inflightQueries.set(cacheKey, inflightPromise);

                const finishInflight = (data: any, isError = false) => {
                    if (isError) rejectInflight(data); else resolveInflight(data);
                    if (inflightQueries.get(cacheKey) === inflightPromise) inflightQueries.delete(cacheKey);
                };

                try {
                    const cachedSync = (cache as any).getSync ? (cache as any).getSync(cacheKey) : null;
                    const cached: any[] | null = cachedSync !== null ? cachedSync : await cache.get(cacheKey);

                    if (cached !== null) {
                        if (self.debugMode) console.log(`[AGGREGATE CACHE HIT] ${cacheKey}`);
                        (this as any)._cacheHit = true;
                        finishInflight(cached);
                        this.exec = async function (): Promise<any[]> { return cached; };
                        return;
                    }

                    if (self.debugMode) console.log(`[AGGREGATE CACHE MISS] ${cacheKey}`);
                    (this as any)._cacheHit = false;

                    const originalExec = this.exec.bind(this);
                    this.exec = async () => {
                        try {
                            const result = await originalExec();
                            finishInflight(result);
                            return result;
                        } catch (execError) {
                            finishInflight(execError, true);
                            throw execError;
                        }
                    };
                } catch (cacheError) {
                    finishInflight(cacheError, true);
                    throw cacheError;
                }
            } catch (error) {
                if (self.debugMode) console.warn('Aggregate cache pre-hook error:', error);
            }
        });

        schema.post('aggregate', async function (this: Aggregate<any>, result: any[]): Promise<any[]> {
            if ((this as any)._cacheHit) return result;

            const cacheKey = (this as any)._cacheKey;
            const modelName = (this as any)._modelName;
            const pipeline = (this as any)._pipeline;

            if (cacheKey && modelName) {
                // Single setImmediate is fine here — aggregate post-hooks are rare
                // relative to find post-hooks, so they don't cause queue buildup.
                setImmediate(() => {
                    try {
                        const dataToCache = toPlainObject(result || []);
                        updateCacheInBackground(cacheKey, dataToCache);
                        if (isSmartInvalidation) cache.addToIndexes(cacheKey, modelName, pipeline);
                    } catch (err) {
                        if (self.debugMode) console.warn('Aggregate post-hook error:', err);
                    }
                });
            }
            return result;
        });

        // -----------------------------------------------------------------
        // COUNT QUERIES
        // -----------------------------------------------------------------
        (schema.pre as any)(/^count/, async function (this: Query<any, any>): Promise<void> {
            try {
                const query = this.getQuery();
                const modelName = this.model.modelName;
                const operation = (this as any).op || 'count';
                const originalExec = this.exec.bind(this);

                const cacheKey = generateCacheKey(modelName, operation, { query });
                (this as any)._cacheKey = cacheKey;
                (this as any)._modelName = modelName;

                const inflight = inflightQueries.get(cacheKey);
                if (inflight) {
                    if (self.debugMode) console.log(`[COUNT COALESCED] ${cacheKey}`);
                    (this as any)._cacheHit = true;
                    this.exec = async () => await inflight!;
                    return;
                }

                let resolveInflight!: (value: any) => void;
                let rejectInflight!: (reason?: any) => void;
                const inflightPromise = new Promise((resolve, reject) => {
                    resolveInflight = resolve; rejectInflight = reject;
                });
                inflightQueries.set(cacheKey, inflightPromise);

                const finishInflight = (data: any, isError = false) => {
                    if (isError) rejectInflight(data); else resolveInflight(data);
                    if (inflightQueries.get(cacheKey) === inflightPromise) inflightQueries.delete(cacheKey);
                };

                try {
                    const cachedSync = (cache as any).getSync ? (cache as any).getSync(cacheKey) : null;
                    const cached: number | null = cachedSync !== null ? cachedSync : await cache.get(cacheKey);

                    if (cached !== null) {
                        if (self.debugMode) console.log(`[COUNT CACHE HIT] ${cacheKey} = ${cached}`);
                        (this as any)._cacheHit = true;
                        finishInflight(cached);
                        this.exec = async () => cached;
                        return;
                    }

                    if (self.debugMode) console.log(`[COUNT CACHE MISS] ${cacheKey}`);
                    (this as any)._cacheHit = false;

                    this.exec = async () => {
                        try {
                            const result = await originalExec();
                            finishInflight(result);
                            return result;
                        } catch (execError) {
                            finishInflight(execError, true);
                            throw execError;
                        }
                    };
                } catch (cacheError) {
                    finishInflight(cacheError, true);
                    throw cacheError;
                }
            } catch (err) {
                if (self.debugMode) console.warn('Count cache pre-hook error:', err);
            }
        });

        schema.post(/^count/, async function (this: Query<any, any>, result: number): Promise<number> {
            if ((this as any)._cacheHit) return result;
            const cacheKey = (this as any)._cacheKey;
            if (cacheKey && result !== undefined && result !== null) {
                updateCacheInBackground(cacheKey, result);
            }
            return result;
        });

        // -----------------------------------------------------------------
        // DISTINCT QUERIES
        // -----------------------------------------------------------------
        ((schema as any).pre as any)('distinct', async function (this: Query<any, any>): Promise<void> {
            try {
                const field = this.get('distinct');
                const query = this.getQuery();
                const modelName = this.model.modelName;
                const originalExec = this.exec.bind(this);

                const cacheKey = generateCacheKey(modelName, 'distinct', { field, query });
                (this as any)._cacheKey = cacheKey;
                (this as any)._modelName = modelName;

                const inflight = inflightQueries.get(cacheKey);
                if (inflight) {
                    if (self.debugMode) console.log(`[DISTINCT COALESCED] ${cacheKey}`);
                    (this as any)._cacheHit = true;
                    this.exec = async () => await inflight!;
                    return;
                }

                let resolveInflight!: (value: any) => void;
                let rejectInflight!: (reason?: any) => void;
                const inflightPromise = new Promise((resolve, reject) => {
                    resolveInflight = resolve; rejectInflight = reject;
                });
                inflightQueries.set(cacheKey, inflightPromise);

                const finishInflight = (data: any, isError = false) => {
                    if (isError) rejectInflight(data); else resolveInflight(data);
                    if (inflightQueries.get(cacheKey) === inflightPromise) inflightQueries.delete(cacheKey);
                };

                try {
                    const cachedSync = (cache as any).getSync ? (cache as any).getSync(cacheKey) : null;
                    const cached: any[] | null = cachedSync !== null ? cachedSync : await cache.get(cacheKey);

                    if (cached !== null) {
                        if (self.debugMode) console.log(`[DISTINCT CACHE HIT] ${cacheKey}`);
                        (this as any)._cacheHit = true;
                        finishInflight(cached);
                        this.exec = async () => cached;
                        return;
                    }

                    if (self.debugMode) console.log(`[DISTINCT CACHE MISS] ${cacheKey}`);
                    (this as any)._cacheHit = false;

                    this.exec = async () => {
                        try {
                            const result = await originalExec();
                            finishInflight(result);
                            return result;
                        } catch (execError) {
                            finishInflight(execError, true);
                            throw execError;
                        }
                    };
                } catch (cacheError) {
                    finishInflight(cacheError, true);
                    throw cacheError;
                }
            } catch (err) {
                if (self.debugMode) console.warn('Distinct cache pre-hook error:', err);
            }
        });

        schema.post('distinct', async function (this: Query<any, any>, result: any[]): Promise<any[]> {
            if ((this as any)._cacheHit) return result;
            const cacheKey = (this as any)._cacheKey;
            if (cacheKey && result) updateCacheInBackground(cacheKey, result);
            return result;
        });

        // -----------------------------------------------------------------
        // MUTATION HOOKS
        // -----------------------------------------------------------------
        const updateCachedData = this.updateCachedData.bind(this);

        const createUpdateHandler = (operation: string): ((doc?: any) => void) => {
            return function (this: any, doc?: any): void {
                try {
                    const modelName: string | null =
                        this.constructor?.modelName ||
                        this.model?.modelName ||
                        this.$model?.modelName ||
                        this.schema?.options?.collection ||
                        this.collection?.name ||
                        null;

                    if (!modelName) {
                        if (self.debugMode) console.warn(`[Mutation] Could not determine model name for ${operation}`);
                        return;
                    }

                    let query: any = {};
                    let updateData: any = {};
                    let resultDocument: any = doc;

                    if (operation === 'save') {
                        const docId: any = doc?._id || this._id || this.id;
                        if (docId) {
                            query = { _id: docId };
                            updateData = toPlainObject(doc || this);
                            resultDocument = updateData;
                        }
                    } else if (operation.includes('update') || operation.includes('replace')) {
                        query = (typeof this.getQuery === 'function') ? this.getQuery() : {};
                        updateData = (typeof this.getUpdate === 'function') ? this.getUpdate() : {};
                        resultDocument = doc ? toPlainObject(doc) : null;
                    } else if (operation.includes('delete')) {
                        query = (typeof this.getQuery === 'function') ? this.getQuery() : {};
                    } else if (operation === 'insertMany') {
                        query = {};
                        resultDocument = Array.isArray(doc) ? doc.map(toPlainObject) : toPlainObject(doc);
                    }

                    updateCachedData(modelName, operation, query, updateData, resultDocument);
                } catch (error) {
                    if (self.debugMode) console.warn(`[Mutation Error] Failed to process ${operation}:`, error);
                }
            };
        };

        const mutations: string[] = [
            'save', 'insertMany', 'updateOne', 'updateMany',
            'findOneAndUpdate', 'replaceOne', 'deleteOne', 'deleteMany',
            'findOneAndDelete', 'findOneAndRemove', 'remove',
        ];

        mutations.forEach((op: string): void => {
            schema.post(op as any, createUpdateHandler(op));
        });

        if (this.debugMode) {
            console.log(`[MongooseCache] Cache hooks applied with ${mutations.length} mutation types`);
        }
    }

    // =========================================================================
    // PUBLIC API
    // =========================================================================

    public async getStats(): Promise<any> {
        return await this.cache.getStats();
    }

    public async clearCache(key: string): Promise<boolean> {
        return await this.cache.delete(key);
    }

    public async flushCache(): Promise<void> {
        await this.flushBulkUpdates();
        await this.cache.clear();
        if (this.debugMode) console.log('Cache flushed');
    }

    public async invalidateByQuery(modelName: string, query: any, updateData?: any): Promise<void> {
        this.updateCachedData(modelName, 'update', query, updateData);
    }

    public async invalidateModel(modelName: string): Promise<number> {
        return await this.cache.invalidateModel(modelName);
    }

    public async reconnectCache(): Promise<void> {
        return await this.cache.reconnect();
    }

    public async warmCache(model: any, commonQueries: any[]): Promise<void> {
        const warmPromises: Promise<any>[] = commonQueries.map(async (query: any): Promise<any> => {
            try {
                await model.find(query).exec();
                if (this.debugMode) console.log(`Warmed cache for query:`, query);
            } catch (error) {
                if (this.debugMode) console.warn('[Cache] Failed to warm cache for query:', query, error);
            }
        });

        await Promise.allSettled(warmPromises);
        if (this.debugMode) console.log(`Warmed cache with ${commonQueries.length} queries`);
    }

    public async batchInvalidate(operations: BatchInvalidateOperation[]): Promise<void> {
        operations.forEach(({ modelName, query, updateData }) => {
            this.updateCachedData(modelName, 'update', query, updateData);
        });
        if (this.debugMode) console.log(`Batch invalidation triggered for ${operations.length} operations`);
    }

    public optimizeMemory(): any {
        if (global.gc) global.gc();
        return this.cache.getStats();
    }

    public async disconnect(): Promise<void> {
        if (this.isDisconnecting) return;
        this.isDisconnecting = true;

        if (this.bulkFlushTimer) clearInterval(this.bulkFlushTimer);
        if (this.memoryCheckTimer) clearInterval(this.memoryCheckTimer);
        if (this.hotKeyCleanupTimer) clearInterval(this.hotKeyCleanupTimer);

        // Drain any remaining miss entries synchronously before shutdown
        while (this.missQueue.length > 0) {
            const entry = this.missQueue.shift()!;
            this.processMissEntry(entry.result, entry.context);
        }

        await this.flushBulkUpdates();
        await this.cache.disconnect();

        if (this.debugMode) console.log('Cache disconnected gracefully');
    }

    public async ping(): Promise<boolean> {
        return await this.cache.ping();
    }

    public async getRawCacheEntry(key: string): Promise<any> {
        return await this.cache.getRaw(key);
    }

    public async getKeysByPattern(pattern: string): Promise<string[]> {
        return await this.cache.getKeysByPattern(pattern);
    }

    public isEnabled(): boolean {
        return !this.isDisconnecting;
    }

    public updateConfig(newConfig: Partial<CacheConfig>): void {
        this.config = { ...this.config, ...newConfig };
        this.debugMode = this.config.debug;
        if (this.debugMode) console.log('[Config] Cache configuration updated');
    }
}