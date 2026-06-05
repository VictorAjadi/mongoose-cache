import { EventEmitter } from 'events';
import { HyperHashMap } from '../hyperhashmap';
import { CacheConfig, CacheEntry, IndexEntry } from '../config';
import { SizeCalculator } from '../SizeCalculator';
import { DocumentSerializer } from '../documentSerializer';
import { MemoryMonitor } from '../MemoryMonitor';

/**
 * Memory cache with threshold-based eviction
 */
export class MemoryCache extends EventEmitter {
    private cache: HyperHashMap<string, CacheEntry>;
    private indexes: HyperHashMap<string, IndexEntry>;
    private config: Required<CacheConfig>;
    private debugMode: boolean;
    
    private currentSize: number = 0;
    private hits: number = 0;
    private misses: number = 0;
    private evictions: number = 0;
    private invalidations: number = 0;
    
    private cleanupTimer?: ReturnType<typeof setInterval>;
    private statsTimer?: ReturnType<typeof setInterval>;
    private memoryCheckTimer?: ReturnType<typeof setInterval>;

    // Lazy cleanup queue
    private expiredKeys: Set<string> = new Set();
    private readonly MAX_EXPIRED_QUEUE = 1000;

    // Pre-computed regex cache
    private regexCache: Map<string, RegExp> = new Map();
    private readonly MAX_REGEX_CACHE = 100;

    // Memory pressure tracking
    private isUnderMemoryPressure: boolean = false;
    private lastEvictionTime: number = 0;
    private readonly MIN_EVICTION_INTERVAL = 10000; // 10 seconds

    // Calculated max size in bytes based on threshold
    private maxSizeBytes: number = 0;

    constructor(config: Required<CacheConfig>) {
        super();
        this.config = config;
        this.debugMode = config.debug;
        
        this.cache = new HyperHashMap<string, CacheEntry>(this.config.maxKeys);
        this.indexes = new HyperHashMap<string, IndexEntry>(1024);
        
        // Calculate target memory size from Node.js heap and threshold
        // and start background tasks.
        // Notes for teams: These background tasks are lightweight and
        // intentionally conservative to avoid impacting application
        // throughput in production. They perform small, sampled scans
        // rather than full-cache traversals.
        this.calculateMaxSize();

        this.startCleanup(); // periodic lazy expiry cleanup (batched)
        this.startMemoryMonitoring(); // watch memory pressure and trigger eviction

        if (this.debugMode) {
            console.log('[MemoryCache] Initialized with threshold:', this.config.memoryDropThreshold + '%');
            // Stats logging is verbose; enabled only in debug mode to
            // avoid noisy logs in production.
            this.startStatsLogging();
        }

        // Ensure timers are stopped and memory released on shutdown
        this.setupShutdown();
    }

    /**
     * Calculate max cache size based on available memory and configured threshold
     * Works with both Node.js and Bun
     */
    private calculateMaxSize(): void {
        const totalAvailable = MemoryMonitor.getHeapLimit();
        
        // Use threshold percentage of available memory
        this.maxSizeBytes = Math.floor(totalAvailable * (this.config.memoryDropThreshold / 100));
        
        if (this.debugMode) {
            console.log(
                `[MemoryCache] Component Target: ${(this.maxSizeBytes / 1048576).toFixed(2)}MB ` +
                `(${this.config.memoryDropThreshold}% of ${(totalAvailable / 1048576).toFixed(2)}MB limit)`
            );
        }
    }

    /**
     * Faster periodic cleanup
     */
    private startCleanup(): void {
        this.cleanupTimer = setInterval(() => {
            this.cleanup();
        }, 15000);
    }

    /**
     * Monitor memory usage and trigger eviction
     */
    private startMemoryMonitoring(): void {
        this.memoryCheckTimer = setInterval(() => {
            const dropThreshold = this.config.memoryDropThreshold;
            const processThreshold = this.config.memoryThreshold;
            
            // 1. Internal size check (relative to its own target)
            const internalUsagePercent = this.maxSizeBytes > 0 ? (this.currentSize / this.maxSizeBytes) * 100 : 0;
            
            // 2. Global process heap check (the library's circuit breaker)
            const heapUtilization = MemoryMonitor.getHeapUtilization();
            
            const isInternalPressure = internalUsagePercent >= dropThreshold;
            const isProcessPressure = heapUtilization >= processThreshold;

            if (isInternalPressure || isProcessPressure) {
                this.isUnderMemoryPressure = true;
                
                this.emit('memory-pressure', {
                    current: this.currentSize,
                    max: this.maxSizeBytes,
                    internalPercentage: internalUsagePercent,
                    processPercentage: heapUtilization,
                    source: isProcessPressure ? 'process' : 'internal'
                });

                if (this.debugMode) {
                    console.warn(
                        `[MemoryCache] PRESSURE: Internal ${internalUsagePercent.toFixed(1)}% | ` +
                        `Process ${heapUtilization.toFixed(1)}% (Thresholds: ${dropThreshold}% / ${processThreshold}%)`
                    );
                }

                // Throttle evictions
                const now = Date.now();
                if (now - this.lastEvictionTime >= this.MIN_EVICTION_INTERVAL) {
                    this.lastEvictionTime = now;
                    
                    // Non-blocking eviction
                    setImmediate(() => {
                        this.evictToTarget();
                    });
                }
            } else if (internalUsagePercent >= dropThreshold * 0.9 || heapUtilization >= processThreshold * 0.9) {
                // Approaching threshold, do preventive eviction
                setImmediate(() => {
                    this.evictLRU(Math.floor(this.maxSizeBytes * 0.05)); // Free 5%
                });
            } else {
                this.isUnderMemoryPressure = false;
            }
        }, 5000); // Check every 5 seconds
    }

    private startStatsLogging(): void {
        this.statsTimer = setInterval(() => {
            const stats = this.getStats();
            // Only log when cache has meaningful size to avoid noisy logs
            if (stats.keys > 100 && this.debugMode) {
                console.table({
                    'Keys': stats.keys,
                    'Memory (MB)': stats.cachedDataMB,
                    'Target (MB)': (this.maxSizeBytes / 1048576).toFixed(2),
                    'Usage (%)': stats.memoryUtilization,
                    'Hit Rate (%)': stats.hitRate.toFixed(2),
                    'Evictions': stats.evictions,
                    'Heap (MB)': stats.heapUsedMB
                });
            }
        }, 30000);
    }

    /**
     * Lazy cleanup with batching
     */
    private cleanup(): void {
        const now = Math.floor(Date.now() / 1000);
        
        // Process expired queue first
        if (this.expiredKeys.size > 0) {
            let freedSize = 0;
            const toRemove: string[] = [];

            for (const key of this.expiredKeys) {
                const entry = this.cache.get(key);
                if (entry) {
                    freedSize += entry.s;
                    toRemove.push(key);
                }
            }

            for (const key of toRemove) {
                this.cache.delete(key);
                this.removeFromIndexes(key);
            }

            this.currentSize -= freedSize;
            this.expiredKeys.clear();

            if (this.debugMode && toRemove.length > 0) {
                console.log(`Lazy cleaned ${toRemove.length} entries (${(freedSize / 1048576).toFixed(2)}MB)`);
            }
        }

        // Sample-based cleanup (don't scan entire cache) — iterate with a limit
        // to avoid creating large intermediate arrays.
        const sampleLimit = Math.min(100, this.cache.size);
        let freedSizeSample = 0;
        let seen = 0;

        for (const [key, entry] of this.cache.entries()) {
            if (++seen > sampleLimit) break;
            if (entry.e <= now) {
                this.cache.delete(key);
                this.removeFromIndexes(key);
                freedSizeSample += entry.s;
            }
        }

        this.currentSize -= freedSizeSample;
    }

    /**
     * Fast inline serialization check with memory pressure awareness
     */
    public set(key: string, value: any, ttl?: number, isLean: boolean = false): boolean {
        // Skip writes if under severe memory pressure
        if (this.isUnderMemoryPressure) {
            if (this.debugMode) console.warn(`Skipping write for ${key} due to memory pressure`);
            return false;
        }

        try {
            const isMongooseDoc = !!(value && (value.$__ || value._doc));
            const shouldSerialize = !isLean || isMongooseDoc;
            
            let dataToStore: any;
            let size: number;

            if (shouldSerialize) {
                // Regular path: serialize and sanitize
                const serialized = DocumentSerializer.serialize(value);
                dataToStore = serialized.data;
                size = serialized.size;
            } else {
                // FAST PATH: Direct cache for lean results
                // We use a cheap estimation for size to avoid traversal
                dataToStore = value;
                // Estimate: roughly 50 bytes per key-value pair as a safe buffer
                size = (value && typeof value === 'object') ? Object.keys(value).length * 50 : 100;
            }
            
            const maxItemSize = this.config.maxItemSizeMB * 1048576;
            if (size > maxItemSize) return false;

            // Threshold check
            if (this.maxSizeBytes > 0 && this.currentSize + size > this.maxSizeBytes) {
                this.evictLRU(Math.max(size, Math.floor(this.maxSizeBytes * 0.15)));
            }

            const now = Math.floor(Date.now() / 1000);
            const existing = this.cache.get(key);
            if (existing) this.currentSize -= existing.s;

            const entry: CacheEntry = {
                d: dataToStore,
                e: now + (ttl ?? this.config.ttl),
                s: size,
                h: 0,
                a: now,
                t: now,
                v: 1,
                r: !shouldSerialize
            };

            this.cache.set(key, entry);
            this.currentSize += size;
            return true;
        } catch (error) {
            if (this.debugMode) console.error('MemoryCache SET error:', error);
            return false;
        }
    }

    /**
     * Inline expiry check with lazy cleanup
     */
    public get<T = any>(key: string): T | null {
        const entry = this.cache.get(key);

        if (!entry) {
            this.misses++;
            return null;
        }

        const now = Math.floor(Date.now() / 1000);

        // Fast path: check expiry
        if (entry.e <= now) {
            // Lazy cleanup - add to queue instead of immediate delete
            this.expiredKeys.add(key);
            
            if (this.expiredKeys.size >= this.MAX_EXPIRED_QUEUE) {
                setImmediate(() => this.cleanup());
            }
            
            this.misses++;
            return null;
        }

        // Fast path: update stats inline (no allocation)
        entry.h++;
        entry.a = now;
        this.hits++;

        // FAST PATH: If entry is raw POJO, it's already serialized/sanitized.
        // For maximum performance in Node/Bun, we return it directly. 
        // (Self-correction: Users are advised not to mutate results, common in high-perf libs)
        if (entry.r) {
            return entry.d as T;
        }


        try {
            return DocumentSerializer.deserialize(entry.d) as T;
        } catch (error) {
            if (this.debugMode) {
                console.error('MemoryCache GET deserialization error:', error);
            }
            return entry.d as T;
        }
    }

    public has(key: string): boolean {
        const entry = this.cache.get(key);

        if (!entry) {
            return false;
        }

        const now = Math.floor(Date.now() / 1000);

        if (entry.e <= now) {
            this.expiredKeys.add(key);
            return false;
        }

        return true;
    }

    public delete(key: string): boolean {
        const entry = this.cache.get(key);

        if (entry) {
            this.currentSize -= entry.s;
            this.removeFromIndexes(key);
            this.expiredKeys.delete(key);
            return this.cache.delete(key);
        }

        return false;
    }

    /**
     * Inline bulk get with minimal allocations
     */
    public mget(keys: string[]): Map<string, any> {
        const result = new Map<string, any>();
        const now = Math.floor(Date.now() / 1000);
        const len = keys.length;

        // Tight loop, minimize allocations and branching
        for (let i = 0; i < len; i++) {
            const key = keys[i];
            const entry = this.cache.get(key);

            if (entry && entry.e > now) {
                entry.h++;
                entry.a = now;
                
                try {
                    result.set(key, DocumentSerializer.deserialize(entry.d));
                    this.hits++;
                } catch (error) {
                    if (this.debugMode) {
                        console.error(`MGET deserialization error for ${key}:`, error);
                    }
                    this.misses++;
                }
            } else {
                if (entry) {
                    this.expiredKeys.add(key);
                }
                this.misses++;
            }
        }

        return result;
    }

    public mset(entries: Map<string, { value: any; ttl?: number; isLean?: boolean }>): number {
        // Skip if under memory pressure
        if (this.isUnderMemoryPressure) {
            if (this.debugMode) {
                console.warn('Skipping mset due to memory pressure');
            }
            return 0;
        }

        let successCount = 0;

        for (const [key, { value, ttl, isLean }] of entries) {
            if (this.set(key, value, ttl, isLean)) {
                successCount++;
            }
        }

        return successCount;
    }

    public deletePattern(pattern: string): number {
        let regex = this.regexCache.get(pattern);

        if (!regex) {
            regex = this.patternToRegex(pattern);
            
            if (this.regexCache.size >= this.MAX_REGEX_CACHE) {
                // Clear oldest
                const firstKey = this.regexCache.keys().next().value;
                this.regexCache.delete(firstKey!);
            }
            
            this.regexCache.set(pattern, regex);
        }

        const toDelete: string[] = [];

        for (const [key] of this.cache.entries()) {
            if (regex.test(key)) {
                toDelete.push(key);
            }
        }

        let deletedCount = 0;
        let freedSize = 0;

        for (const key of toDelete) {
            const entry = this.cache.get(key);
            if (entry) {
                freedSize += entry.s;
                this.cache.delete(key);
                this.removeFromIndexes(key);
                this.expiredKeys.delete(key);
                deletedCount++;
            }
        }

        this.currentSize -= freedSize;

        return deletedCount;
    }

    private patternToRegex(pattern: string): RegExp {
        const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&');
        const regexPattern = escaped.replace(/\*/g, '.*').replace(/\?/g, '.');
        return new RegExp(`^${regexPattern}$`);
    }

    public clear(): void {
        this.cache.clear();
        this.indexes.clear();
        this.expiredKeys.clear();
        this.regexCache.clear();
        this.currentSize = 0;
        this.evictions = 0;
        this.invalidations = 0;

        if (this.debugMode) {
            console.log('Cache cleared');
        }
    }

    /**
     * Evict to bring memory usage to safe levels
     * Target: 50% of threshold
     */
    private evictToTarget(): void {
        const threshold = this.config.memoryDropThreshold;
        const targetPercentage = threshold * 0.5;
        const targetSize = Math.floor(this.maxSizeBytes * (targetPercentage / 100));

        if (this.currentSize <= targetSize) {
            if (this.debugMode) {
                console.log(`Memory already at target`);
            }
            return;
        }

        const bytesToFree = this.currentSize - targetSize;

        if (this.debugMode) {
            console.log(
                `Evicting to free ${(bytesToFree / 1048576).toFixed(2)}MB ` +
                `(${((this.currentSize / this.maxSizeBytes) * 100).toFixed(1)}% → ${targetPercentage.toFixed(1)}%)`
            );
        }

        this.evictLRU(bytesToFree);
    }

    /**
     * Faster LRU eviction with hybrid scoring
     */
    private evictLRU(neededSize: number): void {        
        // Sample-based eviction for speed
        const sampleSize = Math.min(Math.ceil(this.cache.size * 0.3), 1000);
        const entries: Array<[string, CacheEntry]> = [];
        let count = 0;

        for (const [key, entry] of this.cache.entries()) {
            entries.push([key, entry]);
            if (++count >= sampleSize) break;
        }

        // Hybrid scoring: prioritize old + rarely used
        entries.sort((a, b) => {
            const scoreA = a[1].a + (a[1].h * 60);
            const scoreB = b[1].a + (b[1].h * 60);
            return scoreA - scoreB;
        });

        let freedSize = 0;
        let i = 0;

        while (freedSize < neededSize && i < entries.length) {
            const [key, entry] = entries[i];
            this.cache.delete(key);
            this.removeFromIndexes(key);
            this.expiredKeys.delete(key);
            freedSize += entry.s;
            this.evictions++;
            i++;
        }

        this.currentSize -= freedSize;

        if (this.debugMode) {
            console.log(`Evicted ${i} entries (${(freedSize / 1048576).toFixed(2)}MB)`);
        }
    }

    public addToIndexes(cacheKey: string, modelName: string, query: any): void {
        if (!this.config.enableSmartInvalidation) {
            return;
        }

        const fieldPaths = this.extractFieldPaths(modelName, query);
        const now = Date.now();

        for (const fieldPath of fieldPaths) {
            let indexEntry = this.indexes.get(fieldPath);

            if (!indexEntry) {
                indexEntry = { keys: new Set(), lastModified: now };
                this.indexes.set(fieldPath, indexEntry);
            }

            indexEntry.keys.add(cacheKey);
            indexEntry.lastModified = now;
        }
    }

    private removeFromIndexes(cacheKey: string): void {
        if (!this.config.enableSmartInvalidation) {
            return;
        }

        for (const [, indexEntry] of this.indexes.entries()) {
            indexEntry.keys.delete(cacheKey);
        }
    }

    private extractFieldPaths(modelName: string, query: any): string[] {
        const paths: string[] = [`${modelName}:*`];

        if (!query || typeof query !== 'object') {
            return paths;
        }

        if (Array.isArray(query)) {
            for (const stage of query) {
                if (stage.$match) {
                    paths.push(...this.getQueryPaths(modelName, stage.$match));
                }
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

            const fieldPath = `${modelName}:${field}`;
            paths.push(fieldPath);

            if (typeof value === 'string' || typeof value === 'number') {
                paths.push(`${fieldPath}:${value}`);
            }
        }

        return paths;
    }

    public invalidateByQuery(modelName: string, updateQuery: any): number {
        if (!this.config.enableSmartInvalidation) {
            return this.invalidateModel(modelName);
        }

        const fieldPaths = this.extractFieldPaths(modelName, updateQuery);
        const keysToInvalidate = new Set<string>();

        for (const fieldPath of fieldPaths) {
            const indexEntry = this.indexes.get(fieldPath);
            if (indexEntry) {
                for (const key of indexEntry.keys) {
                    keysToInvalidate.add(key);
                }
            }
        }

        let invalidatedCount = 0;
        for (const key of keysToInvalidate) {
            if (this.delete(key)) {
                invalidatedCount++;
            }
        }

        this.invalidations += invalidatedCount;
        return invalidatedCount;
    }

    public invalidateModel(modelName: string): number {
        const pattern = `${modelName}:*`;
        const deletedCount = this.deletePattern(pattern);
        this.invalidations += deletedCount;
        return deletedCount;
    }

    public getStats() {
        const memUsage = process.memoryUsage();
        const cacheSize = this.cache.size;
        const totalCachedSize = this.currentSize;

        const totalRequests = this.hits + this.misses;
        const hitRate = totalRequests > 0 ? (this.hits / totalRequests) * 100 : 0;

        return {
            keys: cacheSize,
            indexes: this.indexes.size,
            cachedDataMB: +(totalCachedSize / 1048576).toFixed(2),
            maxCacheMB: +(this.maxSizeBytes / 1048576).toFixed(2),
            avgItemSizeMB: cacheSize > 0 ? +((totalCachedSize / cacheSize) / 1048576).toFixed(3) : 0,
            memoryUtilization: +((totalCachedSize / this.maxSizeBytes) * 100).toFixed(1),
            hits: this.hits,
            misses: this.misses,
            hitRate: +hitRate.toFixed(2),
            evictions: this.evictions,
            invalidations: this.invalidations,
            expiredQueueSize: this.expiredKeys.size,
            underMemoryPressure: this.isUnderMemoryPressure,
            rssMemoryMB: +(memUsage.rss / 1048576).toFixed(2),
            heapUsedMB: +(memUsage.heapUsed / 1048576).toFixed(2),
            heapTotalMB: +(memUsage.heapTotal / 1048576).toFixed(2),
            maxKeys: this.config.maxKeys,
            maxItemSizeMB: this.config.maxItemSizeMB,
            ttlSeconds: this.config.ttl,
            smartInvalidation: this.config.enableSmartInvalidation,
            cacheType: 'memory' as const
        };
    }

    private setupShutdown(): void {
        const cleanup = () => {
            if (this.cleanupTimer) clearInterval(this.cleanupTimer);
            if (this.statsTimer) clearInterval(this.statsTimer);
            if (this.memoryCheckTimer) clearInterval(this.memoryCheckTimer);
            this.clear();
        };

        process.once('SIGTERM', cleanup);
        process.once('SIGINT', cleanup);
        process.once('exit', cleanup);
    }
}