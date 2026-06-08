import { EventEmitter } from 'events';
import { CacheConfig } from '../config';
/**
 * Memory cache with threshold-based eviction
 */
export declare class MemoryCache extends EventEmitter {
    private cache;
    private indexes;
    private config;
    private debugMode;
    private currentSize;
    private hits;
    private misses;
    private evictions;
    private invalidations;
    private cleanupTimer?;
    private statsTimer?;
    private memoryCheckTimer?;
    private expiredKeys;
    private readonly MAX_EXPIRED_QUEUE;
    private regexCache;
    private readonly MAX_REGEX_CACHE;
    private isUnderMemoryPressure;
    private lastEvictionTime;
    private readonly MIN_EVICTION_INTERVAL;
    private maxSizeBytes;
    constructor(config: Required<CacheConfig>);
    /**
     * Calculate max cache size based on available memory and configured threshold
     * Works with both Node.js and Bun
     */
    private calculateMaxSize;
    /**
     * Faster periodic cleanup
     */
    private startCleanup;
    /**
     * Monitor memory usage and trigger eviction
     */
    private startMemoryMonitoring;
    private startStatsLogging;
    /**
     * Lazy cleanup with batching
     */
    private cleanup;
    /**
     * Fast inline serialization check with memory pressure awareness
     */
    set(key: string, value: any, ttl?: number): boolean;
    /**
     * Inline expiry check with lazy cleanup
     */
    get<T = any>(key: string): T | null;
    has(key: string): boolean;
    delete(key: string): boolean;
    /**
     * Inline bulk get with minimal allocations
     */
    mget(keys: string[]): Map<string, any>;
    mset(entries: Map<string, {
        value: any;
        ttl?: number;
    }>): number;
    deletePattern(pattern: string): number;
    private patternToRegex;
    clear(): void;
    /**
     * Evict to bring memory usage to safe levels
     * Target: 50% of threshold
     */
    private evictToTarget;
    /**
     * Faster LRU eviction with hybrid scoring
     */
    private evictLRU;
    addToIndexes(cacheKey: string, modelName: string, query: any): void;
    private removeFromIndexes;
    private extractFieldPaths;
    private getQueryPaths;
    invalidateByQuery(modelName: string, updateQuery: any): number;
    invalidateModel(modelName: string): number;
    getStats(): {
        keys: number;
        indexes: number;
        cachedDataMB: number;
        maxCacheMB: number;
        avgItemSizeMB: number;
        memoryUtilization: number;
        hits: number;
        misses: number;
        hitRate: number;
        evictions: number;
        invalidations: number;
        expiredQueueSize: number;
        underMemoryPressure: boolean;
        rssMemoryMB: number;
        heapUsedMB: number;
        heapTotalMB: number;
        maxKeys: number;
        maxItemSizeMB: number;
        ttlSeconds: number;
        smartInvalidation: boolean;
        cacheType: "memory";
    };
    private setupShutdown;
}
