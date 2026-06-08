export type CacheConfig = {
    enabled?: boolean;
    debug?: boolean;
    maxKeys?: number;
    ttl?: number;
    maxItemSizeMB?: number;
    enableSmartInvalidation?: boolean;
    useCryptoHash?: boolean;
    redisDropThreshold?: number;
    memoryDropThreshold?: number;
    memoryThreshold?: number;
    redis?: {
        host?: string;
        port?: number;
        password?: string;
        db?: number;
        keyPrefix?: string;
    };
};
export interface CacheEntry {
    d: any;
    e: number;
    s: number;
    h: number;
    a: number;
    t: number;
    v: number;
}
export interface IndexEntry {
    keys: Set<string>;
    lastModified: number;
}
export interface CacheStats {
    keys: number;
    indexes?: number;
    cachedDataMB: number;
    avgItemSizeMB: number;
    memoryUtilization?: number;
    hits: number;
    misses: number;
    hitRate: number;
    evictions?: number;
    invalidations?: number;
    rssMemoryMB?: number;
    heapUsedMB?: number;
    heapTotalMB?: number;
    maxKeys: number;
    maxItemSizeMB: number;
    ttlSeconds: number;
    smartInvalidation: boolean;
    cacheType: 'memory' | 'redis';
    redisConnected?: boolean;
    redisMemoryUsageMB?: number;
    redisMaxMemoryMB?: number;
    underMemoryPressure?: boolean;
}
export declare const DEFAULT_CONFIG: Required<CacheConfig>;
