export type CacheConfig = {
    enabled?: boolean; // Enable/disable the cache system
    debug?: boolean; // Enable debug logging
    maxKeys?: number; // Maximum number of cache keys to store
    ttl?: number; // Time-to-live in seconds
    maxItemSizeMB?: number; // Maximum individual item size in MB
    enableSmartInvalidation?: boolean; // Enable intelligent cache invalidation
    useCryptoHash?: boolean; // Use crypto hash for key generation
    redisDropThreshold?: number; // Redis memory threshold (0-100%) to start evicting
    memoryDropThreshold?: number; // In-memory cache threshold (0-100%) to start evicting
    memoryThreshold?: number; // Node.js/Bun heap threshold (0-100%) to trigger flush
    redis?: {
        host?: string; // Redis server host
        port?: number; // Redis server port
        password?: string; // Redis authentication password
        db?: number; // Redis database number
        keyPrefix?: string; // Prefix for all Redis keys
    };
}

export interface CacheEntry {
    d: any; // Serialized data
    e: number; // Expiry timestamp in seconds
    s: number; // Size in bytes
    h: number; // Hit count
    a: number; // Last access timestamp in seconds
    t: number; // Creation timestamp in seconds
    v: number; // Version number
    r?: boolean; // Raw object flag (true = skip deserialization)
}

export interface IndexEntry {
    keys: Set<string>; // Set of indexed keys
    lastModified: number; // Last modification timestamp
}

export interface CacheStats {
    keys: number; // Number of cached keys
    indexes?: number; // Number of indexes
    cachedDataMB: number; // Total cached data size in MB
    avgItemSizeMB: number; // Average item size in MB
    memoryUtilization?: number; // Memory utilization percentage
    hits: number; // Number of cache hits
    misses: number; // Number of cache misses
    hitRate: number; // Cache hit rate percentage
    evictions?: number; // Number of evicted entries
    invalidations?: number; // Number of invalidations
    rssMemoryMB?: number; // RSS memory usage in MB
    heapUsedMB?: number; // V8 heap used in MB
    heapTotalMB?: number; // V8 heap total in MB
    maxKeys: number; // Maximum keys configuration
    maxItemSizeMB: number; // Maximum item size configuration
    ttlSeconds: number; // TTL configuration in seconds
    smartInvalidation: boolean; // Smart invalidation enabled status
    cacheType: 'memory' | 'redis'; // Current cache backend type
    redisConnected?: boolean; // Redis connection status
    redisMemoryUsageMB?: number; // Redis memory usage in MB
    redisMaxMemoryMB?: number; // Redis max memory in MB
    underMemoryPressure?: boolean; // Memory pressure status
}

export const DEFAULT_CONFIG: Required<CacheConfig> = {
    enabled: true,
    debug: false,
    maxKeys: 10000,
    ttl: 300,
    maxItemSizeMB: 10,
    enableSmartInvalidation: true,
    useCryptoHash: false,
    redisDropThreshold: 85,
    memoryDropThreshold: 80,
    memoryThreshold: 90,
    redis: {
        host: 'localhost',
        port: 6379,
        password: undefined,
        db: 0,
        keyPrefix: 'mongoose:cache:',
    },
};
