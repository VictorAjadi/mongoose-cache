import { EventEmitter } from 'events';
import { CacheConfig } from '../config';
/**
 * Redis adapter with robust connection handling.
 */
export declare class RedisAdapter extends EventEmitter {
    private client;
    private connected;
    private connecting;
    private config;
    private debugMode;
    private hits;
    private misses;
    private errors;
    private readOperations;
    private writeOperations;
    private memoryCheckInterval?;
    private reconnectAttempts;
    private readonly MAX_RECONNECT_ATTEMPTS;
    private lastSuccessfulOperation;
    private healthCheckInterval?;
    private readonly HEALTH_CHECK_INTERVAL;
    private readonly OPERATION_TIMEOUT;
    private writeQueue;
    private writeTimer?;
    private readonly WRITE_BATCH_INTERVAL;
    private readonly MAX_BATCH_SIZE;
    private readCache;
    private readonly READ_CACHE_TTL;
    private readonly MAX_READ_CACHE_SIZE;
    private isUnderMemoryPressure;
    private lastEvictionTime;
    private readonly MIN_EVICTION_INTERVAL;
    private totalCacheSizeMB;
    private redisMaxMemoryBytes;
    constructor(config: Required<CacheConfig>);
    /**
     * Start batch write processor - ONLY AFTER CONNECTION
     */
    private startBatchProcessor;
    /**
     * Start health check monitoring
     */
    private startHealthCheck;
    /**
     * Attempt to reconnect to Redis
     */
    reconnect(mode?: 'in-app' | 'manual'): Promise<void>;
    private flushWriteQueue;
    private cleanReadCache;
    connect(): Promise<boolean>;
    private setupEventHandlers;
    private startMemoryMonitoring;
    getMemoryInfo(): Promise<{
        used: number;
        max: number;
        usagePercentage: number;
        fragmentation: number;
    }>;
    private evictToTarget;
    set(key: string, value: any, ttl?: number): Promise<boolean>;
    get<T = any>(key: string): Promise<T | null>;
    has(key: string): Promise<boolean>;
    delete(key: string): Promise<boolean>;
    deletePattern(pattern: string): Promise<number>;
    mget(keys: string[]): Promise<Map<string, any>>;
    mset(entries: Map<string, {
        value: any;
        ttl?: number;
    }>): Promise<number>;
    clear(): Promise<boolean>;
    getStats(): {
        connected: boolean;
        hits: number;
        misses: number;
        errors: number;
        readOperations: number;
        writeOperations: number;
        totalOperations: number;
        hitRate: number;
        reconnectAttempts: number;
        writeQueueSize: number;
        readCacheSize: number;
        underMemoryPressure: boolean;
        cacheSizeMB: number;
        redisMaxMemoryMB: number;
        redisMemoryUsagePercent: number;
        lastSuccessfulOperation: number;
        timeSinceLastOperation: number;
    };
    resetStats(): void;
    disconnect(): Promise<void>;
    ping(): Promise<boolean>;
}
