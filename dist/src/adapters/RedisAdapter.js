import Redis from 'ioredis';
import { EventEmitter } from 'events';
import { SizeCalculator } from '../SizeCalculator';
import { DocumentSerializer } from '../documentSerializer';
import { MemoryMonitor } from '../MemoryMonitor';
/**
 * Redis adapter with robust connection handling.
 */
export class RedisAdapter extends EventEmitter {
    client = null;
    connected = false;
    connecting = false;
    config;
    debugMode;
    hits = 0;
    misses = 0;
    errors = 0;
    readOperations = 0;
    writeOperations = 0;
    memoryCheckInterval;
    reconnectAttempts = 0;
    MAX_RECONNECT_ATTEMPTS = 10;
    // Connection health monitoring
    lastSuccessfulOperation = Date.now();
    healthCheckInterval;
    HEALTH_CHECK_INTERVAL = 30000; // 30 seconds
    OPERATION_TIMEOUT = 60000; // 60 seconds of no successful operations triggers reconnect
    // Batch write queue
    writeQueue = new Map();
    writeTimer;
    WRITE_BATCH_INTERVAL = 50;
    MAX_BATCH_SIZE = 100;
    // In-memory read cache
    readCache = new Map();
    READ_CACHE_TTL = 5000;
    MAX_READ_CACHE_SIZE = 1000;
    // Memory pressure tracking
    isUnderMemoryPressure = false;
    lastEvictionTime = 0;
    MIN_EVICTION_INTERVAL = 10000;
    // Total cache size tracking
    totalCacheSizeMB = 0;
    // Detected or configured Redis maxmemory in bytes (0 if unknown/not set)
    redisMaxMemoryBytes = 0;
    constructor(config) {
        super();
        this.config = config;
        this.debugMode = config.debug;
    }
    /**
     * Start batch write processor - ONLY AFTER CONNECTION
     */
    startBatchProcessor() {
        if (this.writeTimer) {
            clearInterval(this.writeTimer);
        }
        this.writeTimer = setInterval(() => {
            this.flushWriteQueue().catch(err => {
                if (this.debugMode) {
                    console.warn('Write queue flush error:', err);
                }
            });
        }, this.WRITE_BATCH_INTERVAL);
        if (this.debugMode) {
            console.log('Batch processor started');
        }
    }
    /**
     * Start health check monitoring
     */
    startHealthCheck() {
        if (this.healthCheckInterval) {
            clearInterval(this.healthCheckInterval);
        }
        this.healthCheckInterval = setInterval(async () => {
            const timeSinceLastOp = Date.now() - this.lastSuccessfulOperation;
            // If no successful operations for too long, try to ping
            if (timeSinceLastOp > this.OPERATION_TIMEOUT) {
                if (this.debugMode) {
                    console.warn(`No successful operations for ${Math.floor(timeSinceLastOp / 1000)}s, checking connection...`);
                }
                const isAlive = await this.ping();
                if (!isAlive && this.connected) {
                    if (this.debugMode) {
                        console.error('Ping failed, connection appears dead. Attempting reconnect...');
                    }
                    this.connected = false;
                    this.reconnect();
                }
            }
        }, this.HEALTH_CHECK_INTERVAL);
        if (this.debugMode) {
            console.log('Health check monitoring started');
        }
    }
    /**
     * Attempt to reconnect to Redis
     */
    async reconnect(mode = 'in-app') {
        if (this.connecting) {
            return;
        }
        if (mode === 'in-app') {
            if (this.reconnectAttempts >= this.MAX_RECONNECT_ATTEMPTS) {
                if (this.debugMode) {
                    console.error('Max reconnect attempts reached, giving up');
                }
                return;
            }
            this.reconnectAttempts++;
        }
        if (this.debugMode) {
            console.log(`Attempting manual reconnect (${this.reconnectAttempts}/${this.MAX_RECONNECT_ATTEMPTS})...`);
        }
        // Disconnect old client
        if (this.client) {
            try {
                await this.client.quit();
            }
            catch {
                // Ignore errors on quit
            }
            this.client = null;
        }
        // Try to reconnect
        await this.connect();
    }
    async flushWriteQueue() {
        if (!this.client || !this.connected || this.writeQueue.size === 0) {
            return;
        }
        if (this.isUnderMemoryPressure) {
            if (this.debugMode) {
                console.warn('Skipping flush due to memory pressure');
            }
            return;
        }
        const batch = new Map(this.writeQueue);
        this.writeQueue.clear();
        try {
            const pipeline = this.client.pipeline();
            const now = Date.now();
            for (const [key, { value, ttl, timestamp }] of batch) {
                if (now - timestamp > 5000)
                    continue;
                try {
                    const serializedValue = DocumentSerializer.serialize(value);
                    const size = SizeCalculator.fastSizeEstimate(serializedValue);
                    if (size <= this.config.maxItemSizeMB * 1048576) {
                        const entry = {
                            d: serializedValue,
                            e: Math.floor(Date.now() / 1000) + ttl,
                            s: size,
                            h: 0,
                            a: Math.floor(Date.now() / 1000),
                            t: Math.floor(Date.now() / 1000),
                            v: 1
                        };
                        const serialized = JSON.stringify(entry);
                        pipeline.setex(key, ttl, serialized);
                        this.readCache.set(key, { entry, timestamp: now });
                    }
                }
                catch (error) {
                    if (this.debugMode) {
                        console.warn(`Batch write error for ${key}:`, error);
                    }
                }
            }
            await pipeline.exec();
            this.lastSuccessfulOperation = Date.now();
            if (this.debugMode && batch.size > 10) {
                console.log(`Flushed ${batch.size} writes in batch`);
            }
        }
        catch (error) {
            if (this.debugMode) {
                console.error('Pipeline flush error:', error);
            }
            this.errors++;
        }
    }
    cleanReadCache() {
        if (this.readCache.size <= this.MAX_READ_CACHE_SIZE)
            return;
        const now = Date.now();
        const toDelete = [];
        for (const [key, { timestamp }] of this.readCache) {
            if (now - timestamp > this.READ_CACHE_TTL) {
                toDelete.push(key);
            }
        }
        const sorted = Array.from(this.readCache.entries())
            .sort((a, b) => a[1].timestamp - b[1].timestamp);
        const deleteCount = Math.max(toDelete.length, this.readCache.size - this.MAX_READ_CACHE_SIZE);
        for (let i = 0; i < deleteCount && i < sorted.length; i++) {
            this.readCache.delete(sorted[i][0]);
        }
    }
    async connect() {
        if (this.connected) {
            return true;
        }
        if (this.connecting) {
            const startWait = Date.now();
            while (this.connecting && Date.now() - startWait < 30000) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
            return this.connected;
        }
        this.connecting = true;
        try {
            const redisOptions = {
                // Use host directly; letting system resolver handle any DNS.
                host: this.config.redis.host,
                port: this.config.redis.port,
                password: this.config.redis.password,
                db: this.config.redis.db,
                keyPrefix: this.config.redis.keyPrefix,
                connectTimeout: 10000, // 10 seconds
                commandTimeout: 5000, // 5 seconds
                retryStrategy: (times) => {
                    if (times > this.MAX_RECONNECT_ATTEMPTS) {
                        if (this.debugMode) {
                            console.error('Max retry attempts reached');
                        }
                        return null; // Stop retrying
                    }
                    const delay = Math.min(times * 1000, 10000);
                    if (this.debugMode) {
                        console.log(`Retry attempt ${times}, waiting ${delay}ms...`);
                    }
                    return delay;
                },
                maxRetriesPerRequest: 3,
                enableReadyCheck: true,
                enableOfflineQueue: false, // CRITICAL: Disable offline queue to fail fast
                lazyConnect: false,
                showFriendlyErrorStack: this.debugMode,
                keepAlive: 30000, // Keep connection alive every 30 seconds
                family: 4, // Force IPv4 to avoid IPv6 issues
                // Additional connection stability options
                reconnectOnError: (err) => {
                    const targetError = 'READONLY';
                    if (err.message.includes(targetError)) {
                        return true; // Reconnect on readonly errors
                    }
                    return false;
                },
                connectionName: 'redis-adapter',
            };
            if (this.debugMode) {
                console.log(`Connecting to Redis at ${this.config.redis.host}:${redisOptions.port}...`);
            }
            this.client = new Redis(redisOptions);
            await new Promise((resolve, reject) => {
                const connectionTimeout = (redisOptions.connectTimeout ?? 10000);
                const timeout = setTimeout(() => {
                    reject(new Error(`Redis connection timeout after ${connectionTimeout}ms`));
                }, connectionTimeout);
                this.client.once('ready', () => {
                    clearTimeout(timeout);
                    this.connected = true;
                    this.connecting = false;
                    this.reconnectAttempts = 0;
                    this.lastSuccessfulOperation = Date.now();
                    if (this.debugMode) {
                        console.log('[RedisAdapter] Connected successfully');
                    }
                    resolve();
                });
                this.client.once('error', (error) => {
                    clearTimeout(timeout);
                    this.connecting = false;
                    if (this.debugMode) {
                        console.error('Redis connection error:', {
                            message: error.message,
                            code: error.code,
                            errno: error.errno,
                            syscall: error.syscall
                        });
                    }
                    reject(error);
                });
            });
            this.setupEventHandlers();
            this.startBatchProcessor();
            this.startMemoryMonitoring();
            this.startHealthCheck();
            return true;
        }
        catch (error) {
            this.connecting = false;
            this.connected = false;
            if (this.debugMode) {
                console.error('Redis connection failed:', {
                    message: error.message,
                    code: error.code,
                    errno: error.errno,
                    syscall: error.syscall
                });
            }
            if (this.client) {
                try {
                    this.client.disconnect();
                }
                catch {
                    // Ignore disconnect errors
                }
                this.client = null;
            }
            this.emit('error', error);
            return false;
        }
    }
    setupEventHandlers() {
        if (!this.client)
            return;
        this.client.on('error', (error) => {
            this.errors++;
            if (this.debugMode) {
                console.error('Redis error:', {
                    message: error.message,
                    code: error.code,
                    errno: error.errno,
                    syscall: error.syscall
                });
            }
            this.emit('error', error);
        });
        this.client.on('close', () => {
            this.connected = false;
            if (this.debugMode) {
                console.warn('Redis connection closed');
            }
            this.emit('disconnect');
        });
        this.client.on('reconnecting', (delay) => {
            this.reconnectAttempts++;
            if (this.debugMode) {
                console.log(`Redis reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})...`);
            }
            if (this.reconnectAttempts >= this.MAX_RECONNECT_ATTEMPTS) {
                this.client?.disconnect();
                if (this.debugMode) {
                    console.error('Max reconnect attempts reached');
                }
            }
        });
        this.client.on('ready', () => {
            this.connected = true;
            this.reconnectAttempts = 0;
            this.lastSuccessfulOperation = Date.now();
            if (this.debugMode) {
                console.log('Redis connection restored');
            }
            if (!this.writeTimer) {
                this.startBatchProcessor();
            }
            if (!this.healthCheckInterval) {
                this.startHealthCheck();
            }
            this.emit('reconnect');
        });
        this.client.on('end', () => {
            this.connected = false;
            if (this.debugMode) {
                console.warn('Redis connection ended');
            }
        });
    }
    startMemoryMonitoring() {
        if (this.memoryCheckInterval) {
            clearInterval(this.memoryCheckInterval);
        }
        this.memoryCheckInterval = setInterval(async () => {
            try {
                const info = await this.getMemoryInfo();
                const redisThreshold = this.config.redisDropThreshold;
                const processThreshold = this.config.memoryThreshold;
                const heapUtilization = MemoryMonitor.getHeapUtilization();
                const isRedisPressure = info.usagePercentage >= redisThreshold;
                const isProcessPressure = heapUtilization >= processThreshold;
                if (isRedisPressure || isProcessPressure) {
                    this.isUnderMemoryPressure = true;
                    this.emit('memory-pressure', {
                        ...info,
                        processPercentage: heapUtilization,
                        source: isProcessPressure ? 'process' : 'redis'
                    });
                    if (this.debugMode) {
                        console.warn(`[RedisAdapter] PRESSURE: Redis ${info.usagePercentage.toFixed(1)}% | ` +
                            `Process ${heapUtilization.toFixed(1)}% (Thresholds: ${redisThreshold}% / ${processThreshold}%)`);
                    }
                    const now = Date.now();
                    if (now - this.lastEvictionTime >= this.MIN_EVICTION_INTERVAL) {
                        this.lastEvictionTime = now;
                        setImmediate(() => {
                            this.evictToTarget().catch((err) => {
                                if (this.debugMode) {
                                    console.error('Eviction error:', err);
                                }
                            });
                        });
                    }
                }
                else {
                    this.isUnderMemoryPressure = false;
                }
            }
            catch (error) {
                if (this.debugMode) {
                    console.warn('Memory check failed:', error.message);
                }
            }
        }, 10000);
    }
    async getMemoryInfo() {
        if (!this.client || !this.connected) {
            return { used: 0, max: 0, usagePercentage: 0, fragmentation: 0 };
        }
        try {
            const info = await this.client.info('memory');
            this.lastSuccessfulOperation = Date.now();
            const lines = info.split('\r\n');
            let usedMemory = 0;
            let maxMemory = 0;
            let fragRatio = 1.0;
            for (const line of lines) {
                if (line.startsWith('used_memory:')) {
                    const value = line.split(':')[1]?.trim();
                    usedMemory = value ? parseInt(value) : 0;
                }
                else if (line.startsWith('maxmemory:')) {
                    const value = line.split(':')[1]?.trim();
                    maxMemory = value ? parseInt(value) : 0;
                }
                else if (line.startsWith('mem_fragmentation_ratio:')) {
                    const value = line.split(':')[1]?.trim();
                    fragRatio = value ? parseFloat(value) : 1.0;
                }
            }
            // If maxmemory=0, use configured limit or default to 30MB (free tier)
            if (maxMemory === 0) {
                // Try to read maxmemory from redis CONFIG if not set in INFO
                try {
                    const cfg = await this.client.config('GET', 'maxmemory');
                    // cfg = ['maxmemory', '<value>']
                    if (Array.isArray(cfg) && cfg.length === 2) {
                        const cfgVal = parseInt(cfg[1]);
                        if (!isNaN(cfgVal) && cfgVal > 0) {
                            maxMemory = cfgVal;
                        }
                    }
                }
                catch {
                    // ignore
                }
                // If still not set, fall back to configured hint or sensible default
                if (maxMemory === 0) {
                    const configuredMB = (this.config.redis && this.config.redis.maxMemoryMB) || 0;
                    if (configuredMB > 0) {
                        maxMemory = configuredMB * 1024 * 1024;
                    }
                    else {
                        // Use a safer default for production rather than 30MB
                        maxMemory = 512 * 1024 * 1024; // 512MB
                    }
                }
            }
            // store detected/configured maxmemory for stats
            this.redisMaxMemoryBytes = maxMemory;
            const usagePercentage = maxMemory > 0 ? (usedMemory / maxMemory) * 100 : 0;
            // Update total cache size
            this.totalCacheSizeMB = usedMemory / (1024 * 1024);
            return {
                used: usedMemory,
                max: maxMemory,
                usagePercentage,
                fragmentation: fragRatio
            };
        }
        catch (error) {
            return { used: 0, max: 0, usagePercentage: 0, fragmentation: 0 };
        }
    }
    async evictToTarget() {
        if (!this.client || !this.connected) {
            return 0;
        }
        try {
            const memInfo = await this.getMemoryInfo();
            const threshold = this.config.redisDropThreshold;
            const targetPercentage = threshold * 0.5;
            if (memInfo.usagePercentage <= targetPercentage) {
                return 0;
            }
            const currentBytes = memInfo.used;
            const targetBytes = (memInfo.max * targetPercentage) / 100;
            const bytesToFree = currentBytes - targetBytes;
            if (bytesToFree <= 0) {
                return 0;
            }
            const pattern = this.config.redis.keyPrefix + '*';
            const keys = await this.client.keys(pattern);
            if (keys.length === 0) {
                return 0;
            }
            const sampleSize = Math.min(Math.ceil(keys.length * 0.5), 5000);
            const sampledKeys = keys.slice(0, sampleSize);
            const entries = [];
            const pipeline = this.client.pipeline();
            for (const key of sampledKeys) {
                pipeline.get(key);
            }
            const results = await pipeline.exec();
            this.lastSuccessfulOperation = Date.now();
            for (let i = 0; i < results.length; i++) {
                const [err, data] = results[i];
                if (!err && data) {
                    try {
                        const entry = JSON.parse(data);
                        entries.push({
                            key: sampledKeys[i],
                            accessTime: entry.a,
                            size: entry.s
                        });
                    }
                    catch {
                        continue;
                    }
                }
            }
            entries.sort((a, b) => a.accessTime - b.accessTime);
            let freedBytes = 0;
            const toEvict = [];
            for (const entry of entries) {
                toEvict.push(entry.key);
                freedBytes += entry.size;
                if (freedBytes >= bytesToFree) {
                    break;
                }
            }
            if (toEvict.length > 0) {
                const deletePipeline = this.client.pipeline();
                for (const key of toEvict) {
                    deletePipeline.del(key);
                    this.writeQueue.delete(key);
                    this.readCache.delete(key);
                }
                await deletePipeline.exec();
                this.lastSuccessfulOperation = Date.now();
                if (this.debugMode) {
                    console.log(`Evicted ${toEvict.length} entries, ` +
                        `freed ${(freedBytes / 1048576).toFixed(2)}MB`);
                }
                return toEvict.length;
            }
            return 0;
        }
        catch (error) {
            this.errors++;
            if (this.debugMode) {
                console.error('Redis eviction error:', error.message);
            }
            return 0;
        }
    }
    async set(key, value, ttl) {
        if (!this.client || !this.connected) {
            return false;
        }
        if (this.isUnderMemoryPressure) {
            if (this.debugMode) {
                console.warn(`Skipping write for ${key} due to memory pressure`);
            }
            return false;
        }
        this.writeOperations++;
        try {
            const effectiveTtl = ttl ?? this.config.ttl;
            this.writeQueue.set(key, {
                value,
                ttl: effectiveTtl,
                timestamp: Date.now()
            });
            if (this.writeQueue.size >= this.MAX_BATCH_SIZE) {
                setImmediate(() => {
                    this.flushWriteQueue().catch(() => { });
                });
            }
            return true;
        }
        catch (error) {
            this.errors++;
            if (this.debugMode) {
                console.error(`Redis SET error for ${key}:`, error.message);
            }
            return false;
        }
    }
    async get(key) {
        if (!this.client || !this.connected) {
            this.readOperations++;
            this.misses++;
            return null;
        }
        this.readOperations++;
        const cached = this.readCache.get(key);
        if (cached) {
            const now = Date.now();
            const age = now - cached.timestamp;
            if (age < this.READ_CACHE_TTL && cached.entry.e > Math.floor(now / 1000)) {
                try {
                    this.hits++;
                    return DocumentSerializer.deserialize(cached.entry.d);
                }
                catch {
                    this.hits--;
                    this.readCache.delete(key);
                }
            }
        }
        try {
            const data = await this.client.get(key);
            this.lastSuccessfulOperation = Date.now();
            if (!data) {
                this.misses++;
                this.readCache.delete(key);
                return null;
            }
            const entry = JSON.parse(data);
            const now = Math.floor(Date.now() / 1000);
            if (entry.e <= now) {
                setImmediate(() => {
                    this.delete(key).catch(() => { });
                });
                this.misses++;
                this.readCache.delete(key);
                return null;
            }
            this.hits++;
            this.readCache.set(key, { entry, timestamp: Date.now() });
            this.cleanReadCache();
            entry.h++;
            entry.a = now;
            setImmediate(() => {
                this.client?.setex(key, entry.e - now, JSON.stringify(entry)).catch(() => { });
            });
            try {
                return DocumentSerializer.deserialize(entry.d);
            }
            catch (error) {
                if (this.debugMode) {
                    console.error(`Redis GET deserialization error for ${key}:`, error);
                }
                return entry.d;
            }
        }
        catch (error) {
            this.errors++;
            this.misses++;
            if (this.debugMode) {
                console.error(`Redis GET error for ${key}:`, error.message);
            }
            return null;
        }
    }
    async has(key) {
        if (!this.client || !this.connected) {
            this.readOperations++;
            this.misses++;
            return false;
        }
        this.readOperations++;
        const cached = this.readCache.get(key);
        if (cached) {
            const now = Date.now();
            if (now - cached.timestamp < this.READ_CACHE_TTL &&
                cached.entry.e > Math.floor(now / 1000)) {
                this.hits++;
                return true;
            }
        }
        try {
            const exists = await this.client.exists(key);
            this.lastSuccessfulOperation = Date.now();
            if (exists === 1) {
                this.hits++;
                return true;
            }
            else {
                this.misses++;
                return false;
            }
        }
        catch (error) {
            this.misses++;
            return false;
        }
    }
    async delete(key) {
        if (!this.client || !this.connected) {
            return false;
        }
        this.writeOperations++;
        this.writeQueue.delete(key);
        this.readCache.delete(key);
        try {
            const result = await this.client.del(key);
            this.lastSuccessfulOperation = Date.now();
            return result > 0;
        }
        catch (error) {
            this.errors++;
            if (this.debugMode) {
                console.error(`Redis DELETE error for ${key}:`, error.message);
            }
            return false;
        }
    }
    async deletePattern(pattern) {
        if (!this.client || !this.connected) {
            return 0;
        }
        this.writeOperations++;
        try {
            const keys = await this.client.keys(pattern);
            if (keys.length === 0) {
                return 0;
            }
            for (const key of keys) {
                this.writeQueue.delete(key);
                this.readCache.delete(key);
            }
            const pipeline = this.client.pipeline();
            for (const key of keys) {
                pipeline.del(key);
            }
            const results = await pipeline.exec();
            this.lastSuccessfulOperation = Date.now();
            return results?.filter(([err]) => !err).length ?? 0;
        }
        catch (error) {
            this.errors++;
            if (this.debugMode) {
                console.error(`Redis DELETE PATTERN error for ${pattern}:`, error.message);
            }
            return 0;
        }
    }
    async mget(keys) {
        if (!this.client || !this.connected || keys.length === 0) {
            return new Map();
        }
        this.readOperations += keys.length;
        const result = new Map();
        const keysToFetch = [];
        const now = Date.now();
        const nowSec = Math.floor(now / 1000);
        for (const key of keys) {
            const cached = this.readCache.get(key);
            if (cached &&
                now - cached.timestamp < this.READ_CACHE_TTL &&
                cached.entry.e > nowSec) {
                try {
                    const deserialized = DocumentSerializer.deserialize(cached.entry.d);
                    result.set(key, deserialized);
                    this.hits++;
                }
                catch {
                    keysToFetch.push(key);
                }
            }
            else {
                keysToFetch.push(key);
            }
        }
        if (keysToFetch.length === 0) {
            return result;
        }
        try {
            const values = await this.client.mget(...keysToFetch);
            this.lastSuccessfulOperation = Date.now();
            for (let i = 0; i < keysToFetch.length; i++) {
                const key = keysToFetch[i];
                if (values[i]) {
                    try {
                        const entry = JSON.parse(values[i]);
                        if (entry.e > nowSec) {
                            const deserialized = DocumentSerializer.deserialize(entry.d);
                            result.set(key, deserialized);
                            this.readCache.set(key, { entry, timestamp: now });
                            this.hits++;
                        }
                        else {
                            this.misses++;
                            setImmediate(() => {
                                this.delete(key).catch(() => { });
                            });
                        }
                    }
                    catch (error) {
                        if (this.debugMode) {
                            console.error(`MGET error for ${key}:`, error);
                        }
                        this.misses++;
                    }
                }
                else {
                    this.misses++;
                }
            }
            this.cleanReadCache();
            return result;
        }
        catch (error) {
            this.errors++;
            const fetchedKeys = result.size - (keys.length - keysToFetch.length);
            this.misses += keysToFetch.length - fetchedKeys;
            if (this.debugMode) {
                console.error('Redis MGET error:', error.message);
            }
            return result;
        }
    }
    async mset(entries) {
        if (!this.client || !this.connected || entries.size === 0) {
            return 0;
        }
        this.writeOperations += entries.size;
        if (this.isUnderMemoryPressure) {
            if (this.debugMode) {
                console.warn('Skipping mset due to memory pressure');
            }
            return 0;
        }
        try {
            const pipeline = this.client.pipeline();
            let successCount = 0;
            const now = Date.now();
            for (const [key, { value, ttl }] of entries) {
                try {
                    const serializedValue = DocumentSerializer.serialize(value);
                    const size = SizeCalculator.fastSizeEstimate(serializedValue);
                    if (size <= this.config.maxItemSizeMB * 1048576) {
                        const entry = {
                            d: serializedValue,
                            e: Math.floor(now / 1000) + (ttl ?? this.config.ttl),
                            s: size,
                            h: 0,
                            a: Math.floor(now / 1000),
                            t: Math.floor(now / 1000),
                            v: 1
                        };
                        const serialized = JSON.stringify(entry);
                        const effectiveTtl = ttl ?? this.config.ttl;
                        pipeline.setex(key, effectiveTtl, serialized);
                        this.readCache.set(key, { entry, timestamp: now });
                        successCount++;
                    }
                }
                catch (error) {
                    if (this.debugMode) {
                        console.error(`MSET serialization error for ${key}:`, error);
                    }
                }
            }
            await pipeline.exec();
            this.lastSuccessfulOperation = Date.now();
            this.cleanReadCache();
            return successCount;
        }
        catch (error) {
            this.errors++;
            if (this.debugMode) {
                console.error('Redis MSET error:', error.message);
            }
            return 0;
        }
    }
    async clear() {
        if (!this.client || !this.connected) {
            return false;
        }
        this.writeOperations++;
        try {
            this.writeQueue.clear();
            this.readCache.clear();
            const pattern = this.config.redis.keyPrefix + '*';
            const deleted = await this.deletePattern(pattern);
            if (this.debugMode) {
                console.log(`Cleared ${deleted} entries`);
            }
            return true;
        }
        catch (error) {
            this.errors++;
            if (this.debugMode) {
                console.error('Redis CLEAR error:', error.message);
            }
            return false;
        }
    }
    getStats() {
        const hitRate = (this.hits + this.misses) > 0
            ? (this.hits / (this.hits + this.misses)) * 100
            : 0;
        const redisMaxMB = this.redisMaxMemoryBytes > 0 ? this.redisMaxMemoryBytes / (1024 * 1024) : 0;
        const memoryUsagePercent = redisMaxMB > 0 ? (this.totalCacheSizeMB / redisMaxMB) * 100 : 0;
        // Return a compact set of stats more useful for overall cache overview
        return {
            connected: this.connected,
            hits: this.hits,
            misses: this.misses,
            errors: this.errors,
            readOperations: this.readOperations,
            writeOperations: this.writeOperations,
            totalOperations: this.readOperations + this.writeOperations,
            hitRate: parseFloat(hitRate.toFixed(2)),
            reconnectAttempts: this.reconnectAttempts,
            writeQueueSize: this.writeQueue.size,
            readCacheSize: this.readCache.size,
            underMemoryPressure: this.isUnderMemoryPressure,
            cacheSizeMB: parseFloat(this.totalCacheSizeMB.toFixed(2)),
            redisMaxMemoryMB: parseFloat(redisMaxMB.toFixed(2)),
            redisMemoryUsagePercent: parseFloat(memoryUsagePercent.toFixed(2)),
            lastSuccessfulOperation: this.lastSuccessfulOperation,
            timeSinceLastOperation: Date.now() - this.lastSuccessfulOperation
        };
    }
    resetStats() {
        // Reset basic operation counters
        this.hits = 0;
        this.misses = 0;
        this.errors = 0;
        this.readOperations = 0;
        this.writeOperations = 0;
        // Reset connection-related metrics so monitoring/alerts start fresh
        this.reconnectAttempts = 0;
        this.lastSuccessfulOperation = Date.now();
        // Reset memory-related tracked values (these are informational only)
        this.totalCacheSizeMB = 0;
        this.redisMaxMemoryBytes = 0;
        this.isUnderMemoryPressure = false;
    }
    async disconnect() {
        await this.flushWriteQueue();
        if (this.writeTimer) {
            clearInterval(this.writeTimer);
            this.writeTimer = undefined;
        }
        if (this.memoryCheckInterval) {
            clearInterval(this.memoryCheckInterval);
            this.memoryCheckInterval = undefined;
        }
        if (this.healthCheckInterval) {
            clearInterval(this.healthCheckInterval);
            this.healthCheckInterval = undefined;
        }
        if (this.client) {
            this.connected = false;
            await this.client.quit();
            this.client = null;
            if (this.debugMode) {
                console.log('Redis disconnected');
            }
        }
        this.writeQueue.clear();
        this.readCache.clear();
    }
    async ping() {
        if (!this.client || !this.connected) {
            return false;
        }
        try {
            const result = await this.client.ping();
            if (result === 'PONG') {
                this.lastSuccessfulOperation = Date.now();
                return true;
            }
            return false;
        }
        catch {
            return false;
        }
    }
}
