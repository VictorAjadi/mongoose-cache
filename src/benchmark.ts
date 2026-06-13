import { MongooseCache } from './cache-types/MongooseCache';
import { Schema, model, connect, disconnect } from 'mongoose';
import * as readline from 'readline';
import * as fs from 'fs';
import * as path from 'path';

/**
 * ============================================================================
 * @mongoose-cache - MULTI-CLIENT STRESS TEST (Bun/Node.js Compatible)
 * ============================================================================
 */

// Detect runtime
const isBun = typeof (globalThis as any).Bun !== 'undefined';
const runtime = isBun ? 'Bun' : 'Node.js';
console.log(`🚀 Running on ${runtime} runtime\n`);

// Polyfill for performance if needed
const perf = typeof performance !== 'undefined' ? performance : require('perf_hooks').performance;

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const question = (query: string): Promise<string> =>
    new Promise((resolve) => rl.question(query, resolve));

interface StressTestConfig {
    mongoUri: string;
    backend: 'memory' | 'redis';
    ttl: number;
    debug: boolean;
    redisConfig?: {
        host: string;
        port: number;
        password?: string;
    };
    numClients: number;
    testDuration: number;
    rampUpTime: number;
    hotKeyRatio: number;
    coldKeyRatio: number;
    operationsPerSecond: number;
    recordLatency: boolean;
    outputFile?: string;
}

interface TestMetrics {
    totalRequests: number;
    successfulRequests: number;
    failedRequests: number;
    cacheHits: number;
    cacheMisses: number;
    coalescedRequests: number;
    latencies: number[];
    timestamps: Date[];
    operationTypes: Map<string, number>;
    errors: Map<string, number>;
}

class MetricsCollector {
    private metrics: TestMetrics;
    private lock: Promise<void> = Promise.resolve();

    constructor() {
        this.metrics = this.initMetrics();
    }

    private initMetrics(): TestMetrics {
        return {
            totalRequests: 0,
            successfulRequests: 0,
            failedRequests: 0,
            cacheHits: 0,
            cacheMisses: 0,
            coalescedRequests: 0,
            latencies: [],
            timestamps: [],
            operationTypes: new Map(),
            errors: new Map()
        };
    }

    async recordRequest(success: boolean, latency: number, operationType: string, cacheHit?: boolean) {
        await this.lock;
        this.lock = new Promise((resolve) => {
            this.metrics.totalRequests++;
            if (success) {
                this.metrics.successfulRequests++;
                if (cacheHit !== undefined) {
                    if (cacheHit) this.metrics.cacheHits++;
                    else this.metrics.cacheMisses++;
                }
            } else {
                this.metrics.failedRequests++;
            }
            this.metrics.latencies.push(latency);
            this.metrics.timestamps.push(new Date());
            this.metrics.operationTypes.set(
                operationType,
                (this.metrics.operationTypes.get(operationType) || 0) + 1
            );
            resolve();
        });
    }

    recordError(errorMessage: string) {
        this.metrics.errors.set(errorMessage, (this.metrics.errors.get(errorMessage) || 0) + 1);
    }

    recordCoalescedRequest() {
        this.metrics.coalescedRequests++;
    }

    getMetrics(): TestMetrics {
        return { ...this.metrics, latencies: [...this.metrics.latencies] };
    }

    reset() {
        this.metrics = this.initMetrics();
    }
}

// Global models to prevent recompilation
let globalModels: any = null;

function getModels(cache: MongooseCache) {
    if (globalModels) {
        return globalModels;
    }

    // Define schemas if not already defined
    const ProjectSchema = new Schema({
        title: String,
        code: { type: String, unique: true },
        ownerId: { type: Schema.Types.ObjectId, ref: 'User' },
        resources: { budget: Number, currency: String, allocatedHours: Number },
        flags: { isPublic: Boolean, tags: [String] }
    });

    const UserSchema = new Schema({
        name: { first: String, last: String },
        email: String,
        orgId: { type: Schema.Types.ObjectId, ref: 'Organization' }
    });

    const OrgSchema = new Schema({
        name: String,
        subscription: { tier: String, status: String }
    });

    // Apply cache to schemas
    cache.applyCacheToQueries(ProjectSchema);
    cache.applyCacheToQueries(UserSchema);
    cache.applyCacheToQueries(OrgSchema);

    // Get or create models
    const Project = model('Project', ProjectSchema);
    const User = model('User', UserSchema);
    const Organization = model('Organization', OrgSchema);

    globalModels = { Project, User, Organization };
    return globalModels;
}

class StressTestWorker {
    private config: StressTestConfig;
    private models: any;
    private metricsCollector: MetricsCollector;
    private isRunning: boolean = true;
    private projectIds: string[] = [];

    constructor(config: StressTestConfig, metricsCollector: MetricsCollector, cache: MongooseCache) {
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.models = getModels(cache);
    }

    async initialize() {
        // Get existing project IDs
        const projects = await this.models.Project.find().limit(100).select('_id').lean();
        this.projectIds = projects.map((p: any) => p._id.toString());

        if (this.projectIds.length === 0) {
            await this.seedData();
            const seededProjects = await this.models.Project.find().limit(100).select('_id').lean();
            this.projectIds = seededProjects.map((p: any) => p._id.toString());
        }
    }

    private async seedData() {
        const Organization = this.models.Organization;
        const User = this.models.User;
        const Project = this.models.Project;

        const orgCount = await Organization.countDocuments();
        if (orgCount > 0) return; // Already seeded

        // Create 5 orgs
        const orgs = await Organization.create([
            { name: 'Org1', subscription: { tier: 'enterprise', status: 'active' } },
            { name: 'Org2', subscription: { tier: 'pro', status: 'active' } },
            { name: 'Org3', subscription: { tier: 'free', status: 'active' } },
            { name: 'Org4', subscription: { tier: 'pro', status: 'active' } },
            { name: 'Org5', subscription: { tier: 'enterprise', status: 'active' } }
        ]);

        // Create 20 users
        const users = [];
        for (let i = 0; i < 20; i++) {
            users.push(await User.create({
                name: { first: `User${i}`, last: `Test${i}` },
                email: `user${i}@test.com`,
                orgId: orgs[i % orgs.length]._id
            }));
        }

        // Create 100 projects
        for (let i = 0; i < 100; i++) {
            await Project.create({
                title: `Project_${i}`,
                code: `PRJ-${i}`,
                ownerId: users[i % users.length]._id,
                resources: { budget: 10000 * (i + 1), currency: 'USD', allocatedHours: 100 },
                flags: { isPublic: true, tags: ['test'] }
            });
        }

        console.log('✅ Data seeded successfully');
    }

    private generateOperation(): { type: string; params: any } {
        if (this.projectIds.length === 0) {
            return { type: 'read', params: { projectId: 'dummy' } };
        }

        const rand = Math.random();
        const isHotKey = rand < this.config.hotKeyRatio;
        let projectId: string;

        if (isHotKey && this.projectIds.length > 10) {
            // Hot keys: first 10 projects
            const hotIndex = Math.floor(Math.random() * 10);
            projectId = this.projectIds[hotIndex];
        } else {
            // Cold keys: random project
            projectId = this.projectIds[Math.floor(Math.random() * this.projectIds.length)];
        }

        const operationRand = Math.random();

        if (operationRand < 0.7) {
            return { type: 'read', params: { projectId } };
        } else if (operationRand < 0.85) {
            return {
                type: 'aggregate',
                params: {
                    pipeline: [
                        { $match: { _id: projectId, 'flags.isPublic': true } },
                        { $project: { budget: '$resources.budget', title: 1 } }
                    ]
                }
            };
        } else {
            return {
                type: 'write',
                params: {
                    projectId,
                    update: { $set: { 'resources.budget': Math.random() * 100000 } }
                }
            };
        }
    }

    private async executeOperation(operation: { type: string; params: any }): Promise<{ success: boolean; latency: number; cacheHit?: boolean }> {
        const startTime = perf.now();
        let success = true;
        let cacheHit: boolean | undefined = undefined;

        try {
            if (operation.type === 'read') {
                const result = await this.models.Project.findById(operation.params.projectId)
                    .populate('ownerId')
                    .lean();
                cacheHit = result !== null;
            } else if (operation.type === 'aggregate') {
                const result = await this.models.Project.aggregate(operation.params.pipeline);
                cacheHit = result.length > 0;
            } else if (operation.type === 'write') {
                await this.models.Project.updateOne(
                    { _id: operation.params.projectId },
                    operation.params.update
                );
                cacheHit = false;
            }
        } catch (error) {
            success = false;
            this.metricsCollector.recordError((error as Error).message);
        }

        const latency = perf.now() - startTime;
        return { success, latency, cacheHit };
    }

    async start(workerId: number): Promise<void> {
        const endTime = Date.now() + (this.config.testDuration * 1000);
        const targetInterval = 1000 / (this.config.operationsPerSecond / this.config.numClients);
        let lastRequestTime = Date.now();
        let requestCount = 0;

        while (this.isRunning && Date.now() < endTime) {
            const operation = this.generateOperation();
            const { success, latency, cacheHit } = await this.executeOperation(operation);

            await this.metricsCollector.recordRequest(success, latency, operation.type, cacheHit);
            requestCount++;

            // Rate limiting - only apply every few requests to reduce overhead
            if (requestCount % 5 === 0) {
                const now = Date.now();
                const elapsed = now - lastRequestTime;
                const expectedElapsed = requestCount * targetInterval;
                if (elapsed < expectedElapsed) {
                    await new Promise(resolve => setTimeout(resolve, expectedElapsed - elapsed));
                }
            }

            // Heartbeat every 500 requests
            if (requestCount % 500 === 0 && process.send) {
                process.send({ type: 'heartbeat', workerId, requestCount });
            }
        }

        if (process.send) {
            process.send({ type: 'complete', workerId, requestCount });
        }
    }

    stop() {
        this.isRunning = false;
    }
}

class MultiClientStressTest {
    private config: StressTestConfig;
    private metricsCollector: MetricsCollector;
    private workers: StressTestWorker[] = [];
    private cache: MongooseCache;
    private testStartTime: number = 0;
    private testEndTime: number = 0;
    private intervalId: NodeJS.Timeout | null = null;

    constructor(config: StressTestConfig) {
        this.config = config;
        this.metricsCollector = new MetricsCollector();

        // Initialize cache once
        let cacheConfig: any = {
            ttl: this.config.ttl,
            debug: this.config.debug
        };

        if (this.config.backend === 'redis' && this.config.redisConfig) {
            cacheConfig.redis = this.config.redisConfig;
        }

        this.cache = new MongooseCache(cacheConfig);
    }

    private async setupDatabase() {
        try {
            // Only connect once for setup
            await connect(this.config.mongoUri);
            console.log('✅ Connected to MongoDB');

            // Setup schema and seed data using temporary worker
            const tempWorker = new StressTestWorker(this.config, this.metricsCollector, this.cache);
            await tempWorker.initialize();

            console.log('✅ Database ready');
        } catch (error) {
            console.error('Failed to setup database:', error);
            throw error;
        }
    }

    private async initializeWorkers() {
        console.log(`Initializing ${this.config.numClients} workers...`);

        // Each worker shares the same cache instance and models
        for (let i = 0; i < this.config.numClients; i++) {
            const worker = new StressTestWorker(this.config, this.metricsCollector, this.cache);
            await worker.initialize();
            this.workers.push(worker);
        }

        console.log('✅ All workers initialized');
    }

    private async rampUpClients(): Promise<void> {
        const clientsPerStep = Math.ceil(this.config.numClients / this.config.rampUpTime);
        let activeClients = 0;

        for (let step = 0; step < this.config.rampUpTime; step++) {
            const toAdd = Math.min(clientsPerStep, this.config.numClients - activeClients);

            // Start new workers
            for (let i = activeClients; i < activeClients + toAdd; i++) {
                const workerId = i;
                this.workers[i].start(workerId).catch(error => {
                    console.error(`Worker ${workerId} error:`, error);
                });
            }

            activeClients += toAdd;
            console.log(`Ramping up: ${activeClients}/${this.config.numClients} clients active`);

            if (step < this.config.rampUpTime - 1) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    private startMetricsReporting() {
        this.intervalId = setInterval(() => {
            const elapsed = (perf.now() - this.testStartTime) / 1000;
            if (elapsed <= 0) return;

            const metrics = this.metricsCollector.getMetrics();
            const currentRPS = metrics.totalRequests / elapsed;
            const hitRate = metrics.cacheHits + metrics.cacheMisses > 0
                ? (metrics.cacheHits / (metrics.cacheHits + metrics.cacheMisses)) * 100
                : 0;

            console.log(`[${elapsed.toFixed(0)}s] RPS: ${currentRPS.toFixed(2)} | ` +
                `Total: ${metrics.totalRequests} | ` +
                `Hits: ${metrics.cacheHits} | Misses: ${metrics.cacheMisses} | ` +
                `Hit Rate: ${hitRate.toFixed(1)}% | Errors: ${metrics.failedRequests}`);
        }, 5000);
    }

    private printFinalMetrics() {
        const metrics = this.metricsCollector.getMetrics();
        const duration = (this.testEndTime - this.testStartTime) / 1000;

        // GET CACHE STATS (NEW)
        let cacheStats: any = null;
        try {
            // Assuming cache has stats method
            // cacheStats = await this.cache.getStats();
        } catch (e) {
            // Ignore if not available
        }

        const actualRPS = metrics.totalRequests / duration;
        const successRate = (metrics.successfulRequests / metrics.totalRequests) * 100;
        const hitRate = metrics.cacheHits + metrics.cacheMisses > 0
            ? (metrics.cacheHits / (metrics.cacheHits + metrics.cacheMisses)) * 100
            : 0;

        const sortedLatencies = [...metrics.latencies].sort((a, b) => a - b);
        const p50 = sortedLatencies[Math.floor(sortedLatencies.length * 0.5)] || 0;
        const p95 = sortedLatencies[Math.floor(sortedLatencies.length * 0.95)] || 0;
        const p99 = sortedLatencies[Math.floor(sortedLatencies.length * 0.99)] || 0;
        const avgLatency = metrics.latencies.length > 0
            ? metrics.latencies.reduce((a, b) => a + b, 0) / metrics.latencies.length
            : 0;
        const maxLatency = metrics.latencies.length > 0 ? Math.max(...metrics.latencies) : 0;
        const minLatency = metrics.latencies.length > 0 ? Math.min(...metrics.latencies) : 0;

        console.log('\n' + '='.repeat(80));
        console.log('📊 STRESS TEST FINAL RESULTS');
        console.log('='.repeat(80));
        console.log(`Runtime: ${runtime}`);
        console.log(`Backend: ${this.config.backend}`);
        console.log(`Test Duration: ${duration.toFixed(2)}s`);
        console.log(`Target RPS: ${this.config.operationsPerSecond}`);
        console.log(`Actual RPS: ${actualRPS.toFixed(2)}`);
        console.log(`Success Rate: ${successRate.toFixed(2)}%`);

        // ADDED: Cache metrics
        if (cacheStats) {
            console.log(`\n💾 Cache Statistics:`);
            console.log(`  Cache Type: ${cacheStats.cacheType || 'unknown'}`);
            console.log(`  Cache Size: ${cacheStats.cachedDataMB || 0}MB`);
            console.log(`  Max Size: ${cacheStats.maxCacheMB || 0}MB`);
            console.log(`  Keys Cached: ${cacheStats.keys || 0}`);
            console.log(`  Hit Rate: ${hitRate.toFixed(2)}%`);
            console.log(`  Cache Hits: ${metrics.cacheHits}`);
            console.log(`  Cache Misses: ${metrics.cacheMisses}`);

            if (cacheStats.evictions) {
                console.log(`  Evictions: ${cacheStats.evictions}`);
            }
            if (cacheStats.underMemoryPressure !== undefined) {
                console.log(`  Memory Pressure: ${cacheStats.underMemoryPressure ? 'YES' : 'NO'}`);
            }
        }

        console.log(`\n📈 Latency Statistics (ms):`);
        console.log(`  Min: ${minLatency.toFixed(2)}ms`);
        console.log(`  Average: ${avgLatency.toFixed(2)}ms`);
        console.log(`  Max: ${maxLatency.toFixed(2)}ms`);
        console.log(`  P50: ${p50.toFixed(2)}ms`);
        console.log(`  P95: ${p95.toFixed(2)}ms`);
        console.log(`  P99: ${p99.toFixed(2)}ms`);
        console.log(`\n📝 Operation Breakdown:`);

        for (const [op, count] of metrics.operationTypes) {
            console.log(`  ${op}: ${count} (${((count / metrics.totalRequests) * 100).toFixed(2)}%)`);
        }

        if (metrics.errors.size > 0) {
            console.log(`\n❌ Top Errors:`);
            const sortedErrors = Array.from(metrics.errors.entries()).sort((a, b) => b[1] - a[1]);
            sortedErrors.slice(0, 5).forEach(([error, count]) => {
                const shortError = error.length > 80 ? error.substring(0, 77) + '...' : error;
                console.log(`  ${shortError}: ${count} times`);
            });
        }
        console.log('='.repeat(80));
    }

    private saveMetricsToFile() {
        if (!this.config.outputFile) return;

        const metrics = this.metricsCollector.getMetrics();
        const duration = (this.testEndTime - this.testStartTime) / 1000;
        const sortedLatencies = [...metrics.latencies].sort((a, b) => a - b);

        const report = {
            runtime,
            config: {
                backend: this.config.backend,
                numClients: this.config.numClients,
                testDuration: this.config.testDuration,
                rampUpTime: this.config.rampUpTime,
                targetRPS: this.config.operationsPerSecond,
                hotKeyRatio: this.config.hotKeyRatio,
                ttl: this.config.ttl,
                redisConfig: this.config.redisConfig ? {
                    host: this.config.redisConfig.host,
                    port: this.config.redisConfig.port
                } : undefined
            },
            results: {
                duration,
                totalRequests: metrics.totalRequests,
                successfulRequests: metrics.successfulRequests,
                failedRequests: metrics.failedRequests,
                actualRPS: metrics.totalRequests / duration,
                successRate: (metrics.successfulRequests / metrics.totalRequests) * 100,
                cacheHits: metrics.cacheHits,
                cacheMisses: metrics.cacheMisses,
                hitRate: metrics.cacheHits + metrics.cacheMisses > 0
                    ? (metrics.cacheHits / (metrics.cacheHits + metrics.cacheMisses)) * 100
                    : 0,
                avgLatency: metrics.latencies.length > 0
                    ? metrics.latencies.reduce((a, b) => a + b, 0) / metrics.latencies.length
                    : 0,
                p50Latency: sortedLatencies[Math.floor(sortedLatencies.length * 0.5)] || 0,
                p95Latency: sortedLatencies[Math.floor(sortedLatencies.length * 0.95)] || 0,
                p99Latency: sortedLatencies[Math.floor(sortedLatencies.length * 0.99)] || 0,
                minLatency: sortedLatencies[0] || 0,
                maxLatency: sortedLatencies[sortedLatencies.length - 1] || 0,
                operationTypes: Object.fromEntries(metrics.operationTypes),
                topErrors: Object.fromEntries(Array.from(metrics.errors.entries()).slice(0, 10))
            },
            timestamp: new Date().toISOString()
        };

        const filePath = path.resolve(this.config.outputFile);
        fs.writeFileSync(filePath, JSON.stringify(report, null, 2));
        console.log(`\n💾 Metrics saved to ${filePath}`);
    }

    private async cleanup() {
        console.log('\n🧹 Cleaning up...');

        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }

        for (const worker of this.workers) {
            worker.stop();
        }

        try {
            await disconnect();
        } catch (error) {
            // Ignore disconnect errors
        }
        console.log('✅ Cleanup complete');
    }

    async run() {
        console.log('\n' + '='.repeat(80));
        console.log('🚀 @mongoose-cache MULTI-CLIENT STRESS TEST');
        console.log('='.repeat(80));
        console.log(`Runtime Environment: ${runtime}`);
        console.log(`Configuration:`);
        console.log(`  Backend: ${this.config.backend}`);
        console.log(`  Concurrent Clients: ${this.config.numClients}`);
        console.log(`  Test Duration: ${this.config.testDuration}s`);
        console.log(`  Ramp-up Time: ${this.config.rampUpTime}s`);
        console.log(`  Target RPS: ${this.config.operationsPerSecond}`);
        console.log(`  Hot Keys Ratio: ${(this.config.hotKeyRatio * 100).toFixed(0)}%`);
        console.log(`  TTL: ${this.config.ttl}s`);
        console.log(`  Debug: ${this.config.debug}`);
        if (this.config.backend === 'redis' && this.config.redisConfig) {
            console.log(`  Redis: ${this.config.redisConfig.host}:${this.config.redisConfig.port}`);
        }
        console.log('='.repeat(80));

        try {
            await this.setupDatabase();
            await this.initializeWorkers();

            console.log('\n🔥 Starting stress test...');
            this.testStartTime = perf.now();
            this.testEndTime = this.testStartTime + (this.config.testDuration * 1000);


            this.startMetricsReporting();
            await this.rampUpClients();

            // Wait for test duration
            await new Promise(resolve => setTimeout(resolve, this.config.testDuration * 1000));

            this.testEndTime = perf.now();
            this.printFinalMetrics();
            this.saveMetricsToFile();

        } catch (error) {
            console.error('\n❌ Test failed:', error);
        } finally {
            await this.cleanup();
            rl.close();
            process.exit(0);
        }
    }
}

async function main() {
    console.log('\n🎯 @mongoose-cache Multi-Client Stress Test\n');

    await question('Choose test type (1: Full Stress Test, 2: Stampede Test Only, default: 1): ');

    const mongoUri = await question('Enter MongoDB URI (default: mongodb://localhost:27017/stress-test): ') || 'mongodb://localhost:27017/stress-test';
    const backend = (await question('Choose Backend (memory/redis, default: memory): ')).toLowerCase() || 'memory';
    const ttl = Number(await question('Enter TTL in seconds (default: 60): ')) || 60;
    const debug = (await question('Enable debug logging? (true/false, default: false): ')).toLowerCase() === 'true';

    let redisConfig;
    if (backend === 'redis') {
        const redisHost = await question('Redis Host (default: localhost): ') || 'localhost';
        const redisPort = Number(await question('Redis Port (default: 6379): ')) || 6379;
        const redisPassword = await question('Redis Password (leave blank if none): ');
        redisConfig = { host: redisHost, port: redisPort, ...(redisPassword && { password: redisPassword }) };
        console.log(`Using Redis at ${redisHost}:${redisPort}`);
    }

    const numClients = Number(await question('Number of concurrent clients (default: 50): ')) || 50;
    const testDuration = Number(await question('Test duration in seconds (default: 60): ')) || 60;
    const rampUpTime = Number(await question('Ramp-up time in seconds (default: 10): ')) || 10;
    const operationsPerSecond = Number(await question('Target operations per second (default: 1000): ')) || 1000;
    const hotKeyRatio = Number(await question('Hot key ratio (0-1, default: 0.2): ')) || 0.2;
    const outputFile = await question('Save results to file? (leave blank to skip): ');

    const config: StressTestConfig = {
        mongoUri,
        backend: backend as 'memory' | 'redis',
        ttl,
        debug,
        redisConfig,
        numClients,
        testDuration,
        rampUpTime,
        hotKeyRatio,
        coldKeyRatio: 1 - hotKeyRatio,
        operationsPerSecond,
        recordLatency: true,
        outputFile: outputFile || undefined
    };

    const stressTest = new MultiClientStressTest(config);
    await stressTest.run();
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n\n⚠️ Test interrupted by user');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n\n⚠️ Test terminated');
    process.exit(0);
});

// Run main function
main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});