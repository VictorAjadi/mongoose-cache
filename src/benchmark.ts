
import { MongooseCache } from './cache-types/MongooseCache';
import { Schema, model, connect } from 'mongoose';
import * as readline from 'readline';

/**
 * ============================================================================
 * @mongoose-cache - Production-Heavy Relational Benchmark
 * ============================================================================
 */

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const question = (query: string): Promise<string> =>
    new Promise((resolve) => rl.question(query, resolve));

async function runBenchmark() {
    console.log('\n🚀 Starting @mongoose-cache HEAVY Relational Benchmark...\n');

    const mongoUri = await question('Enter MongoDB URI (default: mongodb://localhost:27017/benchmark-db): ') || 'mongodb://localhost:27017/benchmark-db';
    const backend = (await question('Choose Backend (memory/redis, default: memory): ')).toLowerCase() || 'memory';
    const iterations = Number(await question('Enter number of iterations (default: 2000): ')) || 2000;
    const ttl = Number(await question('Enter TTL in seconds (default: 300): ')) || 300;
    const debug = (await question('Enable debug logging? (true/false, default: false): ')).toLowerCase() === 'true';
    let cacheConfig: any = { ttl: ttl, debug: debug };
    if (backend === 'redis') {
        cacheConfig.redis = {
            host: await question('Redis Host (default: localhost): ') || 'localhost',
            port: Number(await question('Redis Port (default: 6379): ')) || 6379
        };
    }

    try {
        await connect(mongoUri);
        console.log('✅ Connected to MongoDB.\n');

        // 1. HEAVY SCHEMAS
        const OrgSchema = new Schema({
            name: String,
            settings: {
                features: [String],
                branding: { logo: String, colors: { primary: String, secondary: String } },
                security: { mfaRequired: Boolean, ipWhitelist: [String], ssoProvider: String }
            },
            subscription: {
                tier: { type: String, enum: ['free', 'pro', 'enterprise'] },
                status: String,
                expiresAt: Date,
                limits: { users: Number, storageGB: Number, apiCallsMonthly: Number }
            },
            audit: { createdAt: { type: Date, default: Date.now }, lastUpdated: Date }
        });

        const UserSchema = new Schema({
            name: { first: String, last: String },
            email: { type: String, unique: true },
            orgId: { type: Schema.Types.ObjectId, ref: 'Organization' },
            profile: {
                address: { street: String, city: String, country: String, postalCode: String },
                preferences: { theme: String, notifications: { email: Boolean, push: Boolean, sms: Boolean } }
            },
            security: { roles: [String], permissions: [String], lastLogin: Date },
            activityLogs: [{ action: String, timestamp: { type: Date, default: Date.now }, ip: String }]
        });

        const ProjectSchema = new Schema({
            title: String,
            code: { type: String, unique: true },
            ownerId: { type: Schema.Types.ObjectId, ref: 'User' },
            milestones: [{
                name: String,
                dueDate: Date,
                completed: { type: Boolean, default: false },
                tasks: [{ title: String, assignedTo: String, priority: Number }]
            }],
            resources: { budget: Number, currency: String, allocatedHours: Number },
            metadata: Schema.Types.Mixed,
            flags: { isArchived: Boolean, isPublic: Boolean, tags: [String] }
        });

        const cache = new MongooseCache(cacheConfig);
        cache.applyCacheToQueries(OrgSchema);
        cache.applyCacheToQueries(UserSchema);
        cache.applyCacheToQueries(ProjectSchema);

        const Organization = model('Organization', OrgSchema);
        const User = model('User', UserSchema);
        const Project = model('Project', ProjectSchema);

        // 2. SEED HEAVY DATA
        console.log('Generating heavy relational data...');
        await Promise.all([Organization.deleteMany({}), User.deleteMany({}), Project.deleteMany({})]);

        const org = await Organization.create({
            name: 'Global Tech Industries',
            settings: {
                features: ['caching', 'analytics', 'automation'],
                security: { mfaRequired: true, ipWhitelist: ['10.0.0.1', '127.0.0.1'], ssoProvider: 'Okta' }
            },
            subscription: { tier: 'enterprise', status: 'active', limits: { users: 500, storageGB: 1000, apiCallsMonthly: 1000000 } }
        });

        const user = await User.create({
            name: { first: 'Admin', last: 'User' },
            email: 'admin@globaltech.com',
            orgId: org._id,
            security: { roles: ['root', 'billing'], permissions: ['manage_users', 'view_all_data'] },
            activityLogs: Array.from({ length: 50 }, (_, i) => ({ action: `LOGIN_ATTEMPT_${i}`, ip: '1.1.1.1' }))
        });

        const project = await Project.create({
            title: 'Project Antigravity',
            code: 'ANT-101',
            ownerId: user._id,
            milestones: Array.from({ length: 15 }, (_, i) => ({
                name: `Phase ${i + 1}`,
                dueDate: new Date(),
                tasks: Array.from({ length: 10 }, (_, j) => ({ title: `Task ${i}-${j}`, assignedTo: 'Dev Team', priority: 1 }))
            })),
            resources: { budget: 500000, currency: 'USD', allocatedHours: 1200 },
            flags: { isArchived: false, isPublic: true, tags: ['node', 'mongoose', 'high-performance'] }
        });

        // --- STAMPEDE VERIFICATION ---
        console.log('🧪 VERIFYING REQUEST COALESCING (Stampede Protection)...');
        await cache.flushCache();

        console.log('Firing 20 simultaneous queries for the same key...');
        const stampedeStart = performance.now();

        // Execute 20 identical queries at the exact same time
        const results = await Promise.all(
            Array.from({ length: 20 }, () =>
                Project.findById(project._id)
                    .populate({ path: 'ownerId', populate: { path: 'orgId' } })
                    .lean()
            )
        );

        const stampedeEnd = performance.now();
        console.log(`Result: All 20 requests finished in ${(stampedeEnd - stampedeStart).toFixed(2)}ms.`);
        console.log('Check the logs above ^. You should see EXACTLY ONE [CACHE MISS] and 19 [QUERY COALESCED].\n');

        console.log(`\nStarting HEAVY Benchmark (${iterations} iterations)...\n`);

        // --- PERFORMANCE RUNS ---

        // 1. Relational Read (Heavy)
        let dbPopTotal = 0;
        for (let i = 0; i < 50; i++) {
            const start = performance.now();
            await Project.findById(project._id).populate({ path: 'ownerId', populate: { path: 'orgId' } }).setOptions({ cache: false }).lean();
            dbPopTotal += (performance.now() - start);
        }
        const avgDbPop = dbPopTotal / 50;

        await Project.findById(project._id).populate({ path: 'ownerId', populate: { path: 'orgId' } }).lean(); // warm
        const startCachePop = performance.now();
        for (let i = 0; i < iterations; i++) {
            await Project.findById(project._id).populate({ path: 'ownerId', populate: { path: 'orgId' } }).lean();
        }
        const avgCachePop = (performance.now() - startCachePop) / iterations;

        // 2. Complex Aggregation
        const pipeline = [
            { $match: { 'flags.isPublic': true } },
            { $unwind: '$milestones' },
            { $group: { _id: '$resources.currency', totalBudget: { $sum: '$resources.budget' }, avgMilestones: { $avg: { $size: '$milestones.tasks' } } } }
        ];

        let dbAggTotal = 0;
        for (let i = 0; i < 20; i++) {
            const start = performance.now();
            const agg = Project.aggregate(pipeline);
            (agg as any)._cacheOptions = false;
            await agg;
            dbAggTotal += (performance.now() - start);
        }
        const avgDbAgg = dbAggTotal / 20;

        await Project.aggregate(pipeline); // warm
        const startCacheAgg = performance.now();
        for (let i = 0; i < iterations; i++) await Project.aggregate(pipeline);
        const avgCacheAgg = (performance.now() - startCacheAgg) / iterations;

        // 3. Mutation Invalidation
        let totalMutation = 0;
        for (let i = 0; i < 30; i++) {
            const start = performance.now();
            await Project.updateOne({ _id: project._id }, { $set: { 'resources.budget': 500000 + i } });
            totalMutation += (performance.now() - start);
        }
        const avgMutation = totalMutation / 30;

        // FINAL STATS
        console.log('📊 HEAVY PRODUCTION COMPARISON (Side-by-Side)');
        console.log('-------------------------------------------------------------------------');
        console.log('| OPERATION            | WITHOUT CACHE | WITH CACHE    | SPEEDUP       |');
        console.log('-------------------------------------------------------------------------');
        console.log(`| Deep Relational Pop | ${avgDbPop.toFixed(3)}ms       | ${avgCachePop.toFixed(3)}ms       | ${(avgDbPop / avgCachePop).toFixed(1)}x faster    |`);
        console.log(`| Complex Aggregation | ${avgDbAgg.toFixed(3)}ms       | ${avgCacheAgg.toFixed(3)}ms       | ${(avgDbAgg / avgCacheAgg).toFixed(1)}x faster    |`);
        console.log(`| Async Invalidation  | ${avgMutation.toFixed(3)}ms       | N/A           | Backgrounded  |`);
        console.log('-------------------------------------------------------------------------');

        await question('\nPress Enter to cleanup and exit...');
        await Promise.all([Organization.deleteMany({}), User.deleteMany({}), Project.deleteMany({})]);

    } catch (error: any) {
        console.error('\n❌ Error:', error.message);
    } finally {
        const mongoose = await import('mongoose');
        await mongoose.disconnect();
        rl.close();
        process.exit(0);
    }
}

runBenchmark();
