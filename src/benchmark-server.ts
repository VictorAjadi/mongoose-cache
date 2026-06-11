
import express from 'express';
import { connect, Schema, model } from 'mongoose';
import { initCache } from './index';

/**
 * ============================================================================
 * @mongoose-cache - Heavy Relational Express Server
 * ============================================================================
 */

const app = express();
const port = 3000;

// Initialize Cache
const cache = initCache({ ttl: 3600, debug: true });

// 1. PRODUCTION-HEAVY MODELS
const OrgSchema = new Schema({
    name: String,
    settings: {
        features: [String],
        branding: { colors: { primary: String, secondary: String } }
    },
    subscription: { tier: String, status: String, expiresAt: Date }
});

const UserSchema = new Schema({
    name: { first: String, last: String },
    email: String,
    orgId: { type: Schema.Types.ObjectId, ref: 'Organization' },
    profile: { preferences: { theme: String, notifications: { email: Boolean } } }
});

const ProjectSchema = new Schema({
    title: String,
    ownerId: { type: Schema.Types.ObjectId, ref: 'User' },
    milestones: [{
        name: String,
        dueDate: Date,
        tasks: [{ title: String, priority: Number }]
    }],
    resources: { budget: Number, currency: String },
    flags: { tags: [String] }
});

cache.applyCacheToQueries(OrgSchema);
cache.applyCacheToQueries(UserSchema);
cache.applyCacheToQueries(ProjectSchema);

const Organization = model('Organization', OrgSchema);
const User = model('User', UserSchema);
const Project = model('Project', ProjectSchema);

app.use(express.json());

// --- ROUTES ---

// CACHED: Deeply Populated (3 models with nested data)
app.get('/projects/:id', async (req, res) => {
    const start = performance.now();
    const project = await Project.findById(req.params.id)
        .populate({ path: 'ownerId', populate: { path: 'orgId' } })
        .lean();
    const end = performance.now();

    res.json({
        executionTime: `${(end - start).toFixed(4)}ms`,
        project
    });
});

// CACHED: Complex Analytics Aggregation
app.get('/stats', async (_req, res) => {
    const start = performance.now();
    const stats = await Project.aggregate([
        { $unwind: '$milestones' },
        {
            $group: {
                _id: '$resources.currency',
                count: { $sum: 1 },
                totalBudget: { $sum: '$resources.budget' }
            }
        }
    ]);
    const end = performance.now();

    res.json({
        executionTime: `${(end - start).toFixed(4)}ms`,
        stats
    });
});

async function start() {
    try {
        await connect('mongodb://localhost:27017/benchmark-db');
        console.log('✅ Connected to MongoDB');

        // Seed if empty
        const count = await Project.countDocuments();
        if (count === 0) {
            console.log('Seeding heavy relational data...');
            const org = await Organization.create({
                name: 'Global Enterprise',
                settings: { features: ['auth', 'billing', 'api'], branding: { colors: { primary: '#007bff' } } }
            });
            const user = await User.create({
                name: { first: 'Senior', last: 'Developer' },
                email: 'dev@global.com',
                orgId: org._id,
                profile: { preferences: { theme: 'dark', notifications: { email: true } } }
            });
            await Project.create({
                title: 'Operation Antigravity',
                ownerId: user._id,
                milestones: [
                    { name: 'Core Engine', dueDate: new Date(), tasks: [{ title: 'Memory Manager', priority: 1 }] },
                    { name: 'Testing', dueDate: new Date(), tasks: [{ title: 'Unit Tests', priority: 2 }] }
                ],
                resources: { budget: 1500000, currency: 'USD' },
                flags: { tags: ['node', 'mongoose', 'performance'] }
            });
            console.log('Heavy seed data ready.');
        }

        const sample = await Project.findOne();

        app.listen(port, () => {
            console.log(`\n🚀 Heavy Relational Server: http://localhost:${port}`);
            console.log(`- GET  /projects/${sample?._id} -> Deep populated read`);
            console.log(`- GET  /stats           -> Complex aggregation analytics\n`);
        });
    } catch (err) {
        console.error('Server startup failed:', err);
    }
}

start();
