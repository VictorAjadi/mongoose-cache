# 🚀 @mongoose-cache

**Dramatically faster Mongoose queries with production-grade, smart-invalidation caching.**

`@mongoose-cache` is a professional, transparent caching layer built for modern Node.js and Bun environments. It intercepts Mongoose queries (`find`, `aggregate`, `count`, `distinct`) and intelligently invalidates them on writes, reducing database load by up to **90%** while providing sub-millisecond response times.

---

## ✨ Core Features

- **⚡ Zero-Config Acceleration**: Just apply it to your schema and watch performance soar.
- **🧠 Smart Invalidation**: Surgical cache busts based on query pattern matching, not just model names.
- **🔄 Batch I/O**: Merges successive cache writes into 50ms windows, saving thousands of Redis operations.
- **🛡️ Memory Protection**: Built-in circuit breakers flush queues before heap memory reaches critical levels.
- **🌍 Hybrid Backend**: Seamlessly uses **Redis** for distributed systems or **LRU-Memory** for local speed.
- **🌱 Environment Native**: First-class support for both **Node.js** and **Bun** runtimes.

---

## 📦 Installation

```bash
npm install @mongoose-cache
# OR
bun add @mongoose-cache
```

---

## 🚀 Quick Start (Single Instance Pattern)

The recommended way to use `@mongoose-cache` is to initialize it once and share it across your application.

```typescript
// 1. Initialize once (e.g., in lib/cache.ts)
import { initCache } from '@mongoose-cache';

export const cache = initCache({
  ttl: 600, // 10 minutes
  redis: { host: 'localhost', port: 6379 }
});

// 2. Apply to your Schemas (e.g., in models/User.ts)
import { Schema, model } from 'mongoose';
import { cache } from '../lib/cache';

const userSchema = new Schema({ name: String, email: String });
cache.applyCacheToQueries(userSchema); // Caching is now fully active

export const User = model('User', userSchema);

// 3. Just use Mongoose as usual!
const users = await User.find({ name: 'Alice' }); // Automatically cached
```

---

## 🛠️ API Reference

### Global Helpers
| Function | Parameters | Returns | Description |
| :--- | :--- | :--- | :--- |
| `initCache(config?)` | `CacheConfig` | `MongooseCache` | Creates and sets the global singleton instance. |
| `getCache()` | - | `MongooseCache` | Retrieves the global instance (throws if not initialized). |

### `MongooseCache` Instance Methods
| Method | Parameters | Returns | Description |
| :--- | :--- | :--- | :--- |
| `applyCacheToQueries` | `schema, options?` | `void` | Automatically hooks read/write operations for a schema. |
| `getStats()` | - | `Promise<Stats>`| Get detailed performance metrics (hits, misses, etc). |
| `invalidateModel` | `modelName` | `Promise<number>` | Manually clear all cached results for a specific model. |
| `warmCache` | `model, queries[]` | `Promise<void>` | Pre-load common queries into the cache on startup. |
| `flushCache()` | - | `Promise<void>` | complete wipe of all data in the cache backend. |
| `batchInvalidate` | `operations[]` | `Promise<void>` | Process multiple surgical invalidations in one call. |
| `ping()` | - | `Promise<bool>` | Check if the cache backend (Redis/Memory) is alive. |
| `disconnect()` | - | `Promise<void>` | Gracefully flush queues and close connections. |

---

## 🔧 Configuration Options

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `ttl` | `number` | `300` | Global TTL in seconds for cached entries. |
| `maxKeys` | `number` | `10000` | Maximum number of keys to hold in local memory (LRU). |
| `memoryThreshold` | `number` | `60` | Trigger queue flush when heap usage exceeds this %. |
| `enableSmartInvalidation` | `boolean` | `true` | Pattern-based invalidation (e.g., invalidate by filters). |
| `useCryptoHash` | `boolean` | `false` | Use SHA-256 for deterministic keys across clusters. |
| `debug` | `boolean` | `false` | Enable verbose logging for debugging cache hits/misses. |
| `redis` | `object` | `undefined` | ioredis compatible connection configuration. |

---

## 🧠 Why @mongoose-cache?

Standard caching often clears the *entire* model cache whenever *any* record is updated. This results in "cache thrashing." 

`@mongoose-cache` uses **Optimized Query Matching**:
1. When you query: `User.find({ status: 'active' })`, the library indexes that specific pattern.
2. When you update: a user with `{ status: 'inactive' }`, the library determines if it *could* have affected your previous query.
3. If not affected, the cache stays hot!

This "surgical" approach ensures higher hit rates even in write-heavy environments.

---

## ⚡ Benchmarking

Test the impact in your own environment:
```bash
npm run benchmark
```
*Expected results: Latency drops from ~30ms to <1ms for cached hits.*

---

## 🤝 Contributing & Support

- **Node.js**: 16.x, 18.x, 20.x+
- **Bun**: 1.0+
- **Mongoose**: 6.x, 7.x, 8.x

## 📝 License
MIT
