/**
 * ============================================================================
 * @mongoose-cache - Library Entry Point
 * ============================================================================
 *
 * The main export file for mongoose-cache library.
 * Exports all public APIs, types, and utilities needed to use the library.
 *
 * Usage:
 * ```typescript
 * import { MongooseCache, CacheConfig } from 'mongoose-cache-lib';
 *
 * const cache = new MongooseCache({ ttl: 600 });
 * cache.applyCacheToQueries(userSchema);
 * ```
 * ============================================================================
 */
export { MongooseCache } from './cache-types/MongooseCache';
export { UnifiedCache } from './cache-types/UnifiedCache';
export { MemoryCache } from './cache-types/MemoryCache';
export { RedisAdapter } from './adapters/RedisAdapter';
export { DocumentSerializer } from './documentSerializer';
export type { CacheConfig, CacheEntry, CacheStats, IndexEntry } from './config';
export { DEFAULT_CONFIG } from './config';
export { default as PipelineHashGenerator } from './PipelineHashGenerator';
export { SizeCalculator } from './SizeCalculator';
export { MongoDocumentUtils } from './MongoDocumentUtils';
export { OptimizedQueryMatcher } from './OptimizedQueryMatcher';
export { UpdateOperations } from './UpdateOperations';
export { MemoryMonitor } from './MemoryMonitor';
export { default as JSONSchemaValidator } from './JSONSchemaValidator';
export { HyperHashMap } from './hyperhashmap';
export { FastArray } from './fastarray';
export type { CacheConfig as CacheOptions, } from './config';
import { MongooseCache } from './cache-types/MongooseCache';
import { CacheConfig } from './config';
/**
 * Initialize a global cache instance to be used throughout the application.
 */
export declare const initCache: (config?: CacheConfig) => MongooseCache;
/**
 * Retrieve the global cache instance. Throws if not initialized.
 */
export declare const getCache: () => MongooseCache;
