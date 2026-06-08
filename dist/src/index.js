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
// ============================================================================
// MAIN CACHE CLASS
// ============================================================================
export { MongooseCache } from './cache-types/MongooseCache';
// ============================================================================
// CACHE SYSTEM CLASSES
// ============================================================================
export { UnifiedCache } from './cache-types/UnifiedCache';
export { MemoryCache } from './cache-types/MemoryCache';
export { RedisAdapter } from './adapters/RedisAdapter';
// ============================================================================
// SERIALIZATION
// ============================================================================
export { DocumentSerializer } from './documentSerializer';
export { DEFAULT_CONFIG } from './config';
// ============================================================================
// UTILITY CLASSES
// ============================================================================
export { default as PipelineHashGenerator } from './PipelineHashGenerator';
export { SizeCalculator } from './SizeCalculator';
export { MongoDocumentUtils } from './MongoDocumentUtils';
export { OptimizedQueryMatcher } from './OptimizedQueryMatcher';
export { UpdateOperations } from './UpdateOperations';
export { MemoryMonitor } from './MemoryMonitor';
export { default as JSONSchemaValidator } from './JSONSchemaValidator';
// ============================================================================
// DATA STRUCTURES
// ============================================================================
export { HyperHashMap } from './hyperhashmap';
export { FastArray } from './fastarray';
// Global instance management for "one cache declaration" pattern
import { MongooseCache } from './cache-types/MongooseCache';
let globalInstance = null;
/**
 * Initialize a global cache instance to be used throughout the application.
 */
export const initCache = (config) => {
    globalInstance = new MongooseCache(config);
    return globalInstance;
};
/**
 * Retrieve the global cache instance. Throws if not initialized.
 */
export const getCache = () => {
    if (!globalInstance) {
        throw new Error('@mongoose-cache: Global instance not initialized. Call initCache() first.');
    }
    return globalInstance;
};
