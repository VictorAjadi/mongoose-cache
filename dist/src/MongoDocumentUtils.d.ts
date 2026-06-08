import { ObjectId } from 'mongodb';
interface CloneOptions {
    maxDepth?: number;
    preserveCircularRefs?: boolean;
    excludeKeys?: string[];
    includePrivateKeys?: boolean;
}
interface DocumentLike {
    _id?: ObjectId | string;
    [key: string]: any;
    toObject?(): any;
    toJSON?(): any;
    populate?(path?: string): Promise<any>;
    isModified?(path?: string): boolean;
}
export declare class MongoDocumentUtils {
    private static readonly DEFAULT_MAX_DEPTH;
    private static readonly CIRCULAR_REF_MARKER;
    /**
     * ObjectId validation with comprehensive checks
     */
    static isValidObjectId(id: any): boolean;
    /**
     * Safe ObjectId normalization with error handling
     */
    static normalizeId(id: any): ObjectId | string;
    /**
     * Enhanced document normalization with safety checks
     */
    static ensureMongoDocument(doc: any, options?: CloneOptions): DocumentLike;
    /**
     * Advanced deep cloning with circular reference detection and handling
     */
    static deepCloneDocument(doc: any, options?: CloneOptions): any;
    /**
     * Safe ID comparison with comprehensive validation
     */
    static compareIds(id1: any, id2: any): boolean;
    /**
     * Detect circular references in an object
     */
    static hasCircularReference(obj: any, visited?: WeakSet<WeakKey>): boolean;
    /**
     * Sanitize document for safe serialization
     */
    static sanitizeForSerialization(doc: any): any;
    /**
     * Create a cache-safe version of a document
     */
    static createCacheSafeDocument(doc: any): any;
    /**
     * Add mongoose-like methods to normalized documents
     */
    private static addMongooseMethods;
    /**
     * Safe document validation
     */
    static isValidDocument(doc: any): boolean;
}
export {};
