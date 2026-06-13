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

export class MongoDocumentUtils {
    private static readonly DEFAULT_MAX_DEPTH = 50;
    private static readonly CIRCULAR_REF_MARKER = Symbol('__CIRCULAR_REF__');
    
    /**
     * ObjectId validation with comprehensive checks
     */
    static isValidObjectId(id: any): boolean {
        if (!id) return false;
        
        // Handle ObjectId instances
        if (id._bsontype === 'ObjectID' || id._bsontype === 'ObjectId') return true;
        
        // Handle 24-char hex strings
        if (typeof id === 'string' && id.length === 24) {
             for (let i = 0; i < 24; i++) {
                const c = id.charCodeAt(i);
                if (!((c >= 48 && c <= 57) || (c >= 97 && c <= 102) || (c >= 65 && c <= 70))) return false;
             }
             return true;
        }
        
        return false;
    }

    /**
     * Safe ObjectId normalization with error handling
     */
    static normalizeId(id: any): ObjectId | string {
        try {
            if (!id) return id;
            
            if (id instanceof ObjectId) return id;
            
            if (this.isValidObjectId(id)) {
                return new ObjectId(id);
            }
            
            return id;
        } catch (error) {
            console.warn(`Failed to normalize ID ${id}:`, error);
            return id;
        }
    }

    /**
     * Enhanced document normalization with safety checks
     */
    static ensureMongoDocument(doc: any, options: CloneOptions = {}): DocumentLike {
        if (!doc || typeof doc !== 'object') {
            return doc;
        }

        // Prevent processing of circular references
        if (doc[this.CIRCULAR_REF_MARKER]) {
            return doc;
        }

        try {
            const normalized = { ...doc };
            
            // Mark as being processed to prevent infinite recursion
            Object.defineProperty(normalized, this.CIRCULAR_REF_MARKER, {
                value: true,
                enumerable: false,
                writable: false,
                configurable: true
            });

            // Safely handle _id field
            if (normalized._id !== undefined) {
                try {
                    normalized._id = this.normalizeId(normalized._id);
                } catch (error) {
                    console.warn('Failed to normalize _id:', error);
                }
            } else if (!options.excludeKeys?.includes('_id')) {
                normalized._id = new ObjectId();
            }

            // Add mongoose-like methods safely
            this.addMongooseMethods(normalized);

            // Clean up circular reference marker
            delete normalized[this.CIRCULAR_REF_MARKER];

            return normalized;
        } catch (error) {
            console.error('Error in ensureMongoDocument:', error);
            return doc;
        }
    }

    /**
     * Advanced deep cloning with circular reference detection and handling
     */
    static deepCloneDocument(doc: any, options: CloneOptions = {}): any {
        const {
            maxDepth = this.DEFAULT_MAX_DEPTH,
            preserveCircularRefs = false,
            excludeKeys = [],
            includePrivateKeys = false
        } = options;

        // Use WeakMap to track visited objects and handle circular references
        const visited = new WeakMap();
        const pathStack: string[] = [];

        const cloneRecursive = (obj: any, depth: number = 0, path: string = 'root'): any => {
            // Depth check to prevent stack overflow
            if (depth > maxDepth) {
                console.warn(`Maximum cloning depth (${maxDepth}) exceeded at path: ${path}`);
                return preserveCircularRefs ? `[Max Depth Exceeded: ${path}]` : null;
            }

            // Handle primitive types and null/undefined
            if (obj === null || obj === undefined) return obj;
            if (typeof obj !== 'object') return obj;

            // Handle special object types
            if (obj instanceof ObjectId) {
                try {
                    return new ObjectId(obj);
                } catch (error) {
                    console.warn(`Failed to clone ObjectId at ${path}:`, error);
                    return obj.toString();
                }
            }

            if (obj instanceof Date) return new Date(obj.getTime());
            if (obj instanceof RegExp) return new RegExp(obj);
            if (obj instanceof Map) {
                const clonedMap = new Map();
                for (const [key, value] of obj.entries()) {
                    clonedMap.set(key, cloneRecursive(value, depth + 1, `${path}[Map:${key}]`));
                }
                return clonedMap;
            }
            if (obj instanceof Set) {
                const clonedSet = new Set();
                for (const value of obj.values()) {
                    clonedSet.add(cloneRecursive(value, depth + 1, `${path}[Set]`));
                }
                return clonedSet;
            }

            // Handle circular references
            if (visited.has(obj)) {
                const circularPath = visited.get(obj);
                console.warn(`Circular reference detected: ${path} -> ${circularPath}`);
                return preserveCircularRefs ? `[Circular Reference: ${circularPath}]` : null;
            }

            // Mark object as visited
            visited.set(obj, path);
            pathStack.push(path);

            try {
                // Handle arrays
                if (Array.isArray(obj)) {
                    const len = obj.length;
                    const clonedArray = new Array(len);
                    for (let i = 0; i < len; i++) {
                        clonedArray[i] = cloneRecursive(obj[i], depth + 1, `${path}[${i}]`);
                    }
                    visited.delete(obj);
                    pathStack.pop();
                    return clonedArray;
                }

                // Handle plain objects
                const cloned: any = {};
                const keys = Object.keys(obj);
                const len = keys.length;
                
                for (let i = 0; i < len; i++) {
                    const key = keys[i];
                    // Skip excluded keys
                    if (excludeKeys.includes(key)) continue;
                    
                    // Skip private keys unless explicitly included
                    if (!includePrivateKeys && key[0] === '_' && key !== '_id') continue;
                    
                    const value = obj[key];
                    // Skip function properties and symbols
                    if (typeof value === 'function' || typeof key === 'symbol') continue;

                    const keyPath = `${path}.${key}`;
                    
                    try {
                        cloned[key] = cloneRecursive(value, depth + 1, keyPath);
                    } catch (error: any) {
                        if (preserveCircularRefs) {
                            cloned[key] = `[Clone Error]`;
                        }
                    }
                }

                // Apply MongoDB document normalization
                const normalized = this.ensureMongoDocument(cloned, options);
                
                visited.delete(obj);
                pathStack.pop();
                
                return normalized;

            } catch (error: any) {
                console.error(`Error cloning object at ${path}:`, error);
                visited.delete(obj);
                pathStack.pop();
                
                if (preserveCircularRefs) {
                    return `[Clone Error: ${error?.message}]`;
                }
                return null;
            }
        };

        try {
            return cloneRecursive(doc);
        } catch (error: any) {
            console.error('Deep clone failed:', error);
            return preserveCircularRefs ? `[Clone Failed: ${error?.message}]` : null;
        }
    }

    /**
     * Safe ID comparison with comprehensive validation
     */
    static compareIds(id1: any, id2: any): boolean {
        if (id1 === id2) return true;
        if (!id1 || !id2) return false;

        // HIGH-SPEED ID COMPARISON
        // Bypasses Regex and normalization for common types
        const s1 = (typeof id1 === 'string') ? id1 : (id1.toHexString ? id1.toHexString() : String(id1));
        const s2 = (typeof id2 === 'string') ? id2 : (id2.toHexString ? id2.toHexString() : String(id2));
        
        if (s1.length !== s2.length) return false;
        return s1 === s2;
    }

    /**
     * Detect circular references in an object
     */
    static hasCircularReference(obj: any, visited = new WeakSet()): boolean {
        if (!obj || typeof obj !== 'object') return false;
        if (visited.has(obj)) return true;
        
        visited.add(obj);
        
        try {
            if (Array.isArray(obj)) {
                for (let i = 0; i < obj.length; i++) {
                    if (this.hasCircularReference(obj[i], visited)) return true;
                }
                return false;
            }
            
            const keys = Object.keys(obj);
            const len = keys.length;
            for (let i = 0; i < len; i++) {
                if (this.hasCircularReference(obj[keys[i]], visited)) return true;
            }
            return false;
        } finally {
            visited.delete(obj);
        }
    }

    /**
     * Sanitize document for safe serialization
     */
    static sanitizeForSerialization(doc: any): any {
        const options: CloneOptions = {
            maxDepth: 20,
            preserveCircularRefs: true,
            excludeKeys: ['__v', '__cached', '__session'],
            includePrivateKeys: false
        };
        
        return this.deepCloneDocument(doc, options);
    }

    /**
     * Create a cache-safe version of a document
     */
    static createCacheSafeDocument(doc: any): any {
        if (!doc) return doc;
        
        // First, check for circular references
        if (this.hasCircularReference(doc)) {
            console.warn('Circular reference detected, creating safe copy');
            return this.sanitizeForSerialization(doc);
        }
        
        // For simple documents, use regular deep clone
        return this.deepCloneDocument(doc, {
            maxDepth: 30,
            preserveCircularRefs: false,
            excludeKeys: ['__v', '__session', '__populated']
        });
    }

    /**
     * Add mongoose-like methods to normalized documents
     */
    private static addMongooseMethods(normalized: any): void {
        if (!normalized.toObject) {
            normalized.toObject = function() { 
                return MongoDocumentUtils.sanitizeForSerialization(this);
            };
        }
        
        if (!normalized.toJSON) {
            normalized.toJSON = function() { 
                return MongoDocumentUtils.sanitizeForSerialization(this);
            };
        }

        if (!normalized.populate) {
            normalized.populate = function() { 
                return Promise.resolve(this); 
            };
        }

        if (!normalized.isModified) {
            normalized.isModified = function() { 
                return false; 
            };
        }
    }

    /**
     * Safe document validation
     */
    static isValidDocument(doc: any): boolean {
        try {
            if (!doc || typeof doc !== 'object') return false;
            if (this.hasCircularReference(doc)) return false;
            if (doc._id && !this.isValidObjectId(doc._id)) return false;
            return true;
        } catch (error) {
            console.warn('Document validation failed:', error);
            return false;
        }
    }
}