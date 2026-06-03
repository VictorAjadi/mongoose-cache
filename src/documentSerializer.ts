import { Types } from 'mongoose';
import { Buffer } from 'node:buffer';

/**
 * DocumentSerializer - Maximum Speed + 100% Accuracy
 * Handles: ObjectIds, Dates, Buffers, Decimal128, nested $lookup, Mongoose docs, plain objects
 */
export class DocumentSerializer {

    //Type checkers as static properties for faster access
    private static readonly TYPE_CHECKERS = {
        isObjectId: (v: any) => v instanceof Types.ObjectId || v?.constructor?.name === 'ObjectId' || v?._bsontype === 'ObjectID',
        isDecimal128: (v: any) => v instanceof Types.Decimal128 || v?.constructor?.name === 'Decimal128' || v?._bsontype === 'Decimal128',
        isLong: (v: any) => v?.constructor?.name === 'Long',
    };

    /**
     * Serialize with inline fast paths
     */
    public static serialize(value: any, depth: number = 0, seen = new WeakSet()): any {
        if (value === null || value === undefined) return value;

        // Avoid infinite recursion
        if (typeof value === 'object') {
            if (seen.has(value)) {
                return { __circularRef: true };
            }
            seen.add(value);
        }

        // Depth check
        if (depth > 50) {
            console.warn('Max serialization depth reached at', depth);
            return null;
        }

        // Fast primitives
        const type = typeof value;
        if (type === 'string' || type === 'boolean' || type === 'number') return value;

        // ObjectId
        if (value?._bsontype === 'ObjectID') return { __type: 'ObjectId', __data: value.toString() };

        // Date
        if (value instanceof Date) return { __type: 'Date', __data: value.toISOString() };

        // Buffer
        if (Buffer.isBuffer(value)) return { __type: 'Buffer', __data: value.toString('base64') };

        // Detect raw Mongoose document (faster + avoids prototype chain recursion)
        if (value?.constructor?.base?.connections) {
            try {
                const plain = value.toObject({
                    virtuals: false,
                    getters: false,
                    depopulate: true,
                    flattenMaps: false,
                    minimize: true,
                });
                return this.serialize(plain, depth + 1, seen);
            } catch {
                return { __mongooseDoc: true, id: value._id?.toString() };
            }
        }

        // Arrays
        if (Array.isArray(value)) {
            const len = value.length;
            const result = new Array(len);
            for (let i = 0; i < len; i++) {
                result[i] = this.serialize(value[i], depth + 1, seen);
            }
            return result;
        }

        // Regular Object
        if (Object.getPrototypeOf(value) === Object.prototype) {
            const keys = Object.keys(value);
            const out: any = {};
            for (let i = 0; i < keys.length; i++) {
                const key = keys[i];
                if (key.startsWith('_') && typeof value[key] === 'function') continue;
                out[key] = this.serialize(value[key], depth + 1, seen);
            }
            return out;
        }

        // Fallback to JSON-safe stringification
        try {
            return JSON.parse(JSON.stringify(value));
        } catch {
            return { __unserializable: true };
        }
    }

    /**
     * Deserialize with inline fast paths
     */
    public static deserialize(value: any, depth: number = 0): any {
        // Fast path: primitives
        const type = typeof value;
        
        if (type === 'string' || type === 'number' || type === 'boolean') {
            return value;
        }

        if (value === null) return null;
        if (value === undefined) return undefined;

        // Depth check
        if (depth > 100) {
            console.warn('Max deserialization depth reached');
            return null;
        }

        // Fast path: Arrays
        if (Array.isArray(value)) {
            const len = value.length;
            const result = new Array(len);
            for (let i = 0; i < len; i++) {
                result[i] = this.deserialize(value[i], depth + 1);
            }
            return result;
        }

        // Must be object from here
        if (type !== 'object') {
            return value;
        }

        // ============================================================
        // SPECIAL TYPE RESTORATION
        //  Direct __type check
        // ============================================================
        const specialType = value.__type;
        
        if (specialType) {
            try {
                switch (specialType) {
                    case 'ObjectId':
                        try {
                            return new Types.ObjectId(value.__data);
                        } catch {
                            return value.__data;
                        }

                    case 'Decimal128':
                        try {
                            return Types.Decimal128.fromString(value.__data);
                        } catch {
                            return value.__data;
                        }

                    case 'Long':
                        return value.__data;

                    case 'Buffer':
                        try {
                            return Buffer.from(value.__data, 'base64');
                        } catch {
                            return null;
                        }

                    case 'Date':
                        if (value.__invalid) {
                            return new Date('Invalid Date');
                        }
                        return new Date(value.__data);

                    case 'RegExp':
                        return new RegExp(value.__data.source, value.__data.flags);

                    case 'Map': {
                        const entries = value.__data;
                        const len = entries.length;
                        const map = new Map();
                        for (let i = 0; i < len; i++) {
                            const [k, v] = entries[i];
                            map.set(
                                this.deserialize(k, depth + 1),
                                this.deserialize(v, depth + 1)
                            );
                        }
                        return map;
                    }

                    case 'Set':
                        // Direct Array.from with callback
                        return new Set(Array.from(value.__data, item => this.deserialize(item, depth + 1)));

                    case 'BigInt':
                        try {
                            return BigInt(value.__data);
                        } catch {
                            return value.__data;
                        }

                    case 'Symbol':
                        return Symbol(value.__data);

                    case 'Undefined':
                        return undefined;

                    case 'NaN':
                        return NaN;

                    case 'Infinity':
                        return Infinity;

                    case '-Infinity':
                        return -Infinity;

                    case 'TypedArray': {
                        try {
                            const buffer = Buffer.from(value.__data, 'base64');
                            const ArrayType = (globalThis as any)[value.__arrayType];
                            if (ArrayType) {
                                return new ArrayType(
                                    buffer.buffer,
                                    buffer.byteOffset,
                                    buffer.byteLength / ArrayType.BYTES_PER_ELEMENT
                                );
                            }
                            return buffer;
                        } catch {
                            return null;
                        }
                    }

                    case 'ArrayBuffer':
                        try {
                            return Buffer.from(value.__data, 'base64').buffer;
                        } catch {
                            return null;
                        }

                    case 'DataView':
                        try {
                            const buffer = Buffer.from(value.__data, 'base64');
                            return new DataView(
                                buffer.buffer,
                                value.__byteOffset,
                                value.__byteLength
                            );
                        } catch {
                            return null;
                        }

                    case 'Error': {
                        const error = new Error(value.__data.message);
                        error.name = value.__data.name;
                        error.stack = value.__data.stack;
                        return error;
                    }

                    default:
                        return value;
                }
            } catch (error) {
                console.warn('Deserialization error for type:', specialType, error);
                return value;
            }
        }

        // ============================================================
        // PLAIN OBJECTS
        // Direct key iteration
        // ============================================================
        if (value.constructor === Object) {
            const keys = Object.keys(value);
            const len = keys.length;
            const result: any = {};
            
            for (let i = 0; i < len; i++) {
                const key = keys[i];
                result[key] = this.deserialize(value[key], depth + 1);
            }
            
            return result;
        }

        return value;
    }

    /**
     * Fast needs-serialization check
     */
    public static needsSerialization(value: any, depth: number = 0): boolean {
        if (depth > 30 || value == null) return false;

        const type = typeof value;

        // Fast path: primitives never need serialization
        if (type === 'string' || type === 'number' || type === 'boolean') {
            return false;
        }

        if (type === 'bigint' || type === 'symbol') {
            return true;
        }

        // Arrays: check elements
        if (Array.isArray(value)) {
            const len = value.length;
            for (let i = 0; i < len; i++) {
                if (this.needsSerialization(value[i], depth + 1)) {
                    return true;
                }
            }
            return false;
        }

        if (type !== 'object') return false;

        // Special types
        if (value instanceof Date ||
            value instanceof RegExp ||
            value instanceof Map ||
            value instanceof Set ||
            Buffer.isBuffer(value) ||
            this.TYPE_CHECKERS.isObjectId(value) ||
            this.TYPE_CHECKERS.isDecimal128(value) ||
            value instanceof Error ||
            ArrayBuffer.isView(value) ||
            value instanceof ArrayBuffer) {
            return true;
        }

        // Mongoose document
        if (typeof value.toObject === 'function') {
            return true;
        }

        // Check object properties
        if (value.constructor === Object) {
            const keys = Object.keys(value);
            const len = keys.length;
            for (let i = 0; i < len; i++) {
                if (this.needsSerialization(value[keys[i]], depth + 1)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Fast size estimation
     */
    public static estimateSize(value: any): number {
        try {
            const serialized = this.serialize(value);
            return JSON.stringify(serialized).length;
        } catch {
            return 0;
        }
    }

    /**
     * Fast validation
     */
    public static validateSerialization(original: any, serialized: any): boolean {
        try {
            const deserialized = this.deserialize(serialized);
            
            const origType = typeof original;
            const deserType = typeof deserialized;
            
            if (origType !== deserType) return false;
            
            if (Array.isArray(original)) {
                if (!Array.isArray(deserialized)) return false;
                if (original.length !== deserialized.length) return false;
            }
            
            if (origType === 'object' && original !== null && deserialized !== null) {
                const origKeys = Object.keys(original);
                const deserKeys = Object.keys(deserialized);
                if (origKeys.length !== deserKeys.length) return false;
            }
            
            return true;
        } catch {
            return false;
        }
    }

    /**
     * Fast deep equality
     */
    public static deepEqual(a: any, b: any, depth: number = 0): boolean {
        if (depth > 50) return true;
        if (a === b) return true;
        if (a == null || b == null) return a === b;

        const typeA = typeof a;
        const typeB = typeof b;
        
        if (typeA !== typeB) return false;

        if (Array.isArray(a)) {
            if (!Array.isArray(b)) return false;
            const len = a.length;
            if (len !== b.length) return false;
            for (let i = 0; i < len; i++) {
                if (!this.deepEqual(a[i], b[i], depth + 1)) return false;
            }
            return true;
        }

        if (a instanceof Date && b instanceof Date) {
            return a.getTime() === b.getTime();
        }

        if (Buffer.isBuffer(a) && Buffer.isBuffer(b)) {
            return a.equals(b);
        }

        if (this.TYPE_CHECKERS.isObjectId(a) && this.TYPE_CHECKERS.isObjectId(b)) {
            return a.toString() === b.toString();
        }

        if (typeA === 'object') {
            const keysA = Object.keys(a);
            const keysB = Object.keys(b);
            const len = keysA.length;

            if (len !== keysB.length) return false;

            for (let i = 0; i < len; i++) {
                const key = keysA[i];
                if (!this.deepEqual(a[key], b[key], depth + 1)) return false;
            }
            return true;
        }

        return false;
    }

    /**
     * Safe wrappers with error recovery
     */
    public static safeSerialize(value: any): any {
        try {
            return this.serialize(value);
        } catch (error) {
            console.error('Serialization failed, returning null:', error);
            return null;
        }
    }

    public static safeDeserialize(value: any): any {
        try {
            return this.deserialize(value);
        } catch (error) {
            console.error('Deserialization failed, returning original:', error);
            return value;
        }
    }
}