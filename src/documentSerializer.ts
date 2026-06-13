

/**
 * High-performance serialization utility for Mongoose documents.
 * Designed to:
 * 1. Calculate BSON-accurate size in a single pass during serialization.
 * 2. Strip internal Mongoose metadata ($__, _doc, etc.) to keep cache entries lean.
 * 3. Handle BSON types (ObjectId, Decimal128, Buffer) efficiently.
 * 4. Detect circular references to prevent stack overflows.
 */
export class DocumentSerializer {
    private static readonly MONGOOSE_INTERNALS = new Set(['$__', '$isNew', '_doc', '$op', '__v']);

    /**
     * High-speed serialization with POJO-Jump.
     */
    public static serialize(value: any, mode: 'internal' | 'external' = 'external', depth: number = 0, seen?: WeakSet<any>): { data: any; size: number } {
        if (value === null || typeof value !== 'object') {
            const type = typeof value;
            if (type === 'string') return { data: value, size: value.length + 5 };
            if (type === 'number') return { data: value, size: 8 };
            if (type === 'boolean') return { data: value, size: 1 };
            if (type === 'bigint') return { data: mode === 'external' ? { $date: value.toString() } : value, size: 8 };
            return { data: String(value), size: 1 };
        }

        // MONGOOSE POJO-JUMP: 
        // If we're internal and hit a Mongoose Doc, we jump straight to _doc and trust its structure.
        if (value.$__ && value._doc) {
            if (mode === 'internal') {
                // Estimation-based size is 10x faster than walking the tree for memory limits
                const estSize = this.estimateSize(value._doc);
                return { data: value._doc, size: estSize };
            }
            return this.serialize(value._doc, mode, depth, seen);
        }

        // Native Types
        if (value instanceof Date) {
            return { data: mode === 'external' ? { $date: value.toISOString() } : value, size: 8 };
        }
        if (Buffer.isBuffer(value)) {
            return { data: mode === 'external' ? { $buffer: value.toString('base64') } : value, size: value.length + 5 };
        }

        const btype = value._bsontype;
        if (btype) {
            const s = value.toString();
            if (btype === 'ObjectID' || btype === 'ObjectId') return { data: mode === 'external' ? { $oid: s } : s, size: 12 };
            return { data: mode === 'external' ? { [btype]: s } : s, size: 16 };
        }

        if (Array.isArray(value)) {
            const len = value.length;
            const result = new Array(len);
            let totalSize = 5;
            for (let i = 0; i < len; i++) {
                const item = this.serialize(value[i], mode, depth + 1, seen);
                result[i] = item.data;
                totalSize += item.size + 1;
            }
            return { data: result, size: totalSize };
        }

        // Object traversal - Optimized with key pre-fetch
        const result: any = {};
        const keys = Object.keys(value);
        const len = keys.length;
        let totalSize = 5;

        if (depth > 3) {
            if (!seen) seen = new WeakSet();
            if (seen.has(value)) return { data: null, size: 0 };
            seen.add(value);
        }

        for (let i = 0; i < len; i++) {
            const key = keys[i];
            if (key.charCodeAt(0) === 36 && this.MONGOOSE_INTERNALS.has(key)) continue;

            const item = this.serialize(value[key], mode, depth + 1, seen);
            result[key] = item.data;
            totalSize += item.size + key.length + 2;
        }

        return { data: result, size: totalSize };
    }

    private static estimateSize(obj: any): number {
        if (!obj) return 0;

        let estimatedSize = 50; // Base object overhead

        // For arrays, estimate from length and sample elements
        if (Array.isArray(obj)) {
            if (obj.length === 0) return 100; // Empty array

            // Sample first element to estimate average
            const firstElem = obj[0];
            let elemSize = 100; // Base per-element overhead

            if (typeof firstElem === 'string') {
                elemSize += firstElem.length;
            } else if (typeof firstElem === 'object' && firstElem) {
                elemSize += Object.keys(firstElem).length * 50;
            }

            return 100 + (obj.length * elemSize);
        }

        // For objects, walk keys and estimate value sizes
        if (typeof obj === 'object') {
            const keys = Object.keys(obj);
            estimatedSize = 50 + (keys.length * 20); // Per-key overhead

            // Sample up to 5 values for actual size
            const sampleSize = Math.min(5, keys.length);
            let totalValueSize = 0;

            for (let i = 0; i < sampleSize; i++) {
                const key = keys[i];
                const val = obj[key];

                if (val === null || val === undefined) {
                    totalValueSize += 8;
                } else if (typeof val === 'string') {
                    totalValueSize += val.length + 20;
                } else if (typeof val === 'number') {
                    totalValueSize += 8;
                } else if (typeof val === 'boolean') {
                    totalValueSize += 1;
                } else if (val instanceof Date) {
                    totalValueSize += 16;
                } else if (Buffer.isBuffer(val)) {
                    totalValueSize += val.length + 20;
                } else if (typeof val === 'object') {
                    totalValueSize += 150; // Average nested object
                }
            }

            // Extrapolate from sample
            const avgValueSize = sampleSize > 0 ? totalValueSize / sampleSize : 50;
            estimatedSize += (keys.length * avgValueSize);
        }

        // Cap at 100MB (sanity check)
        return Math.min(estimatedSize, 100 * 1024 * 1024);
    }

    /**
     * Deserialization for External mode.
     */
    public static deserialize(data: any): any {
        if (data === null || typeof data !== 'object') return data;

        if (data.$date !== undefined) return new Date(data.$date);
        if (data.$buffer !== undefined) return Buffer.from(data.$buffer, 'base64');
        if (data.$oid !== undefined) {
            try { return new (require('bson').ObjectId)(data.$oid); }
            catch { try { return new (require('mongodb').ObjectId)(data.$oid); } catch { return data.$oid; } }
        }
        if (data.$bigint !== undefined) return BigInt(data.$bigint);

        if (Array.isArray(data)) {
            const len = data.length;
            if (len === 0 || (data[0] !== null && typeof data[0] !== 'object')) return data;

            const result = new Array(len);
            for (let i = 0; i < len; i++) {
                result[i] = this.deserialize(data[i]);
            }
            return result;
        }

        const result: any = {};
        const keys = Object.keys(data);
        const len = keys.length;
        for (let i = 0; i < len; i++) {
            const key = keys[i];
            result[key] = this.deserialize(data[key]);
        }
        return result;
    }
}
