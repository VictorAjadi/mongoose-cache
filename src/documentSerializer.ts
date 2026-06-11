


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
     * Serializes a value and returns both the data and its BSON size.
     * @param value - Value to serialize (Document, Object, Array, Primitive)
     * @param depth - Current recursion depth
     * @param seen - Circular reference detector
     */
    public static serialize(value: any, depth: number = 0, seen = new WeakSet()): { data: any; size: number } {
        // 1. PRIMITIVES
        if (value === null || value === undefined) return { data: value, size: 1 };
        
        const type = typeof value;
        if (type !== 'object') {
            if (type === 'string') return { data: value, size: value.length + 5 }; // int32 len + null term
            if (type === 'number') return { data: value, size: 8 }; // Double
            if (type === 'boolean') return { data: value, size: 1 };
            if (type === 'bigint') return { data: value.toString(), size: 8 };
            return { data: String(value), size: 8 };
        }

        // 2. SAFETY: Circular refs & max depth
        if (seen.has(value)) return { data: null, size: 0 };
        if (depth > 50) return { data: null, size: 0 };
        seen.add(value);

        // 3. BSON / SPECIAL TYPES
        const ctorName = value.constructor?.name;

        if (ctorName === 'ObjectId' || value._bsontype === 'ObjectID') {
            return { data: value.toString(), size: 12 };
        }

        if (value instanceof Date) {
            return { data: value.toISOString(), size: 8 };
        }

        if (ctorName === 'Decimal128' || value._bsontype === 'Decimal128') {
            return { data: value.toString(), size: 16 };
        }

        if (Buffer.isBuffer(value)) {
            return { data: value.toString('base64'), size: value.length + 5 };
        }

        // 4. ARRAYS
        if (Array.isArray(value)) {
            const len = value.length;
            const result = new Array(len);
            let totalSize = 5; // Array overhead (int32 size + null term)
            
            for (let i = 0; i < len; i++) {
                const item = this.serialize(value[i], depth + 1, seen);
                result[i] = item.data;
                totalSize += item.size + 1; // 1 byte type overhead per element
            }
            return { data: result, size: totalSize };
        }

        // 5. OBJECTS & MONGOOSE DOCUMENTS
        const result: any = {};
        const keys = Object.keys(value);
        let totalSize = 5; // Document overhead (int32 size + null term)
        const isMongoose = !!(value.$__ || value._doc);

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            
            // Skip internals
            if (isMongoose && this.MONGOOSE_INTERNALS.has(key)) continue;
            if (key.startsWith('$')) continue;

            const val = value[key];
            const item = this.serialize(val, depth + 1, seen);
            
            result[key] = item.data;
            totalSize += item.size + key.length + 2; // key length + null term + type byte
        }

        return { data: result, size: totalSize };
    }

    /**
     * Reconstructs a value from its serialized form.
     */
    public static deserialize(data: any): any {
        return data; // Memory cache stores pure POJOs
    }
}
