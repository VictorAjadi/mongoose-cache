import { OptimizedQueryMatcher } from "./OptimizedQueryMatcher";
import { MongoDocumentUtils } from "./MongoDocumentUtils";

// Optimized update operations with Copy-on-Write logic
export class UpdateOperations {
    private static readonly PATH_CACHE = new Map<string, string[]>();

    /**
     * Applies MongoDB update operators to a document with Copy-on-Write optimization.
     */
    static applyUpdateToDocument(doc: any, updateData: any): any {
        if (!updateData || typeof updateData !== 'object') return doc;

        let updated = doc;
        let isCloned = false;

        const ensureCloned = () => {
            if (isCloned) return;
            // High-speed clone using faster utility
            updated = MongoDocumentUtils.deepCloneDocument(doc, { maxDepth: 10 });
            isCloned = true;
        };

        const keys = Object.keys(updateData);
        for (let i = 0; i < keys.length; i++) {
            const operator = keys[i];
            const fields = updateData[operator];
            if (operator[0] === '$') {
                ensureCloned();
                this.applyOperator(updated, operator, fields);
            } else {
                // Direct field assignment (Mongoose behavior for top-level keys)
                ensureCloned();
                this.setNestedValue(updated, operator, fields);
            }
        }

        return isCloned ? updated : doc;
    }

    private static applyOperator(doc: any, operator: string, fields: any): void {
        switch (operator) {
            case '$set':
                this.applySet(doc, fields);
                break;
            case '$inc':
                this.applyIncrement(doc, fields);
                break;
            case '$unset':
                this.applyUnset(doc, fields);
                break;
            case '$push':
                this.applyPush(doc, fields);
                break;
            case '$pull':
                this.applyPull(doc, fields);
                break;
            case '$addToSet':
                this.applyAddToSet(doc, fields);
                break;
            case '$pop':
                this.applyPop(doc, fields);
                break;
        }
    }

    private static applySet(doc: any, fields: any): void {
        for (const [field, value] of Object.entries(fields)) {
            this.setNestedValue(doc, field, value);
        }
    }

    private static applyIncrement(doc: any, fields: any): void {
        for (const [field, value] of Object.entries(fields)) {
            const currentValue = this.getNestedValue(doc, field) || 0;
            this.setNestedValue(doc, field, currentValue + (value as number));
        }
    }

    private static applyUnset(doc: any, fields: any): void {
        for (const field of Object.keys(fields)) {
            this.unsetNestedValue(doc, field);
        }
    }

    private static applyPush(doc: any, fields: any): void {
        for (const [field, value] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field) || [];
            if (Array.isArray(array)) {
                if (typeof value === 'object' && value !== null && '$each' in value) {
                    array.push(...(Array.isArray(value.$each) ? value.$each : [value.$each]));
                } else {
                    array.push(value);
                }
            }
        }
    }

    private static applyPull(doc: any, fields: any): void {
        for (const [field, condition] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field);

            // $pull should only work on arrays (MongoDB behavior)
            if (array === undefined) {
                // Field doesn't exist - no-op in MongoDB
                continue;
            }

            if (!Array.isArray(array)) {
                // Field exists but isn't array
                // MongoDB throws error, we log warning for cache consistency
                if (typeof (globalThis as any).process !== 'undefined' &&
                    (globalThis as any).process.env?.NODE_ENV === 'development') {
                    console.warn(
                        `[UpdateOperations] $pull on non-array field '${field}'. ` +
                        `MongoDB would error here. Current value: ${typeof array}`
                    );
                }
                continue;
            }

            // Now safe to filter
            const filtered = array.filter(item =>
                !OptimizedQueryMatcher.documentMatchesQuery(item, condition)
            );
            this.setNestedValue(doc, field, filtered);
        }
    }

    private static applyAddToSet(doc: any, fields: any): void {
        for (const [field, value] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field) || [];
            if (Array.isArray(array)) {
                const items = typeof value === 'object' && value !== null && '$each' in value
                    ? (Array.isArray(value.$each) ? value.$each : [value.$each])
                    : [value];

                for (const item of items) {
                    if (!array.some(existing => OptimizedQueryMatcher['valuesEqual'](existing, item))) {
                        array.push(item);
                    }
                }
            }
        }
    }

    private static applyPop(doc: any, fields: any): void {
        for (const [field, direction] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field);
            if (Array.isArray(array) && array.length > 0) {
                if (direction === 1) array.pop();
                else array.shift();
            }
        }
    }

    private static getNestedValue(obj: any, path: string): any {
        if (obj == null) return undefined;
        if (!path.includes('.')) return obj[path];

        let keys = this.PATH_CACHE.get(path);
        if (!keys) {
            keys = path.split('.');
            if (this.PATH_CACHE.size < 1000) this.PATH_CACHE.set(path, keys);
        }

        let current = obj;
        for (let i = 0; i < keys.length; i++) {
            if (current == null) return undefined;
            current = current[keys[i]];
        }
        return current;
    }

    private static setNestedValue(obj: any, path: string, value: any): void {
        let keys = this.PATH_CACHE.get(path);
        if (!keys) {
            keys = path.split('.');
            if (this.PATH_CACHE.size < 1000) this.PATH_CACHE.set(path, keys);
        }

        const lastKey = keys[keys.length - 1];
        let current = obj;

        for (let i = 0; i < keys.length - 1; i++) {
            const key = keys[i];
            if (!(key in current) || current[key] == null || typeof current[key] !== 'object') {
                current[key] = {};
            }
            current = current[key];
        }

        current[lastKey] = value;
    }

    private static unsetNestedValue(obj: any, path: string): void {
        let keys = this.PATH_CACHE.get(path);
        if (!keys) {
            keys = path.split('.');
            if (this.PATH_CACHE.size < 1000) this.PATH_CACHE.set(path, keys);
        }

        const lastKey = keys[keys.length - 1];
        let current = obj;

        for (let i = 0; i < keys.length - 1; i++) {
            if (current?.[keys[i]] == null) return;
            current = current[keys[i]];
        }

        if (current && typeof current === 'object') {
            delete current[lastKey];
        }
    }
}