import { MongoDocumentUtils } from "./MongoDocumentUtils";
import { OptimizedQueryMatcher } from "./OptimizedQueryMatcher";
// Optimized update operations
export class UpdateOperations {
    static applyUpdateToDocument(doc, updateData) {
        if (!updateData || typeof updateData !== 'object')
            return doc;
        const updated = MongoDocumentUtils.deepCloneDocument(doc);
        for (const [operator, fields] of Object.entries(updateData)) {
            this.applyOperator(updated, operator, fields);
        }
        return MongoDocumentUtils.ensureMongoDocument(updated);
    }
    static applyOperator(doc, operator, fields) {
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
            default:
                if (!operator.startsWith('$')) {
                    // Direct field assignment
                    this.setNestedValue(doc, operator, fields);
                }
        }
    }
    static applySet(doc, fields) {
        for (const [field, value] of Object.entries(fields)) {
            this.setNestedValue(doc, field, value);
        }
    }
    static applyIncrement(doc, fields) {
        for (const [field, value] of Object.entries(fields)) {
            const currentValue = this.getNestedValue(doc, field) || 0;
            this.setNestedValue(doc, field, currentValue + value);
        }
    }
    static applyUnset(doc, fields) {
        for (const field of Object.keys(fields)) {
            this.unsetNestedValue(doc, field);
        }
    }
    static applyPush(doc, fields) {
        for (const [field, value] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field) || [];
            if (Array.isArray(array)) {
                const newArray = [...array];
                if (typeof value === 'object' && value !== null && '$each' in value) {
                    // Handle $push with $each
                    const items = Array.isArray(value.$each) ? value.$each : [value.$each];
                    newArray.push(...items);
                }
                else {
                    newArray.push(value);
                }
                this.setNestedValue(doc, field, newArray);
            }
        }
    }
    static applyPull(doc, fields) {
        for (const [field, condition] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field);
            if (Array.isArray(array)) {
                const newArray = array.filter(item => !OptimizedQueryMatcher.documentMatchesQuery(item, condition));
                this.setNestedValue(doc, field, newArray);
            }
        }
    }
    static applyAddToSet(doc, fields) {
        for (const [field, value] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field) || [];
            if (Array.isArray(array)) {
                const newArray = [...array];
                const items = typeof value === 'object' && value !== null && '$each' in value
                    ? (Array.isArray(value.$each) ? value.$each : [value.$each])
                    : [value];
                for (const item of items) {
                    if (!newArray.some(existing => OptimizedQueryMatcher['valuesEqual'](existing, item))) {
                        newArray.push(item);
                    }
                }
                this.setNestedValue(doc, field, newArray);
            }
        }
    }
    static applyPop(doc, fields) {
        for (const [field, direction] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field);
            if (Array.isArray(array) && array.length > 0) {
                const newArray = [...array];
                if (direction === 1) {
                    newArray.pop(); // Remove last
                }
                else {
                    newArray.shift(); // Remove first
                }
                this.setNestedValue(doc, field, newArray);
            }
        }
    }
    static getNestedValue(obj, path) {
        if (!path.includes('.')) {
            return obj?.[path];
        }
        const keys = path.split('.');
        let current = obj;
        for (const key of keys) {
            if (current == null)
                return undefined;
            current = current[key];
        }
        return current;
    }
    static setNestedValue(obj, path, value) {
        const keys = path.split('.');
        const lastKey = keys.pop();
        let current = obj;
        for (const key of keys) {
            if (!(key in current) || typeof current[key] !== 'object' || current[key] === null) {
                current[key] = {};
            }
            current = current[key];
        }
        current[lastKey] = value;
    }
    static unsetNestedValue(obj, path) {
        const keys = path.split('.');
        const lastKey = keys.pop();
        let current = obj;
        for (const key of keys) {
            if (current?.[key] == null)
                return;
            current = current[key];
        }
        if (current && typeof current === 'object') {
            delete current[lastKey];
        }
    }
}
