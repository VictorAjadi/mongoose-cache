import { MongoDocumentUtils } from "./MongoDocumentUtils";
import { OptimizedQueryMatcher } from "./OptimizedQueryMatcher";

// Optimized update operations
export class UpdateOperations {
    static applyUpdateToDocument(doc: any, updateData: any): any {
        if (!updateData || typeof updateData !== 'object') return doc;

        const updated = MongoDocumentUtils.deepCloneDocument(doc);

        for (const [operator, fields] of Object.entries(updateData)) {
            this.applyOperator(updated, operator, fields);
        }

        return MongoDocumentUtils.ensureMongoDocument(updated);
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
            default:
                if (!operator.startsWith('$')) {
                    // Direct field assignment
                    this.setNestedValue(doc, operator, fields);
                }
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
                const newArray = [...array];
                if (typeof value === 'object' && value !== null && '$each' in value) {
                    // Handle $push with $each
                    const items = Array.isArray(value.$each) ? value.$each : [value.$each];
                    newArray.push(...items);
                } else {
                    newArray.push(value);
                }
                this.setNestedValue(doc, field, newArray);
            }
        }
    }

    private static applyPull(doc: any, fields: any): void {
        for (const [field, condition] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field);
            if (Array.isArray(array)) {
                const newArray = array.filter(item => 
                    !OptimizedQueryMatcher.documentMatchesQuery(item, condition)
                );
                this.setNestedValue(doc, field, newArray);
            }
        }
    }

    private static applyAddToSet(doc: any, fields: any): void {
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

    private static applyPop(doc: any, fields: any): void {
        for (const [field, direction] of Object.entries(fields)) {
            const array = this.getNestedValue(doc, field);
            if (Array.isArray(array) && array.length > 0) {
                const newArray = [...array];
                if (direction === 1) {
                    newArray.pop(); // Remove last
                } else {
                    newArray.shift(); // Remove first
                }
                this.setNestedValue(doc, field, newArray);
            }
        }
    }

    private static getNestedValue(obj: any, path: string): any {
        if (!path.includes('.')) {
            return obj?.[path];
        }
        
        const keys = path.split('.');
        let current = obj;
        
        for (const key of keys) {
            if (current == null) return undefined;
            current = current[key];
        }
        
        return current;
    }

    private static setNestedValue(obj: any, path: string, value: any): void {
        const keys = path.split('.');
        const lastKey = keys.pop()!;
        let current = obj;

        for (const key of keys) {
            if (!(key in current) || typeof current[key] !== 'object' || current[key] === null) {
                current[key] = {};
            }
            current = current[key];
        }

        current[lastKey] = value;
    }

    private static unsetNestedValue(obj: any, path: string): void {
        const keys = path.split('.');
        const lastKey = keys.pop()!;
        let current = obj;

        for (const key of keys) {
            if (current?.[key] == null) return;
            current = current[key];
        }

        if (current && typeof current === 'object') {
            delete current[lastKey];
        }
    }
}