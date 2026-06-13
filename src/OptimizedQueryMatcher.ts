import { ObjectId } from "bson";
import JSONSchemaValidator from "./JSONSchemaValidator";
import { MongoDocumentUtils } from "./MongoDocumentUtils";
import MongoExpressionEvaluator from "./MongoExpressionEvaluator";
import * as turf from '@turf/turf'
export class OptimizedQueryMatcher {
    private static operatorCache = new Map<string, Function>();

    static documentMatchesQuery(doc: any, query: any): boolean {
        if (!query || typeof query !== 'object') return true;
        if (!doc || typeof doc !== 'object') return false;

        return this.evaluateConditions(doc, query);
    }

    private static readonly PATH_CACHE = new Map<string, string[]>();

    private static evaluateConditions(doc: any, conditions: any): boolean {
        // High-speed loop using for...in to avoid entry allocations
        for (const field in conditions) {
            const value = conditions[field];
            // charCode 36 is '$'
            if (field.charCodeAt(0) === 36) {
                if (!this.evaluateLogicalOperator(doc, field, value)) {
                    return false;
                }
            } else {
                if (!this.evaluateFieldCondition(doc, field, value)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static evaluateLogicalOperator(doc: any, operator: string, value: any): boolean {
        switch (operator) {
            case '$or':
                return Array.isArray(value) && value.some(condition =>
                    this.documentMatchesQuery(doc, condition)
                );
            case '$and':
                return Array.isArray(value) && value.every(condition =>
                    this.documentMatchesQuery(doc, condition)
                );
            case '$nor':
                return Array.isArray(value) && !value.some(condition =>
                    this.documentMatchesQuery(doc, condition)
                );
            case '$not':
                return !this.documentMatchesQuery(doc, value);
            case '$where':
                // JavaScript expression evaluation (use with caution)
                try {
                    if (typeof value === 'string') {
                        const func = new Function('obj', `return ${value}`);
                        return func(doc);
                    } else if (typeof value === 'function') {
                        return value.call(doc);
                    }
                } catch {
                    return false;
                }
                return false;
            case '$expr':
                // Expression operator for aggregation expressions
                return MongoExpressionEvaluator.evaluateExpression(doc, value);
            case '$jsonSchema':
                // JSON Schema validation
                return JSONSchemaValidator.validateJsonSchema(doc, value);
            case '$text':
                // Text search (simplified implementation)
                return MongoExpressionEvaluator.evaluateExpression(doc, value);
            case '$comment':
                // Comment operator - always returns true
                return true;
            default:
                return true;
        }
    }

    private static evaluateFieldCondition(doc: any, field: string, value: any): boolean {
        const docValue = this.getNestedValue(doc, field);

        if (value && typeof value === 'object' && !Array.isArray(value) && !(value instanceof ObjectId) && !(value instanceof Date)) {
            // Handle operators
            return this.evaluateFieldOperators(docValue, value);
        } else {
            // Direct comparison
            return this.valuesEqual(docValue, value);
        }
    }

    private static evaluateFieldOperators(docValue: any, operators: any): boolean {
        for (const operator in operators) {
            if (!this.checkOperator(docValue, operator, operators[operator])) {
                return false;
            }
        }
        return true;
    }

    private static checkOperator(docValue: any, operator: string, operatorValue: any): boolean {
        // Use cached operator functions for performance
        const cacheKey = `${operator}:${typeof operatorValue}`;
        let operatorFn = this.operatorCache.get(cacheKey);

        if (!operatorFn) {
            operatorFn = this.createOperatorFunction(operator);
            this.operatorCache.set(cacheKey, operatorFn);
        }

        return operatorFn(docValue, operatorValue);
    }

    private static createOperatorFunction(operator: string): Function {
        switch (operator) {
            // Comparison Operators
            case '$eq': return (doc: any, val: any) => this.valuesEqual(doc, val);
            case '$ne': return (doc: any, val: any) => !this.valuesEqual(doc, val);
            case '$gt': return (doc: any, val: any) => doc != null && doc > val;
            case '$gte': return (doc: any, val: any) => doc != null && doc >= val;
            case '$lt': return (doc: any, val: any) => doc != null && doc < val;
            case '$lte': return (doc: any, val: any) => doc != null && doc <= val;
            case '$in': return (doc: any, val: any) => Array.isArray(val) && val.some(v => this.valuesEqual(doc, v));
            case '$nin': return (doc: any, val: any) => !Array.isArray(val) || !val.some(v => this.valuesEqual(doc, v));

            // Element Operators
            case '$exists': return (doc: any, val: any) => val ? (doc !== undefined) : (doc === undefined);
            case '$type': return (doc: any, val: any) => this.checkType(doc, val);

            // Evaluation Operators
            case '$regex': return (doc: any, val: any) => {
                try {
                    const regex = val instanceof RegExp ? val : new RegExp(val);
                    return typeof doc === 'string' && regex.test(doc);
                } catch { return false; }
            };
            case '$options': return () => true; // Used with $regex, handled in regex logic
            case '$mod': return (doc: any, val: any) => {
                if (!Array.isArray(val) || val.length !== 2) return false;
                return typeof doc === 'number' && doc % val[0] === val[1];
            };

            // Array Operators
            case '$all': return (doc: any, val: any) => {
                if (!Array.isArray(doc) || !Array.isArray(val)) return false;
                return val.every(v => doc.some(d => this.valuesEqual(d, v)));
            };
            case '$elemMatch': return (doc: any, val: any) => {
                if (!Array.isArray(doc)) return false;
                return doc.some(item => this.documentMatchesQuery(item, val));
            };
            case '$size': return (doc: any, val: any) => Array.isArray(doc) && doc.length === val;

            // Bitwise Operators
            case '$bitsAllClear': return (doc: any, val: any) => this.checkBits(doc, val, 'allClear');
            case '$bitsAllSet': return (doc: any, val: any) => this.checkBits(doc, val, 'allSet');
            case '$bitsAnyClear': return (doc: any, val: any) => this.checkBits(doc, val, 'anyClear');
            case '$bitsAnySet': return (doc: any, val: any) => this.checkBits(doc, val, 'anySet');

            // Geospatial Operators (simplified implementations)
            case '$geoIntersects': return (doc: any, val: any) => this.checkGeoIntersects(doc, val);
            case '$geoWithin': return (doc: any, val: any) => this.checkGeoWithin(doc, val);
            case '$near': return (doc: any, val: any) => this.checkNear(doc, val);
            case '$nearSphere': return (doc: any, val: any) => this.checkNearSphere(doc, val);
            case '$maxDistance': return () => true; // Used with $near, handled in near logic
            case '$minDistance': return () => true; // Used with $near, handled in near logic

            // Additional String Operators
            case '$strcasecmp': return (doc: any, val: any) => {
                if (typeof doc !== 'string' || typeof val !== 'string') return false;
                return doc.toLowerCase().localeCompare(val.toLowerCase()) === 0;
            };

            // Array Position Operators
            case '$slice': return () => true; // Projection operator, not query operator
            case '$': return () => true; // Positional operator, not query operator
            case '$[]': return () => true; // All positional operator, not query operator

            // Miscellaneous
            case '$rand': return () => Math.random() < 0.5; // Random selection
            case '$sampleRate': return (_doc: any, val: any) => Math.random() < val;

            default: return () => true;
        }
    }

    // Type checking helper
    private static checkType(value: any, typeSpec: any): boolean {
        const getJSType = (val: any): string => {
            if (val === null) return 'null';
            if (Array.isArray(val)) return 'array';
            if (val instanceof Date) return 'date';
            if (val instanceof ObjectId) return 'objectId';
            if (val instanceof RegExp) return 'regex';
            if (typeof val === 'object') return 'object';
            return typeof val;
        };

        const actualType = getJSType(value);

        if (typeof typeSpec === 'string') {
            return actualType === typeSpec;
        } else if (typeof typeSpec === 'number') {
            // BSON type numbers
            const typeMap: { [key: number]: string } = {
                1: 'number', 2: 'string', 3: 'object', 4: 'array',
                5: 'binData', 6: 'undefined', 7: 'objectId', 8: 'boolean',
                9: 'date', 10: 'null', 11: 'regex', 13: 'javascript',
                16: 'int', 18: 'long', 19: 'decimal'
            };
            return actualType === typeMap[typeSpec];
        } else if (Array.isArray(typeSpec)) {
            return typeSpec.some(t => this.checkType(value, t));
        }

        return false;
    }

    // Bitwise operation helpers
    private static checkBits(value: any, mask: any, operation: string): boolean {
        if (typeof value !== 'number' || typeof mask !== 'number') return false;

        switch (operation) {
            case 'allClear': return (value & mask) === 0;
            case 'allSet': return (value & mask) === mask;
            case 'anyClear': return (value & mask) !== mask;
            case 'anySet': return (value & mask) !== 0;
            default: return false;
        }
    }

    // Simplified geospatial helpers (basic implementations)
    private static checkGeoIntersects(docValue: any, _query: any): boolean {
        // Simplified implementation - in real scenarios, you'd use proper geospatial libraries
        if (!docValue || !docValue.coordinates) return false;
        // This would need proper geometric intersection logic
        return true;
    }

    private static checkGeoWithin(docValue: any, query: any): boolean {
        try {
            const docPoint = turf.point([docValue.coordinates[1], docValue.coordinates[0]]);

            // query.geometry should be GeoJSON polygon or similar
            if (!query.geometry) return false;

            const polygon = turf.polygon(query.geometry.coordinates);
            return turf.booleanPointInPolygon(docPoint, polygon);
        } catch (error) {
            if (typeof (globalThis as any).process !== 'undefined') {
                console.error('[QueryMatcher] Geospatial calculation failed:', error);
            }
            return false;
        }
    }

    private static checkNear(docValue: any, query: any): boolean {
        // Simplified implementation
        if (!docValue || !docValue.coordinates || !query.coordinates) return false;

        const [x1, y1] = docValue.coordinates;
        const [x2, y2] = query.coordinates;
        const distance = Math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2);

        if (query.maxDistance !== undefined && distance > query.maxDistance) return false;
        if (query.minDistance !== undefined && distance < query.minDistance) return false;

        return true;
    }

    private static checkNearSphere(docValue: any, query: any): boolean {
        // For sphere calculations, you'd use haversine formula or similar
        return this.checkNear(docValue, query);
    }
    private static getNestedValue(obj: any, path: string): any {
        if (obj == null) return undefined;
        if (!path.includes('.')) return obj[path];

        let keys = this.PATH_CACHE.get(path);
        if (!keys) {
            keys = path.split('.');
            if (this.PATH_CACHE.size < 2000) this.PATH_CACHE.set(path, keys);
        }

        let current = obj;
        for (let i = 0; i < keys.length; i++) {
            if (current == null) return undefined;
            current = current[keys[i]];
        }

        return current;
    }
    private static valuesEqual(a: any, b: any): boolean {
        if (a === b) return true;
        if (a == null || b == null) return a === b;

        // Handle ObjectId comparison
        if (a instanceof ObjectId || b instanceof ObjectId ||
            MongoDocumentUtils.isValidObjectId(a) || MongoDocumentUtils.isValidObjectId(b)) {
            return MongoDocumentUtils.compareIds(a, b);
        }

        // Handle Date comparison
        if (a instanceof Date && b instanceof Date) {
            return a.getTime() === b.getTime();
        }

        // Handle array comparison
        if (Array.isArray(a) && Array.isArray(b)) {
            if (a.length !== b.length) return false;
            return a.every((item, index) => this.valuesEqual(item, b[index]));
        }

        return false;
    }
}