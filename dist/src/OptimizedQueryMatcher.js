import { ObjectId } from "bson";
import JSONSchemaValidator from "./JSONSchemaValidator";
import { MongoDocumentUtils } from "./MongoDocumentUtils";
import MongoExpressionEvaluator from "./MongoExpressionEvaluator";
export class OptimizedQueryMatcher {
    static operatorCache = new Map();
    static documentMatchesQuery(doc, query) {
        if (!query || typeof query !== 'object')
            return true;
        if (!doc || typeof doc !== 'object')
            return false;
        return this.evaluateConditions(doc, query);
    }
    static evaluateConditions(doc, conditions) {
        for (const [field, value] of Object.entries(conditions)) {
            if (field.startsWith('$')) {
                if (!this.evaluateLogicalOperator(doc, field, value)) {
                    return false;
                }
            }
            else {
                if (!this.evaluateFieldCondition(doc, field, value)) {
                    return false;
                }
            }
        }
        return true;
    }
    static evaluateLogicalOperator(doc, operator, value) {
        switch (operator) {
            case '$or':
                return Array.isArray(value) && value.some(condition => this.documentMatchesQuery(doc, condition));
            case '$and':
                return Array.isArray(value) && value.every(condition => this.documentMatchesQuery(doc, condition));
            case '$nor':
                return Array.isArray(value) && !value.some(condition => this.documentMatchesQuery(doc, condition));
            case '$not':
                return !this.documentMatchesQuery(doc, value);
            case '$where':
                // JavaScript expression evaluation (use with caution)
                try {
                    if (typeof value === 'string') {
                        const func = new Function('obj', `return ${value}`);
                        return func(doc);
                    }
                    else if (typeof value === 'function') {
                        return value.call(doc);
                    }
                }
                catch {
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
    static evaluateFieldCondition(doc, field, value) {
        const docValue = this.getNestedValue(doc, field);
        if (value && typeof value === 'object' && !Array.isArray(value) && !(value instanceof ObjectId) && !(value instanceof Date)) {
            // Handle operators
            return this.evaluateFieldOperators(docValue, value);
        }
        else {
            // Direct comparison
            return this.valuesEqual(docValue, value);
        }
    }
    static evaluateFieldOperators(docValue, operators) {
        for (const [operator, operatorValue] of Object.entries(operators)) {
            if (!this.checkOperator(docValue, operator, operatorValue)) {
                return false;
            }
        }
        return true;
    }
    static checkOperator(docValue, operator, operatorValue) {
        // Use cached operator functions for performance
        const cacheKey = `${operator}:${typeof operatorValue}`;
        let operatorFn = this.operatorCache.get(cacheKey);
        if (!operatorFn) {
            operatorFn = this.createOperatorFunction(operator);
            this.operatorCache.set(cacheKey, operatorFn);
        }
        return operatorFn(docValue, operatorValue);
    }
    static createOperatorFunction(operator) {
        switch (operator) {
            // Comparison Operators
            case '$eq': return (doc, val) => this.valuesEqual(doc, val);
            case '$ne': return (doc, val) => !this.valuesEqual(doc, val);
            case '$gt': return (doc, val) => doc != null && doc > val;
            case '$gte': return (doc, val) => doc != null && doc >= val;
            case '$lt': return (doc, val) => doc != null && doc < val;
            case '$lte': return (doc, val) => doc != null && doc <= val;
            case '$in': return (doc, val) => Array.isArray(val) && val.some(v => this.valuesEqual(doc, v));
            case '$nin': return (doc, val) => !Array.isArray(val) || !val.some(v => this.valuesEqual(doc, v));
            // Element Operators
            case '$exists': return (doc, val) => val ? (doc !== undefined) : (doc === undefined);
            case '$type': return (doc, val) => this.checkType(doc, val);
            // Evaluation Operators
            case '$regex': return (doc, val) => {
                try {
                    const regex = val instanceof RegExp ? val : new RegExp(val);
                    return typeof doc === 'string' && regex.test(doc);
                }
                catch {
                    return false;
                }
            };
            case '$options': return () => true; // Used with $regex, handled in regex logic
            case '$mod': return (doc, val) => {
                if (!Array.isArray(val) || val.length !== 2)
                    return false;
                return typeof doc === 'number' && doc % val[0] === val[1];
            };
            // Array Operators
            case '$all': return (doc, val) => {
                if (!Array.isArray(doc) || !Array.isArray(val))
                    return false;
                return val.every(v => doc.some(d => this.valuesEqual(d, v)));
            };
            case '$elemMatch': return (doc, val) => {
                if (!Array.isArray(doc))
                    return false;
                return doc.some(item => this.documentMatchesQuery(item, val));
            };
            case '$size': return (doc, val) => Array.isArray(doc) && doc.length === val;
            // Bitwise Operators
            case '$bitsAllClear': return (doc, val) => this.checkBits(doc, val, 'allClear');
            case '$bitsAllSet': return (doc, val) => this.checkBits(doc, val, 'allSet');
            case '$bitsAnyClear': return (doc, val) => this.checkBits(doc, val, 'anyClear');
            case '$bitsAnySet': return (doc, val) => this.checkBits(doc, val, 'anySet');
            // Geospatial Operators (simplified implementations)
            case '$geoIntersects': return (doc, val) => this.checkGeoIntersects(doc, val);
            case '$geoWithin': return (doc, val) => this.checkGeoWithin(doc, val);
            case '$near': return (doc, val) => this.checkNear(doc, val);
            case '$nearSphere': return (doc, val) => this.checkNearSphere(doc, val);
            case '$maxDistance': return () => true; // Used with $near, handled in near logic
            case '$minDistance': return () => true; // Used with $near, handled in near logic
            // Additional String Operators
            case '$strcasecmp': return (doc, val) => {
                if (typeof doc !== 'string' || typeof val !== 'string')
                    return false;
                return doc.toLowerCase().localeCompare(val.toLowerCase()) === 0;
            };
            // Array Position Operators
            case '$slice': return () => true; // Projection operator, not query operator
            case '$': return () => true; // Positional operator, not query operator
            case '$[]': return () => true; // All positional operator, not query operator
            // Miscellaneous
            case '$rand': return () => Math.random() < 0.5; // Random selection
            case '$sampleRate': return (doc, val) => Math.random() < val;
            default: return () => true;
        }
    }
    // Type checking helper
    static checkType(value, typeSpec) {
        const getJSType = (val) => {
            if (val === null)
                return 'null';
            if (Array.isArray(val))
                return 'array';
            if (val instanceof Date)
                return 'date';
            if (val instanceof ObjectId)
                return 'objectId';
            if (val instanceof RegExp)
                return 'regex';
            if (typeof val === 'object')
                return 'object';
            return typeof val;
        };
        const actualType = getJSType(value);
        if (typeof typeSpec === 'string') {
            return actualType === typeSpec;
        }
        else if (typeof typeSpec === 'number') {
            // BSON type numbers
            const typeMap = {
                1: 'number', 2: 'string', 3: 'object', 4: 'array',
                5: 'binData', 6: 'undefined', 7: 'objectId', 8: 'boolean',
                9: 'date', 10: 'null', 11: 'regex', 13: 'javascript',
                16: 'int', 18: 'long', 19: 'decimal'
            };
            return actualType === typeMap[typeSpec];
        }
        else if (Array.isArray(typeSpec)) {
            return typeSpec.some(t => this.checkType(value, t));
        }
        return false;
    }
    // Bitwise operation helpers
    static checkBits(value, mask, operation) {
        if (typeof value !== 'number' || typeof mask !== 'number')
            return false;
        switch (operation) {
            case 'allClear': return (value & mask) === 0;
            case 'allSet': return (value & mask) === mask;
            case 'anyClear': return (value & mask) !== mask;
            case 'anySet': return (value & mask) !== 0;
            default: return false;
        }
    }
    // Simplified geospatial helpers (basic implementations)
    static checkGeoIntersects(docValue, query) {
        // Simplified implementation - in real scenarios, you'd use proper geospatial libraries
        if (!docValue || !docValue.coordinates)
            return false;
        // This would need proper geometric intersection logic
        return true;
    }
    static checkGeoWithin(docValue, query) {
        // Simplified implementation
        if (!docValue || !docValue.coordinates)
            return false;
        // This would need proper geometric containment logic
        return true;
    }
    static checkNear(docValue, query) {
        // Simplified implementation
        if (!docValue || !docValue.coordinates || !query.coordinates)
            return false;
        const [x1, y1] = docValue.coordinates;
        const [x2, y2] = query.coordinates;
        const distance = Math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2);
        if (query.maxDistance !== undefined && distance > query.maxDistance)
            return false;
        if (query.minDistance !== undefined && distance < query.minDistance)
            return false;
        return true;
    }
    static checkNearSphere(docValue, query) {
        // For sphere calculations, you'd use haversine formula or similar
        return this.checkNear(docValue, query);
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
    static valuesEqual(a, b) {
        if (a === b)
            return true;
        if (a == null || b == null)
            return a === b;
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
            if (a.length !== b.length)
                return false;
            return a.every((item, index) => this.valuesEqual(item, b[index]));
        }
        return false;
    }
}
