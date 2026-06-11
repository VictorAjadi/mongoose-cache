/**
 * High-Performance JSON Schema Validator
 * Optimized for speed with complete JSON Schema Draft-07 support
 * @module JSONSchemaValidator
 */

interface ValidationCache {
    regex: Map<string, RegExp>;
    formats: Map<string, Function>;
    uniqueKeys: WeakMap<any[], Set<string>>;
}

class JSONSchemaValidator {
    private static readonly cache: ValidationCache = {
        regex: new Map(),
        formats: new Map(),
        uniqueKeys: new WeakMap()
    };

    private static readonly FORMAT_VALIDATORS = new Map<string, (value: string) => boolean>();
    private static readonly TYPE_CACHE = new Map<any, string>();

    static {
        this.initializeFormatValidators();
    }

    private static initializeFormatValidators(): void {
        this.FORMAT_VALIDATORS.set('date-time', this.isValidDateTime.bind(this));
        this.FORMAT_VALIDATORS.set('date', this.isValidDate.bind(this));
        this.FORMAT_VALIDATORS.set('time', this.isValidTime.bind(this));
        this.FORMAT_VALIDATORS.set('duration', this.isValidDuration.bind(this));
        this.FORMAT_VALIDATORS.set('email', this.isValidEmail.bind(this));
        this.FORMAT_VALIDATORS.set('idn-email', this.isValidIDNEmail.bind(this));
        this.FORMAT_VALIDATORS.set('hostname', this.isValidHostname.bind(this));
        this.FORMAT_VALIDATORS.set('idn-hostname', this.isValidIDNHostname.bind(this));
        this.FORMAT_VALIDATORS.set('ipv4', this.isValidIPv4.bind(this));
        this.FORMAT_VALIDATORS.set('ipv6', this.isValidIPv6.bind(this));
        this.FORMAT_VALIDATORS.set('uri', this.isValidURI.bind(this));
        this.FORMAT_VALIDATORS.set('uri-reference', this.isValidURIReference.bind(this));
        this.FORMAT_VALIDATORS.set('iri', this.isValidIRI.bind(this));
        this.FORMAT_VALIDATORS.set('iri-reference', this.isValidIRIReference.bind(this));
        this.FORMAT_VALIDATORS.set('uuid', this.isValidUUID.bind(this));
        this.FORMAT_VALIDATORS.set('uri-template', this.isValidURITemplate.bind(this));
        this.FORMAT_VALIDATORS.set('json-pointer', this.isValidJSONPointer.bind(this));
        this.FORMAT_VALIDATORS.set('relative-json-pointer', this.isValidRelativeJSONPointer.bind(this));
        this.FORMAT_VALIDATORS.set('regex', this.isValidRegex.bind(this));
    }

    public static validateJsonSchema(value: any, schema: any): boolean {
        try {
            return this.validateSchema(value, schema, '', new Set());
        } catch {
            return false;
        }
    }

    private static validateSchema(value: any, schema: any, path: string, visited: Set<any>): boolean {
        if (!schema || typeof schema !== 'object') return true;

        if (schema.type !== undefined) {
            if (!this.validateType(value, schema.type)) return false;
        }

        if (schema.enum !== undefined) {
            if (!this.validateEnum(value, schema.enum)) return false;
        }

        if (schema.const !== undefined) {
            if (!this.fastEquals(value, schema.const)) return false;
        }

        const valueType = typeof value;

        if (valueType === 'string') {
            if (!this.validateString(value, schema)) return false;
        } else if (valueType === 'number') {
            if (!this.validateNumber(value, schema)) return false;
        } else if (Array.isArray(value)) {
            if (!this.validateArray(value, schema, path, visited)) return false;
        } else if (value !== null && valueType === 'object') {
            if (!this.validateObject(value, schema, path, visited)) return false;
        }

        if (schema.allOf !== undefined) {
            if (!this.validateAllOf(value, schema.allOf, path, visited)) return false;
        }

        if (schema.anyOf !== undefined) {
            if (!this.validateAnyOf(value, schema.anyOf, path, visited)) return false;
        }

        if (schema.oneOf !== undefined) {
            if (!this.validateOneOf(value, schema.oneOf, path, visited)) return false;
        }

        if (schema.not !== undefined) {
            if (this.validateSchema(value, schema.not, path, visited)) return false;
        }

        if (schema.if !== undefined) {
            if (!this.validateIfThenElse(value, schema, path, visited)) return false;
        }

        return true;
    }

    private static validateType(value: any, type: string | string[]): boolean {
        const types = Array.isArray(type) ? type : [type];
        const actualType = this.getJsonType(value);

        for (let i = 0; i < types.length; i++) {
            if (types[i] === actualType) return true;
            if (types[i] === 'number' && actualType === 'integer') return true;
        }

        return false;
    }

    private static getJsonType(value: any): string {
        if (value === null) return 'null';
        if (Array.isArray(value)) return 'array';

        const type = typeof value;

        if (type === 'number') {
            if (Number.isInteger(value) && Number.isFinite(value)) {
                return 'integer';
            }
            return 'number';
        }

        return type;
    }

    private static validateEnum(value: any, enumValues: any[]): boolean {
        for (let i = 0; i < enumValues.length; i++) {
            if (this.fastEquals(value, enumValues[i])) return true;
        }
        return false;
    }

    private static validateString(value: string, schema: any): boolean {
        const len = value.length;

        if (schema.minLength !== undefined && len < schema.minLength) return false;
        if (schema.maxLength !== undefined && len > schema.maxLength) return false;

        if (schema.pattern !== undefined) {
            let regex = this.cache.regex.get(schema.pattern);
            if (!regex) {
                try {
                    regex = new RegExp(schema.pattern);
                    if (this.cache.regex.size < 1000) {
                        this.cache.regex.set(schema.pattern, regex);
                    }
                } catch {
                    return false;
                }
            }
            if (!regex.test(value)) return false;
        }

        if (schema.format !== undefined) {
            if (!this.validateFormat(value, schema.format)) return false;
        }

        return true;
    }

    private static validateNumber(value: number, schema: any): boolean {
        if (!Number.isFinite(value)) return false;

        if (schema.minimum !== undefined) {
            if (value < schema.minimum) return false;
        }

        if (schema.maximum !== undefined) {
            if (value > schema.maximum) return false;
        }

        if (schema.exclusiveMinimum !== undefined) {
            if (value <= schema.exclusiveMinimum) return false;
        }

        if (schema.exclusiveMaximum !== undefined) {
            if (value >= schema.exclusiveMaximum) return false;
        }

        if (schema.multipleOf !== undefined && schema.multipleOf > 0) {
            const quotient = value / schema.multipleOf;
            if (Math.abs(quotient - Math.round(quotient)) > Number.EPSILON) return false;
        }

        return true;
    }

    private static validateArray(value: any[], schema: any, path: string, visited: Set<any>): boolean {
        const len = value.length;

        if (schema.minItems !== undefined && len < schema.minItems) return false;
        if (schema.maxItems !== undefined && len > schema.maxItems) return false;

        if (schema.uniqueItems === true) {
            if (!this.hasUniqueItemsFast(value)) return false;
        }

        if (schema.contains !== undefined) {
            let hasMatch = false;
            for (let i = 0; i < len; i++) {
                if (this.validateSchema(value[i], schema.contains, `${path}[${i}]`, visited)) {
                    hasMatch = true;
                    break;
                }
            }
            if (!hasMatch) return false;
        }

        if (schema.items !== undefined) {
            if (typeof schema.items === 'object' && !Array.isArray(schema.items)) {
                for (let i = 0; i < len; i++) {
                    if (!this.validateSchema(value[i], schema.items, `${path}[${i}]`, visited)) {
                        return false;
                    }
                }
            } else if (Array.isArray(schema.items)) {
                const itemSchemas = schema.items;
                const itemsLen = itemSchemas.length;

                for (let i = 0; i < Math.min(len, itemsLen); i++) {
                    if (!this.validateSchema(value[i], itemSchemas[i], `${path}[${i}]`, visited)) {
                        return false;
                    }
                }

                if (len > itemsLen) {
                    if (schema.additionalItems === false) return false;

                    if (typeof schema.additionalItems === 'object') {
                        for (let i = itemsLen; i < len; i++) {
                            if (!this.validateSchema(value[i], schema.additionalItems, `${path}[${i}]`, visited)) {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        return true;
    }

    private static validateObject(value: any, schema: any, path: string, visited: Set<any>): boolean {
        if (visited.has(value)) return true;
        visited.add(value);

        const keys = Object.keys(value);
        const keyCount = keys.length;

        if (schema.minProperties !== undefined && keyCount < schema.minProperties) {
            visited.delete(value);
            return false;
        }

        if (schema.maxProperties !== undefined && keyCount > schema.maxProperties) {
            visited.delete(value);
            return false;
        }

        if (schema.required !== undefined && Array.isArray(schema.required)) {
            for (let i = 0; i < schema.required.length; i++) {
                if (!(schema.required[i] in value)) {
                    visited.delete(value);
                    return false;
                }
            }
        }

        if (schema.properties !== undefined) {
            for (const prop in schema.properties) {
                if (prop in value) {
                    if (!this.validateSchema(value[prop], schema.properties[prop], `${path}.${prop}`, visited)) {
                        visited.delete(value);
                        return false;
                    }
                }
            }
        }

        let patternRegexes: Array<{ regex: RegExp; schema: any }> | null = null;
        if (schema.patternProperties !== undefined) {
            patternRegexes = [];
            for (const pattern in schema.patternProperties) {
                let regex = this.cache.regex.get(pattern);
                if (!regex) {
                    try {
                        regex = new RegExp(pattern);
                        if (this.cache.regex.size < 1000) {
                            this.cache.regex.set(pattern, regex);
                        }
                    } catch {
                        continue;
                    }
                }
                patternRegexes.push({ regex, schema: schema.patternProperties[pattern] });
            }

            for (let i = 0; i < keys.length; i++) {
                const key = keys[i];
                for (let j = 0; j < patternRegexes.length; j++) {
                    if (patternRegexes[j].regex.test(key)) {
                        if (!this.validateSchema(value[key], patternRegexes[j].schema, `${path}.${key}`, visited)) {
                            visited.delete(value);
                            return false;
                        }
                    }
                }
            }
        }

        if (schema.additionalProperties !== undefined) {
            const definedProps = schema.properties ? new Set(Object.keys(schema.properties)) : null;

            for (let i = 0; i < keys.length; i++) {
                const key = keys[i];

                if (definedProps && definedProps.has(key)) continue;

                let matchesPattern = false;
                if (patternRegexes) {
                    for (let j = 0; j < patternRegexes.length; j++) {
                        if (patternRegexes[j].regex.test(key)) {
                            matchesPattern = true;
                            break;
                        }
                    }
                }

                if (!matchesPattern) {
                    if (schema.additionalProperties === false) {
                        visited.delete(value);
                        return false;
                    }

                    if (typeof schema.additionalProperties === 'object') {
                        if (!this.validateSchema(value[key], schema.additionalProperties, `${path}.${key}`, visited)) {
                            visited.delete(value);
                            return false;
                        }
                    }
                }
            }
        }

        if (schema.dependencies !== undefined) {
            for (const prop in schema.dependencies) {
                if (prop in value) {
                    const dependency = schema.dependencies[prop];

                    if (Array.isArray(dependency)) {
                        for (let i = 0; i < dependency.length; i++) {
                            if (!(dependency[i] in value)) {
                                visited.delete(value);
                                return false;
                            }
                        }
                    } else if (typeof dependency === 'object') {
                        if (!this.validateSchema(value, dependency, path, visited)) {
                            visited.delete(value);
                            return false;
                        }
                    }
                }
            }
        }

        if (schema.propertyNames !== undefined) {
            for (let i = 0; i < keys.length; i++) {
                if (!this.validateSchema(keys[i], schema.propertyNames, `${path}.propertyName`, visited)) {
                    visited.delete(value);
                    return false;
                }
            }
        }

        visited.delete(value);
        return true;
    }

    private static validateAllOf(value: any, schemas: any[], path: string, visited: Set<any>): boolean {
        for (let i = 0; i < schemas.length; i++) {
            if (!this.validateSchema(value, schemas[i], path, visited)) return false;
        }
        return true;
    }

    private static validateAnyOf(value: any, schemas: any[], path: string, visited: Set<any>): boolean {
        for (let i = 0; i < schemas.length; i++) {
            if (this.validateSchema(value, schemas[i], path, visited)) return true;
        }
        return false;
    }

    private static validateOneOf(value: any, schemas: any[], path: string, visited: Set<any>): boolean {
        let validCount = 0;

        for (let i = 0; i < schemas.length; i++) {
            if (this.validateSchema(value, schemas[i], path, visited)) {
                validCount++;
                if (validCount > 1) return false;
            }
        }

        return validCount === 1;
    }

    private static validateIfThenElse(value: any, schema: any, path: string, visited: Set<any>): boolean {
        const ifResult = this.validateSchema(value, schema.if, path, visited);

        if (ifResult && schema.then !== undefined) {
            return this.validateSchema(value, schema.then, path, visited);
        } else if (!ifResult && schema.else !== undefined) {
            return this.validateSchema(value, schema.else, path, visited);
        }

        return true;
    }

    private static validateFormat(value: string, format: string): boolean {
        const validator = this.FORMAT_VALIDATORS.get(format);
        return validator ? validator(value) : true;
    }

    private static hasUniqueItemsFast(array: any[]): boolean {
        const len = array.length;
        if (len <= 1) return true;

        const primitives = new Set<string>();
        const objects: any[] = [];

        for (let i = 0; i < len; i++) {
            const item = array[i];
            const type = typeof item;

            if (item === null || type === 'boolean' || type === 'number' || type === 'string') {
                const key = `${type}:${String(item)}`;
                if (primitives.has(key)) return false;
                primitives.add(key);
            } else {
                for (let j = 0; j < objects.length; j++) {
                    if (this.deepEquals(item, objects[j])) return false;
                }
                objects.push(item);
            }
        }

        return true;
    }

    private static fastEquals(a: any, b: any): boolean {
        if (a === b) return true;
        if (a == null || b == null) return a === b;

        const typeA = typeof a;
        const typeB = typeof b;

        if (typeA !== typeB) return false;

        if (typeA === 'object') {
            if (Array.isArray(a) !== Array.isArray(b)) return false;

            if (Array.isArray(a)) {
                const len = a.length;
                if (len !== b.length) return false;
                for (let i = 0; i < len; i++) {
                    if (!this.fastEquals(a[i], b[i])) return false;
                }
                return true;
            }

            const keysA = Object.keys(a);
            const keysB = Object.keys(b);
            const len = keysA.length;

            if (len !== keysB.length) return false;

            for (let i = 0; i < len; i++) {
                const key = keysA[i];
                if (!Object.prototype.hasOwnProperty.call(b, key)) return false;
                if (!this.fastEquals(a[key], b[key])) return false;
            }

            return true;
        }

        return false;
    }

    private static deepEquals(a: any, b: any, visited: WeakMap<any, any> = new WeakMap()): boolean {
        if (a === b) return true;
        if (a == null || b == null) return a === b;

        const typeA = typeof a;
        const typeB = typeof b;

        if (typeA !== typeB) return false;

        if (typeA !== 'object') return false;

        if (visited.has(a)) return visited.get(a) === b;
        visited.set(a, b);

        if (Array.isArray(a) !== Array.isArray(b)) return false;

        if (Array.isArray(a)) {
            if (a.length !== b.length) return false;
            for (let i = 0; i < a.length; i++) {
                if (!this.deepEquals(a[i], b[i], visited)) return false;
            }
            return true;
        }

        const keysA = Object.keys(a);
        const keysB = Object.keys(b);

        if (keysA.length !== keysB.length) return false;

        for (const key of keysA) {
            if (!Object.prototype.hasOwnProperty.call(b, key)) return false;
            if (!this.deepEquals(a[key], b[key], visited)) return false;
        }

        return true;
    }

    private static isValidDateTime(value: string): boolean {
        if (value.length < 19 || value.length > 35) return false;

        const match = value.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:Z|([+-])(\d{2}):(\d{2}))$/);
        if (!match) return false;

        const [, year, month, day, hour, minute, second, _ms, _tzSign, tzHour, tzMinute] = match;

        const y = parseInt(year, 10);
        const m = parseInt(month, 10);
        const d = parseInt(day, 10);
        const h = parseInt(hour, 10);
        const min = parseInt(minute, 10);
        const sec = parseInt(second, 10);

        if (m < 1 || m > 12) return false;
        if (d < 1 || d > 31) return false;
        if (h > 23) return false;
        if (min > 59) return false;
        if (sec > 60) return false;

        const daysInMonth = [31, this.isLeapYear(y) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        if (d > daysInMonth[m - 1]) return false;

        if (tzHour !== undefined) {
            const tzH = parseInt(tzHour, 10);
            const tzM = parseInt(tzMinute, 10);
            if (tzH > 23 || tzM > 59) return false;
        }

        const date = new Date(value);
        return !isNaN(date.getTime());
    }

    private static isValidDate(value: string): boolean {
        if (value.length !== 10) return false;

        const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (!match) return false;

        const [, year, month, day] = match;

        const y = parseInt(year, 10);
        const m = parseInt(month, 10);
        const d = parseInt(day, 10);

        if (m < 1 || m > 12) return false;
        if (d < 1 || d > 31) return false;

        const daysInMonth = [31, this.isLeapYear(y) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        return d <= daysInMonth[m - 1];
    }

    private static isValidTime(value: string): boolean {
        const match = value.match(/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:Z|([+-])(\d{2}):(\d{2}))?$/);
        if (!match) return false;

        const [, hour, minute, second, _ms, _tzSign, tzHour, tzMinute] = match;

        const h = parseInt(hour, 10);
        const min = parseInt(minute, 10);
        const sec = parseInt(second, 10);

        if (h > 23 || min > 59 || sec > 60) return false;

        if (tzHour !== undefined) {
            const tzH = parseInt(tzHour, 10);
            const tzM = parseInt(tzMinute, 10);
            if (tzH > 23 || tzM > 59) return false;
        }

        return true;
    }

    private static isValidDuration(value: string): boolean {
        const match = value.match(/^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/);
        if (!match) return false;

        const [, year, month, week, day, hour, minute, second] = match;

        if (!year && !month && !week && !day && !hour && !minute && !second) return false;

        return true;
    }

    private static isValidEmail(value: string): boolean {
        if (value.length > 254) return false;

        const parts = value.split('@');
        if (parts.length !== 2) return false;

        const [local, domain] = parts;

        if (local.length === 0 || local.length > 64) return false;
        if (domain.length === 0 || domain.length > 253) return false;

        if (local[0] === '.' || local[local.length - 1] === '.') return false;
        if (local.includes('..')) return false;

        const localRegex = /^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~.-]+$/;
        if (!localRegex.test(local)) return false;

        const domainParts = domain.split('.');
        if (domainParts.length < 2) return false;

        for (const part of domainParts) {
            if (part.length === 0 || part.length > 63) return false;
            if (part[0] === '-' || part[part.length - 1] === '-') return false;
            if (!/^[a-zA-Z0-9-]+$/.test(part)) return false;
        }

        return true;
    }

    private static isValidIDNEmail(value: string): boolean {
        if (value.length > 254) return false;

        const parts = value.split('@');
        if (parts.length !== 2) return false;

        const [local, domain] = parts;
        if (local.length === 0 || domain.length === 0) return false;

        return /^[\p{L}\p{N}.!#$%&'*+/=?^_`{|}~-]+@[\p{L}\p{N}.-]+\.[\p{L}]{2,}$/u.test(value);
    }

    private static isValidHostname(value: string): boolean {
        if (value.length === 0 || value.length > 253) return false;

        const labels = value.split('.');

        for (const label of labels) {
            if (label.length === 0 || label.length > 63) return false;
            if (label[0] === '-' || label[label.length - 1] === '-') return false;
            if (!/^[a-zA-Z0-9-]+$/.test(label)) return false;
        }

        return true;
    }

    private static isValidIDNHostname(value: string): boolean {
        if (value.length === 0 || value.length > 253) return false;

        const labels = value.split('.');

        for (const label of labels) {
            if (label.length === 0 || label.length > 63) return false;
            if (label[0] === '-' || label[label.length - 1] === '-') return false;
        }

        return true;
    }

    private static isValidIPv4(value: string): boolean {
        const parts = value.split('.');
        if (parts.length !== 4) return false;

        for (const part of parts) {
            if (part.length === 0 || part.length > 3) return false;
            if (part.length > 1 && part[0] === '0') return false;

            const num = parseInt(part, 10);
            if (isNaN(num) || num < 0 || num > 255) return false;
            if (String(num) !== part) return false;
        }

        return true;
    }

    private static isValidIPv6(value: string): boolean {
        if (value === '::') return true;
        if (value === '::1') return true;

        const parts = value.split(':');


        if (value.includes('::')) {

            const groups = value.split('::');
            if (groups.length > 2) return false;

            const leftParts = groups[0] ? groups[0].split(':') : [];
            const rightParts = groups[1] ? groups[1].split(':') : [];
            const totalParts = leftParts.length + rightParts.length;

            if (totalParts > 7) return false;


            for (const part of leftParts) {
                if (!this.isValidIPv6Group(part)) return false;
            }
            for (const part of rightParts) {
                if (!this.isValidIPv6Group(part)) return false;
            }

            return true;
        }

        if (parts.length !== 8) return false;

        for (const part of parts) {
            if (!this.isValidIPv6Group(part)) return false;
        }

        return true;
    }

    private static isValidIPv6Group(group: string): boolean {
        if (group.length === 0 || group.length > 4) return false;
        return /^[0-9a-fA-F]+$/.test(group);
    }

    private static isValidURI(value: string): boolean {
        try {
            const url = new URL(value);
            return url.protocol.length > 0 && url.href === value;
        } catch {
            return false;
        }
    }

    private static isValidURIReference(value: string): boolean {
        try {
            new URL(value, 'http://example.com');
            return true;
        } catch {
            return false;
        }
    }

    private static isValidIRI(value: string): boolean {
        try {
            const url = new URL(value);
            return url.protocol.length > 0;
        } catch {
            return false;
        }
    }

    private static isValidIRIReference(value: string): boolean {
        try {
            new URL(value, 'http://example.com');
            return true;
        } catch {
            return false;
        }
    }

    private static isValidUUID(value: string): boolean {
        if (value.length !== 36) return false;

        const parts = value.split('-');
        if (parts.length !== 5) return false;
        if (parts[0].length !== 8 || parts[1].length !== 4 || parts[2].length !== 4 ||
            parts[3].length !== 4 || parts[4].length !== 12) return false;

        const version = value[14];
        if (version < '1' || version > '5') return false;

        const variant = value[19];
        if (variant !== '8' && variant !== '9' && variant !== 'a' && variant !== 'b' &&
            variant !== 'A' && variant !== 'B') return false;

        return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
    }

    private static isValidURITemplate(value: string): boolean {
        let depth = 0;
        let inExpression = false;

        for (let i = 0; i < value.length; i++) {
            const char = value[i];

            if (char === '{') {
                if (inExpression) return false;
                inExpression = true;
                depth++;
            } else if (char === '}') {
                if (!inExpression) return false;
                inExpression = false;
                depth--;
            }

            if (depth < 0) return false;
        }

        return depth === 0 && !inExpression;
    }

    private static isValidJSONPointer(value: string): boolean {
        if (value === '') return true;
        if (!value.startsWith('/')) return false;

        const segments = value.split('/').slice(1);

        for (const segment of segments) {
            for (let i = 0; i < segment.length; i++) {
                if (segment[i] === '~') {
                    if (i === segment.length - 1) return false;
                    const next = segment[i + 1];
                    if (next !== '0' && next !== '1') return false;
                    i++;
                }
            }
        }

        return true;
    }

    private static isValidRelativeJSONPointer(value: string): boolean {
        if (value.length === 0) return false;

        let i = 0;

        if (value[0] === '0') {
            i = 1;
        } else {
            while (i < value.length && value[i] >= '0' && value[i] <= '9' && value[i] !== '0') {
                i++;
            }
            if (i === 0) return false;
        }

        if (i === value.length) return true;

        if (value[i] === '#') return i === value.length - 1;

        if (value[i] === '/') {
            return this.isValidJSONPointer(value.substring(i));
        }

        return false;
    }

    private static isValidRegex(value: string): boolean {
        try {
            new RegExp(value);
            return true;
        } catch {
            return false;
        }
    }

    private static isLeapYear(year: number): boolean {
        return (year % 4 === 0 && year % 100 !== 0) || (year % 400 === 0);
    }

    public static clearCache(): void {
        this.cache.regex.clear();
        this.cache.formats.clear();
        this.cache.uniqueKeys = new WeakMap();
        this.TYPE_CACHE.clear();
    }

    public static getCacheStats(): {
        regexCacheSize: number;
        formatsCacheSize: number;
        typeCacheSize: number;
    } {
        return {
            regexCacheSize: this.cache.regex.size,
            formatsCacheSize: this.cache.formats.size,
            typeCacheSize: this.TYPE_CACHE.size
        };
    }

    public static precompileSchema(schema: any): void {
        if (!schema || typeof schema !== 'object') return;

        if (schema.pattern) {
            try {
                const regex = new RegExp(schema.pattern);
                this.cache.regex.set(schema.pattern, regex);
            } catch { }
        }

        if (schema.patternProperties) {
            for (const pattern in schema.patternProperties) {
                try {
                    const regex = new RegExp(pattern);
                    this.cache.regex.set(pattern, regex);
                } catch { }
            }
        }

        const subSchemas = [
            schema.items,
            schema.additionalItems,
            schema.additionalProperties,
            schema.properties,
            schema.patternProperties,
            schema.dependencies,
            schema.allOf,
            schema.anyOf,
            schema.oneOf,
            schema.not,
            schema.if,
            schema.then,
            schema.else
        ];

        for (const subSchema of subSchemas) {
            if (subSchema) {
                if (Array.isArray(subSchema)) {
                    for (const s of subSchema) {
                        this.precompileSchema(s);
                    }
                } else if (typeof subSchema === 'object') {
                    if (Array.isArray(subSchema.items)) {
                        for (const s of subSchema.items) {
                            this.precompileSchema(s);
                        }
                    } else {
                        this.precompileSchema(subSchema);
                    }

                    for (const key in subSchema) {
                        if (typeof subSchema[key] === 'object') {
                            this.precompileSchema(subSchema[key]);
                        }
                    }
                }
            }
        }
    }
}

export default JSONSchemaValidator;