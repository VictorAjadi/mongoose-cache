/**
 * MongoDB Expression Evaluator
 * High-performance evaluation engine for MongoDB query expressions
 * @module MongoExpressionEvaluator
 */
import { ObjectId } from 'mongodb';
class MongoExpressionEvaluator {
    static operatorCache = new Map();
    static fieldPathCache = new Map();
    static diacriticMap = new Map();
    static {
        this.initializeDiacriticMap();
    }
    static initializeDiacriticMap() {
        const diacritics = {
            'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a', 'å': 'a', 'æ': 'ae',
            'ç': 'c', 'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
            'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
            'ñ': 'n', 'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o', 'ø': 'o',
            'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
            'ý': 'y', 'ÿ': 'y',
            'À': 'A', 'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A', 'Å': 'A', 'Æ': 'AE',
            'Ç': 'C', 'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
            'Ì': 'I', 'Í': 'I', 'Î': 'I', 'Ï': 'I',
            'Ñ': 'N', 'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O', 'Ø': 'O',
            'Ù': 'U', 'Ú': 'U', 'Û': 'U', 'Ü': 'U',
            'Ý': 'Y'
        };
        for (const [key, value] of Object.entries(diacritics)) {
            this.diacriticMap.set(key, value);
        }
    }
    static evaluateExpression(document, expr, root) {
        if (!expr || typeof expr !== 'object')
            return true;
        const context = {
            document,
            root: root || document,
            variables: new Map()
        };
        for (const [operator, operand] of Object.entries(expr)) {
            if (!this.evaluateOperator(operator, operand, context)) {
                return false;
            }
        }
        return true;
    }
    static evaluateOperator(operator, operand, context) {
        const cacheKey = `${operator}:${typeof operand}`;
        let evaluator = this.operatorCache.get(cacheKey);
        if (!evaluator) {
            evaluator = this.getOperatorEvaluator(operator);
            this.operatorCache.set(cacheKey, evaluator);
        }
        return evaluator(operand, context);
    }
    static getOperatorEvaluator(operator) {
        switch (operator) {
            // Comparison operators
            case '$eq': return (op, ctx) => this.evaluateComparison('$eq', op, ctx);
            case '$ne': return (op, ctx) => this.evaluateComparison('$ne', op, ctx);
            case '$gt': return (op, ctx) => this.evaluateComparison('$gt', op, ctx);
            case '$gte': return (op, ctx) => this.evaluateComparison('$gte', op, ctx);
            case '$lt': return (op, ctx) => this.evaluateComparison('$lt', op, ctx);
            case '$lte': return (op, ctx) => this.evaluateComparison('$lte', op, ctx);
            case '$in': return (op, ctx) => this.evaluateComparison('$in', op, ctx);
            case '$nin': return (op, ctx) => this.evaluateComparison('$nin', op, ctx);
            // Logical operators
            case '$and': return (op, ctx) => this.evaluateLogical('$and', op, ctx);
            case '$or': return (op, ctx) => this.evaluateLogical('$or', op, ctx);
            case '$not': return (op, ctx) => this.evaluateLogical('$not', op, ctx);
            case '$nor': return (op, ctx) => this.evaluateLogical('$nor', op, ctx);
            // Element operators
            case '$exists': return (op, ctx) => this.evaluateExists(op, ctx);
            case '$type': return (op, ctx) => this.evaluateType(op, ctx);
            // Array operators
            case '$all': return (op, ctx) => this.evaluateArrayOperator('$all', op, ctx);
            case '$elemMatch': return (op, ctx) => this.evaluateArrayOperator('$elemMatch', op, ctx);
            case '$size': return (op, ctx) => this.evaluateArrayOperator('$size', op, ctx);
            // Text search
            case '$text': return (op, ctx) => this.evaluateFullTextSearch(op, ctx);
            case '$regex': return (op, ctx) => this.evaluateRegexSearch(op, ctx);
            // Conditional operators
            case '$cond': return (op, ctx) => !!this.evaluateConditional(op, ctx);
            case '$ifNull': return (op, ctx) => !!this.evaluateIfNull(op, ctx);
            case '$switch': return (op, ctx) => !!this.evaluateSwitch(op, ctx);
            // Arithmetic operators
            case '$add':
            case '$subtract':
            case '$multiply':
            case '$divide':
            case '$mod':
            case '$abs':
            case '$ceil':
            case '$floor':
            case '$round':
            case '$sqrt':
            case '$pow':
            case '$ln':
            case '$log':
            case '$log10':
            case '$exp':
            case '$trunc':
                return (op, ctx) => this.evaluateArithmetic(operator, op, ctx) !== null;
            // String operators
            case '$concat':
            case '$substr':
            case '$substrCP':
            case '$substrBytes':
            case '$toLower':
            case '$toUpper':
            case '$strcasecmp':
            case '$strLenCP':
            case '$strLenBytes':
            case '$indexOfBytes':
            case '$indexOfCP':
            case '$split':
            case '$trim':
            case '$ltrim':
            case '$rtrim':
            case '$replaceOne':
            case '$replaceAll':
                return (op, ctx) => this.evaluateString(operator, op, ctx) !== null;
            // Date operators
            case '$year':
            case '$month':
            case '$dayOfMonth':
            case '$dayOfWeek':
            case '$dayOfYear':
            case '$hour':
            case '$minute':
            case '$second':
            case '$millisecond':
            case '$week':
            case '$isoWeek':
            case '$isoWeekYear':
            case '$isoDayOfWeek':
            case '$dateToString':
            case '$dateFromString':
            case '$dateToParts':
            case '$dateFromParts':
                return (op, ctx) => this.evaluateDate(operator, op, ctx) !== null;
            // Comparison aggregation
            case '$cmp': return (op, ctx) => this.evaluateCmp(op, ctx) !== null;
            // Field path reference
            default:
                if (operator.startsWith('$')) {
                    return (op, ctx) => {
                        const value = this.getFieldValue(operator.substring(1), ctx.document);
                        return value !== undefined && value !== null;
                    };
                }
                return () => true;
        }
    }
    static evaluateComparison(op, operand, context) {
        const values = Array.isArray(operand)
            ? operand.map(val => this.resolveValue(val, context))
            : [this.resolveValue(operand, context)];
        if (values.length < 2 && op !== '$in' && op !== '$nin')
            return true;
        const [left, right] = values;
        switch (op) {
            case '$eq':
                return this.fastEquals(left, right);
            case '$ne':
                return !this.fastEquals(left, right);
            case '$gt':
                return this.compare(left, right) > 0;
            case '$gte':
                return this.compare(left, right) >= 0;
            case '$lt':
                return this.compare(left, right) < 0;
            case '$lte':
                return this.compare(left, right) <= 0;
            case '$in':
                if (!Array.isArray(right))
                    return false;
                return right.some(val => this.fastEquals(left, val));
            case '$nin':
                if (!Array.isArray(right))
                    return true;
                return !right.some(val => this.fastEquals(left, val));
            default:
                return true;
        }
    }
    static evaluateLogical(op, operand, context) {
        switch (op) {
            case '$and':
                if (!Array.isArray(operand))
                    return true;
                for (const expr of operand) {
                    if (!this.evaluateExpression(context.document, expr, context.root)) {
                        return false;
                    }
                }
                return true;
            case '$or':
                if (!Array.isArray(operand))
                    return true;
                for (const expr of operand) {
                    if (this.evaluateExpression(context.document, expr, context.root)) {
                        return true;
                    }
                }
                return false;
            case '$not':
                return !this.evaluateExpression(context.document, operand, context.root);
            case '$nor':
                if (!Array.isArray(operand))
                    return true;
                for (const expr of operand) {
                    if (this.evaluateExpression(context.document, expr, context.root)) {
                        return false;
                    }
                }
                return true;
            default:
                return true;
        }
    }
    static evaluateExists(operand, context) {
        const shouldExist = !!operand;
        return shouldExist
            ? context.document !== undefined && context.document !== null
            : context.document === undefined || context.document === null;
    }
    static evaluateType(operand, context) {
        const value = context.document;
        const typeSpec = operand;
        const actualType = this.getBSONType(value);
        if (typeof typeSpec === 'string') {
            return actualType === typeSpec;
        }
        else if (typeof typeSpec === 'number') {
            return actualType === this.bsonTypeNumberToString(typeSpec);
        }
        else if (Array.isArray(typeSpec)) {
            return typeSpec.some(t => typeof t === 'string'
                ? actualType === t
                : actualType === this.bsonTypeNumberToString(t));
        }
        return false;
    }
    static evaluateArrayOperator(op, operand, context) {
        const value = context.document;
        switch (op) {
            case '$all':
                if (!Array.isArray(value) || !Array.isArray(operand))
                    return false;
                return operand.every(searchVal => value.some(docVal => this.fastEquals(docVal, searchVal)));
            case '$elemMatch':
                if (!Array.isArray(value))
                    return false;
                return value.some(item => this.evaluateExpression(item, operand, context.root));
            case '$size':
                if (!Array.isArray(value))
                    return false;
                return value.length === operand;
            default:
                return false;
        }
    }
    static evaluateFullTextSearch(operand, context) {
        if (!operand || typeof operand !== 'object')
            return false;
        const searchText = operand.$search;
        if (!searchText || typeof searchText !== 'string')
            return false;
        const options = {
            caseSensitive: operand.$caseSensitive || false,
            diacriticSensitive: operand.$diacriticSensitive || false,
            language: operand.$language || 'en'
        };
        const textContent = this.extractTextContent(context.document);
        const searchTerms = this.parseSearchTerms(searchText);
        return this.performTextSearch(textContent, searchTerms, options);
    }
    static evaluateRegexSearch(operand, context) {
        const textContent = this.extractTextContent(context.document);
        if (typeof operand === 'string') {
            try {
                const regex = new RegExp(operand, 'i');
                return regex.test(textContent);
            }
            catch {
                return false;
            }
        }
        if (typeof operand === 'object' && operand !== null) {
            const pattern = operand.$regex || operand.pattern;
            const options = operand.$options || operand.flags || '';
            if (!pattern)
                return false;
            try {
                const regex = new RegExp(pattern, options);
                return regex.test(textContent);
            }
            catch {
                return false;
            }
        }
        return false;
    }
    static extractTextContent(obj, visited = new WeakSet()) {
        if (obj === null || obj === undefined)
            return '';
        const type = typeof obj;
        if (type === 'string')
            return obj;
        if (type === 'number' || type === 'boolean')
            return String(obj);
        if (type !== 'object')
            return '';
        if (visited.has(obj))
            return '';
        visited.add(obj);
        if (Array.isArray(obj)) {
            const parts = [];
            for (let i = 0; i < obj.length; i++) {
                const part = this.extractTextContent(obj[i], visited);
                if (part)
                    parts.push(part);
            }
            return parts.join(' ');
        }
        const parts = [];
        for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                const part = this.extractTextContent(obj[key], visited);
                if (part)
                    parts.push(part);
            }
        }
        return parts.join(' ');
    }
    static parseSearchTerms(searchText) {
        const terms = { include: [], exclude: [], phrases: [] };
        const phraseRegex = /"([^"]+)"/g;
        let match;
        while ((match = phraseRegex.exec(searchText)) !== null) {
            terms.phrases.push(match[1]);
        }
        const remainingText = searchText.replace(phraseRegex, '');
        const words = remainingText.split(/\s+/).filter(word => word.length > 0);
        for (const word of words) {
            if (word.startsWith('-') && word.length > 1) {
                terms.exclude.push(word.substring(1).toLowerCase());
            }
            else {
                terms.include.push(word.toLowerCase());
            }
        }
        return terms;
    }
    static performTextSearch(textContent, searchTerms, options) {
        let content = options.caseSensitive ? textContent : textContent.toLowerCase();
        if (!options.diacriticSensitive) {
            content = this.removeDiacriticsFast(content);
        }
        for (const excludeTerm of searchTerms.exclude) {
            let term = excludeTerm;
            if (!options.diacriticSensitive) {
                term = this.removeDiacriticsFast(term);
            }
            if (content.indexOf(term) !== -1) {
                return false;
            }
        }
        for (const phrase of searchTerms.phrases) {
            let searchPhrase = options.caseSensitive ? phrase : phrase.toLowerCase();
            if (!options.diacriticSensitive) {
                searchPhrase = this.removeDiacriticsFast(searchPhrase);
            }
            if (content.indexOf(searchPhrase) === -1) {
                return false;
            }
        }
        if (searchTerms.include.length > 0) {
            for (const term of searchTerms.include) {
                let searchTerm = term;
                if (!options.diacriticSensitive) {
                    searchTerm = this.removeDiacriticsFast(searchTerm);
                }
                if (content.indexOf(searchTerm) !== -1) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }
    static removeDiacriticsFast(str) {
        const len = str.length;
        const result = new Array(len);
        let hasChanges = false;
        for (let i = 0; i < len; i++) {
            const char = str[i];
            const replacement = this.diacriticMap.get(char);
            if (replacement) {
                result[i] = replacement;
                hasChanges = true;
            }
            else {
                result[i] = char;
            }
        }
        return hasChanges ? result.join('') : str;
    }
    static evaluateConditional(operand, context) {
        if (!Array.isArray(operand) || operand.length !== 3)
            return null;
        const condition = this.resolveValue(operand[0], context);
        return condition
            ? this.resolveValue(operand[1], context)
            : this.resolveValue(operand[2], context);
    }
    static evaluateIfNull(operand, context) {
        if (!Array.isArray(operand) || operand.length !== 2)
            return null;
        const expr = this.resolveValue(operand[0], context);
        return expr !== null && expr !== undefined
            ? expr
            : this.resolveValue(operand[1], context);
    }
    static evaluateSwitch(operand, context) {
        if (!operand || typeof operand !== 'object')
            return null;
        const branches = operand.branches || [];
        for (const branch of branches) {
            if (branch.case && branch.then) {
                const caseValue = this.resolveValue(branch.case, context);
                if (caseValue) {
                    return this.resolveValue(branch.then, context);
                }
            }
        }
        return operand.default !== undefined
            ? this.resolveValue(operand.default, context)
            : null;
    }
    static evaluateArithmetic(op, operand, context) {
        const values = Array.isArray(operand)
            ? operand.map(val => this.resolveValue(val, context))
            : [this.resolveValue(operand, context)];
        const numbers = values.map(v => {
            const num = Number(v);
            return isNaN(num) ? null : num;
        }).filter(n => n !== null);
        if (numbers.length === 0)
            return null;
        switch (op) {
            case '$add':
                return numbers.reduce((sum, num) => sum + num, 0);
            case '$subtract':
                return numbers.length >= 2 ? numbers[0] - numbers[1] : numbers[0];
            case '$multiply':
                return numbers.reduce((product, num) => product * num, 1);
            case '$divide':
                if (numbers.length >= 2 && numbers[1] !== 0) {
                    return numbers[0] / numbers[1];
                }
                return null;
            case '$mod':
                if (numbers.length >= 2 && numbers[1] !== 0) {
                    return numbers[0] % numbers[1];
                }
                return null;
            case '$abs':
                return Math.abs(numbers[0]);
            case '$ceil':
                return Math.ceil(numbers[0]);
            case '$floor':
                return Math.floor(numbers[0]);
            case '$round':
                if (numbers.length >= 2) {
                    const decimals = numbers[1];
                    const multiplier = Math.pow(10, decimals);
                    return Math.round(numbers[0] * multiplier) / multiplier;
                }
                return Math.round(numbers[0]);
            case '$trunc':
                if (numbers.length >= 2) {
                    const decimals = numbers[1];
                    const multiplier = Math.pow(10, decimals);
                    return Math.trunc(numbers[0] * multiplier) / multiplier;
                }
                return Math.trunc(numbers[0]);
            case '$sqrt':
                return numbers[0] >= 0 ? Math.sqrt(numbers[0]) : null;
            case '$pow':
                return numbers.length >= 2 ? Math.pow(numbers[0], numbers[1]) : null;
            case '$ln':
                return numbers[0] > 0 ? Math.log(numbers[0]) : null;
            case '$log':
                if (numbers.length >= 2 && numbers[0] > 0 && numbers[1] > 0 && numbers[1] !== 1) {
                    return Math.log(numbers[0]) / Math.log(numbers[1]);
                }
                return null;
            case '$log10':
                return numbers[0] > 0 ? Math.log10(numbers[0]) : null;
            case '$exp':
                return Math.exp(numbers[0]);
            default:
                return null;
        }
    }
    static evaluateString(op, operand, context) {
        const values = Array.isArray(operand)
            ? operand.map(val => this.resolveValue(val, context))
            : [this.resolveValue(operand, context)];
        switch (op) {
            case '$concat':
                return values.map(val => String(val ?? '')).join('');
            case '$substr':
            case '$substrBytes':
                if (values.length >= 3) {
                    const str = String(values[0] ?? '');
                    const start = Math.max(0, Number(values[1]) || 0);
                    const length = Number(values[2]) || 0;
                    return str.substring(start, start + length);
                }
                return '';
            case '$substrCP':
                if (values.length >= 3) {
                    const str = String(values[0] ?? '');
                    const codePoints = [...str];
                    const start = Math.max(0, Number(values[1]) || 0);
                    const length = Number(values[2]) || 0;
                    return codePoints.slice(start, start + length).join('');
                }
                return '';
            case '$toLower':
                return String(values[0] ?? '').toLowerCase();
            case '$toUpper':
                return String(values[0] ?? '').toUpperCase();
            case '$strcasecmp':
                if (values.length >= 2) {
                    const str1 = String(values[0] ?? '').toLowerCase();
                    const str2 = String(values[1] ?? '').toLowerCase();
                    return str1 < str2 ? -1 : str1 > str2 ? 1 : 0;
                }
                return 0;
            case '$strLenCP':
                return [...String(values[0] ?? '')].length;
            case '$strLenBytes':
                return Buffer.byteLength(String(values[0] ?? ''), 'utf8');
            case '$indexOfBytes':
            case '$indexOfCP': {
                if (values.length >= 2) {
                    const str = String(values[0] ?? '');
                    const substring = String(values[1] ?? '');
                    const start = values.length >= 3 ? Math.max(0, Number(values[2]) || 0) : 0;
                    const end = values.length >= 4 ? Number(values[3]) : str.length;
                    if (op === '$indexOfCP') {
                        const codePoints = [...str];
                        const searchPoints = [...substring];
                        for (let i = start; i < Math.min(end, codePoints.length); i++) {
                            let match = true;
                            for (let j = 0; j < searchPoints.length && i + j < codePoints.length; j++) {
                                if (codePoints[i + j] !== searchPoints[j]) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match)
                                return i;
                        }
                    }
                    else {
                        const searchStr = str.substring(start, end);
                        const index = searchStr.indexOf(substring);
                        return index === -1 ? -1 : start + index;
                    }
                }
                return -1;
            }
            case '$split': {
                if (values.length >= 2) {
                    const str = String(values[0] ?? '');
                    const delimiter = String(values[1] ?? '');
                    return delimiter ? str.split(delimiter) : [str];
                }
                return null;
            }
            case '$trim': {
                const str = String(values[0] ?? '');
                const chars = values.length >= 2 ? String(values[1]) : null;
                if (chars) {
                    const pattern = new RegExp(`^[${this.escapeRegex(chars)}]+|[${this.escapeRegex(chars)}]+$`, 'g');
                    return str.replace(pattern, '');
                }
                return str.trim();
            }
            case '$ltrim': {
                const str = String(values[0] ?? '');
                const chars = values.length >= 2 ? String(values[1]) : null;
                if (chars) {
                    const pattern = new RegExp(`^[${this.escapeRegex(chars)}]+`, 'g');
                    return str.replace(pattern, '');
                }
                return str.trimStart();
            }
            case '$rtrim': {
                const str = String(values[0] ?? '');
                const chars = values.length >= 2 ? String(values[1]) : null;
                if (chars) {
                    const pattern = new RegExp(`[${this.escapeRegex(chars)}]+$`, 'g');
                    return str.replace(pattern, '');
                }
                return str.trimEnd();
            }
            case '$replaceOne':
                if (values.length >= 3) {
                    const str = String(values[0] ?? '');
                    const find = String(values[1] ?? '');
                    const replacement = String(values[2] ?? '');
                    return str.replace(find, replacement);
                }
                return '';
            case '$replaceAll':
                if (values.length >= 3) {
                    const str = String(values[0] ?? '');
                    const find = String(values[1] ?? '');
                    const replacement = String(values[2] ?? '');
                    return str.split(find).join(replacement);
                }
                return '';
            default:
                return null;
        }
    }
    static evaluateDate(operator, value, context) {
        const resolve = (v) => this.resolveValue(v, context);
        const ensureDate = (v) => {
            if (v instanceof Date)
                return v;
            const d = new Date(v);
            if (isNaN(d.getTime()))
                throw new Error(`Invalid date: ${v}`);
            return d;
        };
        const getUnitMillis = (unit) => {
            const map = {
                millisecond: 1,
                second: 1000,
                minute: 60 * 1000,
                hour: 60 * 60 * 1000,
                day: 24 * 60 * 60 * 1000,
                week: 7 * 24 * 60 * 60 * 1000,
                month: 30 * 24 * 60 * 60 * 1000, // approximate for subtraction
                quarter: 3 * 30 * 24 * 60 * 60 * 1000,
                year: 365 * 24 * 60 * 60 * 1000
            };
            return map[unit] ?? 0;
        };
        const addDate = (date, amount, unit) => {
            const d = new Date(date);
            switch (unit) {
                case 'year':
                    d.setFullYear(d.getFullYear() + amount);
                    break;
                case 'quarter':
                    d.setMonth(d.getMonth() + amount * 3);
                    break;
                case 'month':
                    d.setMonth(d.getMonth() + amount);
                    break;
                case 'week':
                    d.setDate(d.getDate() + amount * 7);
                    break;
                case 'day':
                    d.setDate(d.getDate() + amount);
                    break;
                case 'hour':
                    d.setHours(d.getHours() + amount);
                    break;
                case 'minute':
                    d.setMinutes(d.getMinutes() + amount);
                    break;
                case 'second':
                    d.setSeconds(d.getSeconds() + amount);
                    break;
                case 'millisecond':
                    d.setMilliseconds(d.getMilliseconds() + amount);
                    break;
                default: throw new Error(`Unsupported date unit: ${unit}`);
            }
            return d;
        };
        switch (operator) {
            case '$dateAdd': {
                const { startDate, unit, amount, timezone } = value;
                const start = ensureDate(resolve(startDate));
                const amt = Number(resolve(amount));
                const result = addDate(start, amt, unit);
                return timezone ? new Date(result.toLocaleString('en-US', { timeZone: timezone })) : result;
            }
            case '$dateSubtract': {
                const { startDate, unit, amount, timezone } = value;
                const start = ensureDate(resolve(startDate));
                const amt = Number(resolve(amount));
                const result = addDate(start, -amt, unit);
                return timezone ? new Date(result.toLocaleString('en-US', { timeZone: timezone })) : result;
            }
            case '$dateDiff': {
                const { startDate, endDate, unit } = value;
                const start = ensureDate(resolve(startDate));
                const end = ensureDate(resolve(endDate));
                const diffMs = end.getTime() - start.getTime();
                switch (unit) {
                    case 'year': return end.getFullYear() - start.getFullYear();
                    case 'month': return (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
                    case 'week': return diffMs / getUnitMillis('week');
                    case 'day': return diffMs / getUnitMillis('day');
                    case 'hour': return diffMs / getUnitMillis('hour');
                    case 'minute': return diffMs / getUnitMillis('minute');
                    case 'second': return diffMs / getUnitMillis('second');
                    case 'millisecond': return diffMs;
                    default: throw new Error(`Unsupported dateDiff unit: ${unit}`);
                }
            }
            case '$year': return ensureDate(resolve(value)).getUTCFullYear();
            case '$month': return ensureDate(resolve(value)).getUTCMonth() + 1;
            case '$dayOfMonth': return ensureDate(resolve(value)).getUTCDate();
            case '$dayOfWeek': return ensureDate(resolve(value)).getUTCDay() + 1;
            case '$hour': return ensureDate(resolve(value)).getUTCHours();
            case '$minute': return ensureDate(resolve(value)).getUTCMinutes();
            case '$second': return ensureDate(resolve(value)).getUTCSeconds();
            case '$millisecond': return ensureDate(resolve(value)).getUTCMilliseconds();
            case '$isoWeekYear': {
                const d = ensureDate(resolve(value));
                const year = d.getUTCFullYear();
                const firstThursday = new Date(Date.UTC(year, 0, 4));
                const dayDiff = (d.getTime() - firstThursday.getTime()) / getUnitMillis('day');
                const weekNum = Math.floor((dayDiff + firstThursday.getUTCDay() + 1) / 7);
                return weekNum < 1 ? year - 1 : weekNum > 52 ? year + 1 : year;
            }
            case '$isoWeek': {
                const d = ensureDate(resolve(value));
                const day = d.getUTCDay() || 7;
                d.setUTCDate(d.getUTCDate() + 4 - day);
                const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
                return Math.ceil(((d.getTime() - yearStart.getTime()) / getUnitMillis('day') + 1) / 7);
            }
            case '$isoDayOfWeek': return ensureDate(resolve(value)).getUTCDay() || 7;
            case '$dateTrunc': {
                const { date, unit, binSize = 1, timezone } = value;
                const d = ensureDate(resolve(date));
                const truncated = new Date(d);
                switch (unit) {
                    case 'year':
                        truncated.setUTCMonth(0, 1);
                        truncated.setUTCHours(0, 0, 0, 0);
                        break;
                    case 'month':
                        truncated.setUTCDate(1);
                        truncated.setUTCHours(0, 0, 0, 0);
                        break;
                    case 'day':
                        truncated.setUTCHours(0, 0, 0, 0);
                        break;
                    case 'hour':
                        truncated.setUTCMinutes(0, 0, 0);
                        break;
                    case 'minute':
                        truncated.setUTCSeconds(0, 0);
                        break;
                    case 'second':
                        truncated.setUTCMilliseconds(0);
                        break;
                }
                if (timezone)
                    return new Date(truncated.toLocaleString('en-US', { timeZone: timezone }));
                return truncated;
            }
            case '$dateFromParts': {
                const { year, month = 1, day = 1, hour = 0, minute = 0, second = 0, millisecond = 0, timezone } = value;
                const d = new Date(Date.UTC(year, month - 1, day, hour, minute, second, millisecond));
                return timezone ? new Date(d.toLocaleString('en-US', { timeZone: timezone })) : d;
            }
            case '$dateToString': {
                const { date, format = '%Y-%m-%dT%H:%M:%S.%LZ', timezone } = value;
                const d = ensureDate(resolve(date));
                const target = timezone ? new Date(d.toLocaleString('en-US', { timeZone: timezone })) : d;
                const pad = (n, len = 2) => n.toString().padStart(len, '0');
                return format
                    .replace('%Y', pad(target.getUTCFullYear(), 4))
                    .replace('%m', pad(target.getUTCMonth() + 1))
                    .replace('%d', pad(target.getUTCDate()))
                    .replace('%H', pad(target.getUTCHours()))
                    .replace('%M', pad(target.getUTCMinutes()))
                    .replace('%S', pad(target.getUTCSeconds()))
                    .replace('%L', pad(target.getUTCMilliseconds(), 3));
            }
            default:
                return null;
        }
    }
    static evaluateCmp(operand, context) {
        if (!Array.isArray(operand) || operand.length !== 2)
            return null;
        const left = this.resolveValue(operand[0], context);
        const right = this.resolveValue(operand[1], context);
        return this.compare(left, right);
    }
    static resolveValue(value, context) {
        if (value === null || value === undefined)
            return value;
        // Handle string expressions like $ROOT, $CURRENT, or $field
        if (typeof value === 'string' && value.startsWith('$')) {
            if (value === '$ROOT')
                return context.root;
            if (value === '$CURRENT')
                return context.document;
            // Handle variables like $$varName
            if (value.startsWith('$$')) {
                const varName = value.substring(2);
                return context.variables?.get(varName);
            }
            // Handle field path like $fieldName.subField
            return this.getFieldValue(value.substring(1), context.document);
        }
        // Handle objects
        if (typeof value === 'object' && !Array.isArray(value)) {
            const keys = Object.keys(value);
            // Check for operator expressions like { $sum: ... } or { $dateFromString: ... }
            if (keys.length > 0 && keys.some(key => key.startsWith('$'))) {
                for (const [key, val] of Object.entries(value)) {
                    if (key.startsWith('$')) {
                        const evaluator = this.getOperatorEvaluator(key);
                        return evaluator(val, context);
                    }
                }
            }
            // Otherwise, recursively resolve fields in a plain object
            const resolved = {};
            for (const [key, val] of Object.entries(value)) {
                resolved[key] = this.resolveValue(val, context);
            }
            return resolved;
        }
        // Handle arrays recursively
        if (Array.isArray(value)) {
            return value.map(item => this.resolveValue(item, context));
        }
        // Primitive value
        return value;
    }
    static getFieldValue(fieldPath, document) {
        if (!document || typeof document !== 'object')
            return undefined;
        let cachedPath = this.fieldPathCache.get(fieldPath);
        if (!cachedPath) {
            cachedPath = fieldPath.split('.');
            if (this.fieldPathCache.size < 1000) {
                this.fieldPathCache.set(fieldPath, cachedPath);
            }
        }
        let current = document;
        for (let i = 0; i < cachedPath.length; i++) {
            if (current == null)
                return undefined;
            current = current[cachedPath[i]];
        }
        return current;
    }
    static fastEquals(a, b) {
        if (a === b)
            return true;
        if (a == null || b == null)
            return a === b;
        const typeA = typeof a;
        const typeB = typeof b;
        if (typeA !== typeB)
            return false;
        if (typeA === 'object') {
            if (a instanceof ObjectId || b instanceof ObjectId) {
                return String(a) === String(b);
            }
            if (a instanceof Date && b instanceof Date) {
                return a.getTime() === b.getTime();
            }
            if (Array.isArray(a) && Array.isArray(b)) {
                if (a.length !== b.length)
                    return false;
                for (let i = 0; i < a.length; i++) {
                    if (!this.fastEquals(a[i], b[i]))
                        return false;
                }
                return true;
            }
            if (Array.isArray(a) || Array.isArray(b))
                return false;
            const keysA = Object.keys(a);
            const keysB = Object.keys(b);
            if (keysA.length !== keysB.length)
                return false;
            for (const key of keysA) {
                if (!Object.prototype.hasOwnProperty.call(b, key))
                    return false;
                if (!this.fastEquals(a[key], b[key]))
                    return false;
            }
            return true;
        }
        return false;
    }
    static compare(a, b) {
        if (a === b)
            return 0;
        if (a == null)
            return b == null ? 0 : -1;
        if (b == null)
            return 1;
        const typeA = typeof a;
        const typeB = typeof b;
        if (typeA === 'number' && typeB === 'number') {
            return a < b ? -1 : a > b ? 1 : 0;
        }
        if (typeA === 'string' && typeB === 'string') {
            return a.localeCompare(b);
        }
        if (a instanceof Date && b instanceof Date) {
            const diff = a.getTime() - b.getTime();
            return diff < 0 ? -1 : diff > 0 ? 1 : 0;
        }
        if (a instanceof ObjectId && b instanceof ObjectId) {
            return String(a).localeCompare(String(b));
        }
        const typeOrder = {
            'null': 0,
            'undefined': 0,
            'number': 1,
            'string': 2,
            'object': 3,
            'array': 4,
            'boolean': 5
        };
        const orderA = typeOrder[Array.isArray(a) ? 'array' : typeA] || 6;
        const orderB = typeOrder[Array.isArray(b) ? 'array' : typeB] || 6;
        if (orderA !== orderB) {
            return orderA < orderB ? -1 : 1;
        }
        const strA = String(a);
        const strB = String(b);
        return strA.localeCompare(strB);
    }
    static getBSONType(value) {
        if (value === null)
            return 'null';
        if (value === undefined)
            return 'undefined';
        const type = typeof value;
        if (type === 'number') {
            if (Number.isInteger(value)) {
                return value >= -2147483648 && value <= 2147483647 ? 'int' : 'long';
            }
            return 'double';
        }
        if (type === 'string')
            return 'string';
        if (type === 'boolean')
            return 'bool';
        if (type === 'object') {
            if (value instanceof Date)
                return 'date';
            if (value instanceof ObjectId)
                return 'objectId';
            if (value instanceof RegExp)
                return 'regex';
            if (Array.isArray(value))
                return 'array';
            if (Buffer.isBuffer(value))
                return 'binData';
            return 'object';
        }
        return 'unknown';
    }
    //Convert BSON type numbers to readable strings
    static bsonTypeNumberToString(typeNumber) {
        const typeMap = {
            '1': 'double',
            '2': 'string',
            '3': 'object',
            '4': 'array',
            '5': 'binData',
            '6': 'undefined',
            '7': 'objectId',
            '8': 'bool',
            '9': 'date',
            '10': 'null',
            '11': 'regex',
            '13': 'javascript',
            '14': 'symbol',
            '15': 'javascriptWithScope',
            '16': 'int',
            '17': 'timestamp',
            '18': 'long',
            '19': 'decimal',
            '-1': 'minKey',
            '127': 'maxKey'
        };
        return typeMap[String(typeNumber)] || 'unknown';
    }
    static escapeRegex(str) {
        // Escapes regex special characters: . * + ? ^ $ { } ( ) | [ ] \
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
    static clearCache() {
        this.operatorCache.clear();
        this.fieldPathCache.clear();
    }
    static getCacheStats() {
        return {
            operatorCacheSize: this.operatorCache.size,
            fieldPathCacheSize: this.fieldPathCache.size
        };
    }
}
export default MongoExpressionEvaluator;
