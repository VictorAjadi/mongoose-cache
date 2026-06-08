/**
 * MongoDB Expression Evaluator
 * High-performance evaluation engine for MongoDB query expressions
 * @module MongoExpressionEvaluator
 */
declare class MongoExpressionEvaluator {
    private static readonly operatorCache;
    private static readonly fieldPathCache;
    private static readonly diacriticMap;
    private static initializeDiacriticMap;
    static evaluateExpression(document: any, expr: any, root?: any): boolean;
    private static evaluateOperator;
    private static getOperatorEvaluator;
    private static evaluateComparison;
    private static evaluateLogical;
    private static evaluateExists;
    private static evaluateType;
    private static evaluateArrayOperator;
    private static evaluateFullTextSearch;
    private static evaluateRegexSearch;
    private static extractTextContent;
    private static parseSearchTerms;
    private static performTextSearch;
    private static removeDiacriticsFast;
    private static evaluateConditional;
    private static evaluateIfNull;
    private static evaluateSwitch;
    private static evaluateArithmetic;
    private static evaluateString;
    private static evaluateDate;
    private static evaluateCmp;
    private static resolveValue;
    private static getFieldValue;
    private static fastEquals;
    private static compare;
    private static getBSONType;
    private static bsonTypeNumberToString;
    private static escapeRegex;
    static clearCache(): void;
    static getCacheStats(): {
        operatorCacheSize: number;
        fieldPathCacheSize: number;
    };
}
export default MongoExpressionEvaluator;
