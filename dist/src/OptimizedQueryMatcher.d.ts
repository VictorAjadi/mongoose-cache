export declare class OptimizedQueryMatcher {
    private static operatorCache;
    static documentMatchesQuery(doc: any, query: any): boolean;
    private static evaluateConditions;
    private static evaluateLogicalOperator;
    private static evaluateFieldCondition;
    private static evaluateFieldOperators;
    private static checkOperator;
    private static createOperatorFunction;
    private static checkType;
    private static checkBits;
    private static checkGeoIntersects;
    private static checkGeoWithin;
    private static checkNear;
    private static checkNearSphere;
    private static getNestedValue;
    private static valuesEqual;
}
