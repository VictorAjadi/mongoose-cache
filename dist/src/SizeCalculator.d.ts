/**
 * Uses exact BSON serialization logic with proper type handling
 */
export declare class SizeCalculator {
    private static readonly BSON_TYPES;
    private static readonly TYPE_SIZES;
    private static readonly DOCUMENT_OVERHEAD;
    private static readonly ELEMENT_OVERHEAD;
    private static readonly STRING_OVERHEAD;
    private static readonly BINARY_OVERHEAD;
    private static readonly CODE_W_SCOPE_BASE;
    private static readonly DBREF_BASE;
    private static readonly FIELD_NAME_TERMINATOR;
    private static readonly CSTRING_TERMINATOR;
    private static readonly UTF8_CACHE;
    private static readonly FIELD_CACHE;
    private visited;
    private depth;
    private static readonly MAX_DEPTH;
    /**
     * 100% ACCURATE fast size estimation for ALL data types and structures
     */
    static fastSizeEstimate(value: any): number;
    /**
     * Main size calculation entry point
     */
    private calculateSize;
    /**
     * Handle ALL BSON special types using proper type constants
     */
    private handleBSONSpecialTypes;
    /**
     * Handle generic object types (arrays, plain objects, class instances)
     */
    private handleObjectType;
    /**
     * Calculate BSON document size using DOCUMENT_OVERHEAD constant
     * DOCUMENT_OVERHEAD = int32 size (4) + null terminator (1) = 5 bytes
     */
    private calculateDocumentSize;
    /**
     * Calculate BSON array size using DOCUMENT_OVERHEAD constant
     * Arrays use same overhead as documents: int32 size (4) + null terminator (1)
     */
    private calculateArraySize;
    /**
     * Calculate number size using type constants
     */
    private calculateNumberSize;
    /**
     * Calculate string size using STRING_OVERHEAD constant
     * STRING_OVERHEAD = int32 length (4) + null terminator (1) = 5 bytes
     */
    private calculateStringSize;
    /**
     * Calculate symbol size using SYMBOL type constant
     */
    private calculateSymbolSize;
    /**
     * Calculate binary size using BINARY_OVERHEAD constant
     * BINARY_OVERHEAD = int32 length (4) + subtype (1) = 5 bytes
     */
    private calculateBinarySize;
    /**
     * Calculate RegExp size using REGEXP type constant
     */
    private calculateRegExpSize;
    /**
     * Calculate Code size using CODE and CODE_W_SCOPE constants
     * CODE_W_SCOPE_BASE = int32 total_size (4) + int32 code_length (4) = 8 bytes
     */
    private calculateCodeSize;
    /**
     * Calculate DBRef size using DBREF_BASE constant
     * DBREF_BASE = 12 bytes overhead for field names and structure
     * DBRef structure: { $ref: string, $id: any, $db?: string }
     */
    private calculateDBRefSize;
    /**
     * Handle Map as BSON document
     */
    private handleMap;
    /**
     * Handle Set as BSON array
     */
    private handleSet;
    /**
     * Calculate size for any BSON type using type constants
     */
    private calculateTypeSize;
    /**
     * Calculate field overhead using ELEMENT_OVERHEAD constant
     * Field overhead = ELEMENT_OVERHEAD (type byte) + field name UTF-8 + FIELD_NAME_TERMINATOR
     */
    private getFieldOverhead;
    /**
     * Calculate UTF-8 byte length with caching
     */
    private utf8Length;
    /**
     * Static utility methods
     */
    static calculateSize(value: any): number;
    static calculateMemoryFootprint(value: any): number;
    static calculateBatchSize(documents: any[]): {
        total: number;
        average: number;
        sizes: number[];
    };
    static validateSizeCalculation(document: any, expectedSize: number): {
        calculated: number;
        expected: number;
        difference: number;
        isValid: boolean;
        accuracyPercent: number;
    };
    static clearCaches(): void;
}
export declare const calculateBSONSize: typeof SizeCalculator.fastSizeEstimate;
export declare const calculateMemoryFootprint: typeof SizeCalculator.calculateMemoryFootprint;
export declare const calculateBatchSize: typeof SizeCalculator.calculateBatchSize;
export declare const validateSizeCalculation: typeof SizeCalculator.validateSizeCalculation;
export declare const fastSizeEstimate: typeof SizeCalculator.fastSizeEstimate;
export default SizeCalculator;
