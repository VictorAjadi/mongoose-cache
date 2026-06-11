
/**
 * DEPRECATED: Size calculation is now integrated into DocumentSerializer.serialize()
 * for single-pass performance. This class remains for backward compatibility.
 */
export class SizeCalculator {
    public static calculateSize(_value: any): number {
        // Fallback to minimal size if called directly
        return 0;
    }

    public static fastSizeEstimate(_value: any): number {
        // Return 0 as it's no longer used in the main performance path
        return 0;
    }
}