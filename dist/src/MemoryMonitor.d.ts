/**
 * MemoryMonitor - Unified utility for tracking heap utilization across the library
 */
export declare class MemoryMonitor {
    private static heapLimit;
    /**
     * Get the total available heap size for the current process
     */
    static getHeapLimit(): number;
    /**
     * Get current heap utilization percentage
     */
    static getHeapUtilization(): number;
    /**
     * Check if the process is under memory pressure based on a threshold
     */
    static isUnderPressure(threshold: number): boolean;
    /**
     * Get a detailed memory report
     */
    static getMemoryReport(): {
        heapUsed: number;
        heapTotal: number;
        heapLimit: number;
        utilization: number;
        rss: number;
        external: number;
    };
}
