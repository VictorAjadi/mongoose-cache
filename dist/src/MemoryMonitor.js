import * as v8 from 'node:v8';
/**
 * MemoryMonitor - Unified utility for tracking heap utilization across the library
 */
export class MemoryMonitor {
    static heapLimit = 0;
    /**
     * Get the total available heap size for the current process
     */
    static getHeapLimit() {
        if (this.heapLimit > 0)
            return this.heapLimit;
        try {
            // Node.js: Use v8 statistics for true container-aware heap limit
            if (v8 && typeof v8.getHeapStatistics === 'function') {
                const stats = v8.getHeapStatistics();
                this.heapLimit = stats.heap_size_limit;
                return this.heapLimit;
            }
            // Bun: Use performance.memory if available
            if (typeof performance !== 'undefined' && performance.memory) {
                this.heapLimit = performance.memory.jsHeapSizeLimit;
                return this.heapLimit;
            }
        }
        catch { }
        // Fallback or Bun: Use heapTotal or a safe default (1.5GB)
        const mem = process.memoryUsage();
        this.heapLimit = mem.heapTotal || 1536 * 1024 * 1024;
        return this.heapLimit;
    }
    /**
     * Get current heap utilization percentage
     */
    static getHeapUtilization() {
        const limit = this.getHeapLimit();
        const used = process.memoryUsage().heapUsed;
        return (used / limit) * 100;
    }
    /**
     * Check if the process is under memory pressure based on a threshold
     */
    static isUnderPressure(threshold) {
        return this.getHeapUtilization() >= threshold;
    }
    /**
     * Get a detailed memory report
     */
    static getMemoryReport() {
        const mem = process.memoryUsage();
        const limit = this.getHeapLimit();
        const utilization = (mem.heapUsed / limit) * 100;
        return {
            heapUsed: mem.heapUsed,
            heapTotal: mem.heapTotal,
            heapLimit: limit,
            utilization: +(utilization.toFixed(2)),
            rss: mem.rss,
            external: mem.external
        };
    }
}
