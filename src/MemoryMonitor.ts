/**
 * MemoryMonitor - Unified utility for tracking heap utilization across Node.js and Bun
 */
export class MemoryMonitor {
    private static heapLimit: number = 0;
    private static v8: any = null;

    private static initV8() {
        if (this.v8 !== null) return;
        try {
            // Only works in Node.js
            this.v8 = require('node:v8');
        } catch {
            this.v8 = null;
        }
    }

    public static getHeapLimit(): number {
        if (this.heapLimit > 0) return this.heapLimit;

        try {
            this.initV8();

            // Node.js path
            if (this.v8 && typeof this.v8.getHeapStatistics === 'function') {
                const stats = this.v8.getHeapStatistics();
                this.heapLimit = stats.heap_size_limit;
                return this.heapLimit;
            }

            // Bun path
            if (typeof performance !== 'undefined' && (performance as any).memory) {
                this.heapLimit = (performance as any).memory.jsHeapSizeLimit;
                return this.heapLimit;
            }
        } catch { }

        // Universal fallback
        const mem = process.memoryUsage();
        this.heapLimit = mem.heapTotal || 1536 * 1024 * 1024;
        return this.heapLimit;
    }

    public static getHeapUtilization(): number {
        const limit = this.getHeapLimit();
        const used = process.memoryUsage().heapUsed;
        return (used / limit) * 100;
    }

    public static isUnderPressure(threshold: number): boolean {
        return this.getHeapUtilization() >= threshold;
    }

    public static getMemoryReport() {
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