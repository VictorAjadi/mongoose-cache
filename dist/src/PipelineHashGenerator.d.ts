/**
 * Fast pipeline hashing using JSON.stringify
 * No DocumentSerializer overhead - raw speed for hash generation
 */
type PipelineStage = Record<string, any>;
declare class PipelineHashGenerator {
    /**
     * Fast hash using direct JSON.stringify (no serialization overhead)
     * This is ONLY for generating cache keys - NOT for storage
     */
    static generateHash(pipeline: PipelineStage[]): string;
    /**
     * Generate hash with field filtering for cache invalidation
     */
    static generateHashWithFilter(pipeline: PipelineStage[], options?: {
        ignoreFields?: string[];
        ignoreStages?: string[];
        onlyStages?: string[];
    }): string;
    /**
     * Filter pipeline based on options
     */
    private static filterPipeline;
    /**
     * Recursively filter object fields
     */
    private static filterObject;
    /**
     * MD5 for maximum speed (still collision-resistant for cache keys)
     */
    static generateFastHash(pipeline: PipelineStage[]): string;
    /**
     * Generate hash for specific stages only
     */
    static generateStagesHash(pipeline: PipelineStage[], stageIndices: number[]): string;
    /**
     * Check if pipelines are equivalent
     */
    static arePipelinesEquivalent(pipeline1: PipelineStage[], pipeline2: PipelineStage[]): boolean;
    /**
     * Compare with field filtering
     */
    static arePipelinesEquivalentWithFilter(pipeline1: PipelineStage[], pipeline2: PipelineStage[], ignoreFields?: string[]): boolean;
    /**
     * Pipeline statistics
     */
    static getPipelineStats(pipeline: PipelineStage[]): {
        stageCount: number;
        stageTypes: string[];
        estimatedSize: number;
        hash: string;
        fastHash: string;
    };
    /**
     * Generate complete cache key for pipelines
     */
    static generateCacheKey(modelName: string, pipeline: PipelineStage[], options?: any): string;
    /**
     * Batch hash generation for multiple pipelines
     */
    static generateBatchHashes(pipelines: PipelineStage[][]): string[];
}
export default PipelineHashGenerator;
