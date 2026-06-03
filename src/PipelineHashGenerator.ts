import { createHash } from 'node:crypto';

/**
 * Fast pipeline hashing using JSON.stringify
 * No DocumentSerializer overhead - raw speed for hash generation
 */
type PipelineStage = Record<string, any>;

class PipelineHashGenerator {
  /**
   * Fast hash using direct JSON.stringify (no serialization overhead)
   * This is ONLY for generating cache keys - NOT for storage
   */
  public static generateHash(pipeline: PipelineStage[]): string {
    if (!pipeline || pipeline.length === 0) {
      return 'empty_pipeline';
    }

    try {
      // Direct JSON.stringify - fastest approach
      const pipelineString = JSON.stringify(pipeline);
      
      // Use SHA-256 for collision resistance (faster than you think)
      return createHash('sha256')
        .update(pipelineString, 'utf8')
        .digest('hex')
        .substring(0, 16);
    } catch (error) {
      // Fallback: simple concatenation
      return createHash('sha256')
        .update(String(pipeline), 'utf8')
        .digest('hex')
        .substring(0, 16);
    }
  }

  /**
   * Generate hash with field filtering for cache invalidation
   */
  public static generateHashWithFilter(
    pipeline: PipelineStage[], 
    options: {
      ignoreFields?: string[];
      ignoreStages?: string[];
      onlyStages?: string[];
    } = {}
  ): string {
    const filteredPipeline = this.filterPipeline(pipeline, options);
    return this.generateHash(filteredPipeline);
  }

  /**
   * Filter pipeline based on options
   */
  private static filterPipeline(
    pipeline: PipelineStage[], 
    options: {
      ignoreFields?: string[];
      ignoreStages?: string[];
      onlyStages?: string[];
    }
  ): PipelineStage[] {
    const { ignoreFields = [], ignoreStages = [], onlyStages = [] } = options;

    return pipeline
      .filter(stage => {
        const stageType = Object.keys(stage)[0];
        
        if (onlyStages.length > 0 && !onlyStages.includes(stageType)) {
          return false;
        }
        
        return !ignoreStages.includes(stageType);
      })
      .map(stage => {
        if (ignoreFields.length === 0) {
          return stage;
        }

        const filteredStage: PipelineStage = {};
        for (const [stageType, stageContent] of Object.entries(stage)) {
          if (typeof stageContent === 'object' && stageContent !== null) {
            filteredStage[stageType] = this.filterObject(stageContent, ignoreFields);
          } else {
            filteredStage[stageType] = stageContent;
          }
        }
        return filteredStage;
      });
  }

  /**
   * Recursively filter object fields
   */
  private static filterObject(obj: any, ignoreFields: string[]): any {
    if (Array.isArray(obj)) {
      return obj.map(item => 
        typeof item === 'object' && item !== null 
          ? this.filterObject(item, ignoreFields) 
          : item
      );
    }

    if (typeof obj === 'object' && obj !== null) {
      const filtered: any = {};
      for (const [key, value] of Object.entries(obj)) {
        if (!ignoreFields.includes(key)) {
          filtered[key] = typeof value === 'object' && value !== null 
            ? this.filterObject(value, ignoreFields) 
            : value;
        }
      }
      return filtered;
    }

    return obj;
  }

  /**
   * MD5 for maximum speed (still collision-resistant for cache keys)
   */
  public static generateFastHash(pipeline: PipelineStage[]): string {
    if (!pipeline || pipeline.length === 0) {
      return 'empty';
    }

    try {
      const pipelineString = JSON.stringify(pipeline);
      return createHash('md5')
        .update(pipelineString, 'utf8')
        .digest('hex')
        .substring(0, 12);
    } catch (error) {
      return createHash('md5')
        .update(String(pipeline), 'utf8')
        .digest('hex')
        .substring(0, 12);
    }
  }

  /**
   * Generate hash for specific stages only
   */
  public static generateStagesHash(pipeline: PipelineStage[], stageIndices: number[]): string {
    const selectedStages = pipeline.filter((_, index) => stageIndices.includes(index));
    return this.generateHash(selectedStages);
  }

  /**
   * Check if pipelines are equivalent
   */
  public static arePipelinesEquivalent(pipeline1: PipelineStage[], pipeline2: PipelineStage[]): boolean {
    if (pipeline1.length !== pipeline2.length) {
      return false;
    }

    return this.generateHash(pipeline1) === this.generateHash(pipeline2);
  }

  /**
   * Compare with field filtering
   */
  public static arePipelinesEquivalentWithFilter(
    pipeline1: PipelineStage[], 
    pipeline2: PipelineStage[],
    ignoreFields: string[] = ['skip', 'limit', 'sort']
  ): boolean {
    const hash1 = this.generateHashWithFilter(pipeline1, { ignoreFields });
    const hash2 = this.generateHashWithFilter(pipeline2, { ignoreFields });
    
    return hash1 === hash2;
  }

  /**
   * Pipeline statistics
   */
  public static getPipelineStats(pipeline: PipelineStage[]): {
    stageCount: number;
    stageTypes: string[];
    estimatedSize: number;
    hash: string;
    fastHash: string;
  } {
    const stageTypes = pipeline.map(stage => Object.keys(stage)[0]);
    const estimatedSize = JSON.stringify(pipeline).length;
    const hash = this.generateHash(pipeline);
    const fastHash = this.generateFastHash(pipeline);

    return {
      stageCount: pipeline.length,
      stageTypes,
      estimatedSize,
      hash,
      fastHash
    };
  }

  /**
   * Generate complete cache key for pipelines
   */
  public static generateCacheKey(modelName: string, pipeline: PipelineStage[], options?: any): string {
    const pipelineHash = this.generateFastHash(pipeline); // Use fast hash
    const parts = [modelName, 'agg', pipelineHash];

    if (options) {
      const optionKeys = ['session', 'collation', 'comment', 'allowDiskUse'];
      const relevantOptions: any = {};
      
      for (const key of optionKeys) {
        if (options[key] !== undefined) {
          relevantOptions[key] = options[key];
        }
      }
      
      if (Object.keys(relevantOptions).length > 0) {
        const optionsHash = createHash('md5')
          .update(JSON.stringify(relevantOptions), 'utf8')
          .digest('hex')
          .substring(0, 6);
        parts.push(optionsHash);
      }
    }

    return parts.join(':');
  }

  /**
   * Batch hash generation for multiple pipelines
   */
  public static generateBatchHashes(pipelines: PipelineStage[][]): string[] {
    return pipelines.map(pipeline => this.generateFastHash(pipeline));
  }
}

export default PipelineHashGenerator;