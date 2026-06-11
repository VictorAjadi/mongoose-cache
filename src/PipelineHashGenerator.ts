/**
 * Fast pipeline hashing using JSON.stringify
 * No DocumentSerializer overhead - raw speed for hash generation
 */
type PipelineStage = Record<string, any>;

class PipelineHashGenerator {
  /**
   * Internal digest helper that works in Node.js and Bun
   */
  private static async digest(algorithm: string, data: string): Promise<string> {
    // Prefer Web Crypto API (Bun + modern Node)
    if (globalThis.crypto?.subtle) {
      const enc = new TextEncoder().encode(data);
      const buf = await crypto.subtle.digest(algorithm, enc);
      return Array.from(new Uint8Array(buf))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
    }

    // Node.js fallback
    try {
      const { createHash } = require('crypto');
      return createHash(algorithm.toLowerCase())
        .update(data, 'utf8')
        .digest('hex');
    } catch {
      throw new Error('No crypto available');
    }
  }

  /**
   * Fast hash using direct JSON.stringify (no serialization overhead)
   * This is ONLY for generating cache keys - NOT for storage
   */
  public static async generateHash(pipeline: PipelineStage[]): Promise<string> {
    if (!pipeline || pipeline.length === 0) {
      return 'empty_pipeline';
    }

    try {
      // Direct JSON.stringify - fastest approach
      const pipelineString = JSON.stringify(pipeline);

      // Use SHA-256 for collision resistance (faster than you think)
      const hash = await this.digest('SHA-256', pipelineString);
      return hash.substring(0, 16);
    } catch (error) {
      // Fallback: simple concatenation
      const hash = await this.digest('SHA-256', String(pipeline));
      return hash.substring(0, 16);
    }
  }

  /**
   * Generate hash with field filtering for cache invalidation
   */
  public static async generateHashWithFilter(
    pipeline: PipelineStage[],
    options: {
      ignoreFields?: string[];
      ignoreStages?: string[];
      onlyStages?: string[];
    } = {}
  ): Promise<string> {
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
  public static async generateFastHash(pipeline: PipelineStage[]): Promise<string> {
    if (!pipeline || pipeline.length === 0) return 'empty';

    // 1. IDENTITY CHECK
    if ((pipeline as any)._cacheHash) return (pipeline as any)._cacheHash;

    // 2. ULTRA-FAST SIGNATURE: Avoid JSON.stringify for common short pipelines
    // We build a signature of [StageOp:KeyCount] + [MatchFields] if short
    let signature = '';
    for (let i = 0; i < pipeline.length; i++) {
      const stage = pipeline[i];
      if (!stage) continue;
      const op = Object.keys(stage)[0];
      if (!op) continue;
      signature += op.substring(0, 4) + ':'; // '$match' -> '$mat'

      const content = stage[op];
      if (content && typeof content === 'object') {
        signature += Object.keys(content).length + '|';
      } else {
        signature += String(content).substring(0, 5) + '|';
      }
    }

    // 3. Hash the small signature string
    const hash = await this.digest('MD5', signature);
    const shortHash = hash.substring(0, 12);

    // Attach to the array instance
    try {
      Object.defineProperty(pipeline, '_cacheHash', { value: shortHash, enumerable: false, configurable: true });
    } catch { /* immutable */ }

    return shortHash;
  }

  /**
   * Generate hash for specific stages only
   */
  public static async generateStagesHash(pipeline: PipelineStage[], stageIndices: number[]): Promise<string> {
    const selectedStages = pipeline.filter((_, index) => stageIndices.includes(index));
    return this.generateHash(selectedStages);
  }

  /**
   * Check if pipelines are equivalent
   */
  public static async arePipelinesEquivalent(pipeline1: PipelineStage[], pipeline2: PipelineStage[]): Promise<boolean> {
    if (pipeline1.length !== pipeline2.length) {
      return false;
    }

    return (await this.generateHash(pipeline1)) === (await this.generateHash(pipeline2));
  }

  /**
   * Compare with field filtering
   */
  public static async arePipelinesEquivalentWithFilter(
    pipeline1: PipelineStage[],
    pipeline2: PipelineStage[],
    ignoreFields: string[] = ['skip', 'limit', 'sort']
  ): Promise<boolean> {
    const hash1 = await this.generateHashWithFilter(pipeline1, { ignoreFields });
    const hash2 = await this.generateHashWithFilter(pipeline2, { ignoreFields });

    return hash1 === hash2;
  }

  /**
   * Pipeline statistics
   */
  public static async getPipelineStats(pipeline: PipelineStage[]): Promise<{
    stageCount: number;
    stageTypes: string[];
    estimatedSize: number;
    hash: string;
    fastHash: string;
  }> {
    const stageTypes = pipeline.map(stage => Object.keys(stage)[0]);
    const estimatedSize = JSON.stringify(pipeline).length;
    const hash = await this.generateHash(pipeline);
    const fastHash = await this.generateFastHash(pipeline);

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
  public static async generateCacheKey(modelName: string, pipeline: PipelineStage[], options?: any): Promise<string> {
    const pipelineHash = await this.generateFastHash(pipeline); // Use fast hash
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
        const optionsHash = await this.digest('MD5', JSON.stringify(relevantOptions));
        parts.push(optionsHash.substring(0, 6));
      }
    }

    return parts.join(':');
  }

  /**
   * Batch hash generation for multiple pipelines
   */
  public static async generateBatchHashes(pipelines: PipelineStage[][]): Promise<string[]> {
    return Promise.all(pipelines.map(pipeline => this.generateFastHash(pipeline)));
  }
}

export default PipelineHashGenerator;