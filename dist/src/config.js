export const DEFAULT_CONFIG = {
    enabled: true,
    debug: false,
    maxKeys: 10000,
    ttl: 300,
    maxItemSizeMB: 10,
    enableSmartInvalidation: true,
    useCryptoHash: false,
    redisDropThreshold: 85,
    memoryDropThreshold: 80,
    memoryThreshold: 90,
    redis: {
        host: 'localhost',
        port: 6379,
        password: undefined,
        db: 0,
        keyPrefix: 'mongoose:cache:',
    },
};
