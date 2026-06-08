/**
 * HyperHashMap: A high-performance hash map implementation using Robin Hood hashing.
 *
 * This data structure optimizes for fast lookups and reduced collision chains through:
 * - Robin Hood hashing: when inserting, steals slots from items with shorter probe distances
 * - Distance tracking: maintains probe distances to enable early termination during searches
 * - Automatic resizing: doubles capacity when load factor exceeds 75%
 * - Flexible key types: supports strings, numbers, and objects with custom equality
 *
 * @template K - Key type (string, number, or object)
 * @template V - Value type
 */
export declare class HyperHashMap<K, V> {
    private keys;
    private values;
    private distances;
    private capacity;
    private size_;
    private mask;
    private maxDistance;
    private readonly loadFactor;
    /**
     * Initializes a new HyperHashMap with specified or default capacity.
     * @param initialCapacity - Initial capacity (rounded up to nearest power of 2), defaults to 32768
     */
    constructor(initialCapacity?: number);
    /**
     * Rounds a number up to the nearest power of two.
     * @param n - Number to round
     * @returns Nearest power of two >= n
     */
    private nextPowerOfTwo;
    /**
     * Fast hash function optimized for different key types.
     * Uses FNV-1a for strings and a Murmur3-like algorithm for numbers.
     * @param key - Key to hash
     * @returns 32-bit unsigned hash value
     */
    private fastHash;
    /**
     * Sets a key-value pair in the map using Robin Hood hashing.
     * Automatically resizes if load factor exceeded or probe distance > 255.
     * @param key - Key to set
     * @param value - Value to associate with key
     */
    set(key: K, value: V): void;
    /**
     * Retrieves the value associated with a key.
     * Uses probe distance to enable early termination when searching.
     * @param key - Key to look up
     * @returns Associated value or undefined if not found
     */
    get(key: K): V | undefined;
    /**
     * Checks if a key exists in the map.
     * @param key - Key to check
     * @returns True if key exists, false otherwise
     */
    has(key: K): boolean;
    /**
     * Deletes a key-value pair and performs backward shift to maintain Robin Hood properties.
     * @param key - Key to delete
     * @returns True if deleted, false if not found
     */
    delete(key: K): boolean;
    /**
     * Resizes the hash table to double capacity and rehashes all entries.
     * Called when load factor exceeded or probe distance limit reached.
     */
    private resize;
    /**
     * Clears all entries from the map.
     */
    clear(): void;
    /**
     * Returns an iterator of [key, value] pairs.
     */
    entries(): Generator<[K, V]>;
    /**
     * Returns an iterator of keys.
     */
    keys_iter(): Generator<K>;
    /**
     * Returns an iterator of values.
     */
    values_iter(): Generator<V>;
    /**
     * Iterates over all entries with a callback function.
     * @param callback - Function called with (value, key, map) for each entry
     */
    forEach(callback: (value: V, key: K, map: HyperHashMap<K, V>) => void): void;
    /**
     * Compares two keys for equality, handling objects by structure.
     * @param a - First key (may be undefined)
     * @param b - Second key
     * @returns True if keys are equal
     */
    private keysEqual;
    /**
     * Gets the number of stored entries.
     */
    get size(): number;
    /**
     * Gets the current load factor (size / capacity).
     */
    get loadFactor_(): number;
    /**
     * Gets the current capacity of the hash table.
     */
    getCapacity(): number;
    /**
     * Gets the maximum probe distance encountered.
     */
    getMaxDistance(): number;
    /**
     * Returns detailed statistics about the hash map state.
     * Useful for performance analysis and optimization.
     */
    getStats(): {
        size: number;
        capacity: number;
        loadFactor: number;
        maxDistance: number;
        averageProbeLength: number;
    };
}
