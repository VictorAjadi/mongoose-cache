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
export class HyperHashMap<K, V> {
    private keys: (K | undefined)[]; // Sparse array storing keys
    private values: (V | undefined)[]; // Sparse array storing values
    private distances: Uint8Array; // Probe distance for each slot (0-255)
    private capacity: number; // Current hash table size (always power of 2)
    private size_: number = 0; // Number of stored key-value pairs
    private mask: number; // Bitmask for fast modulo (capacity - 1)
    private maxDistance: number = 0; // Maximum probe distance seen (for search optimization)
    private readonly loadFactor: number = 0.75; // Threshold for triggering resize

    /**
     * Initializes a new HyperHashMap with specified or default capacity.
     * @param initialCapacity - Initial capacity (rounded up to nearest power of 2), defaults to 32768
     */
    constructor(initialCapacity: number = 32768) {
        this.capacity = this.nextPowerOfTwo(initialCapacity);
        this.mask = this.capacity - 1;
        this.keys = new Array(this.capacity);
        this.values = new Array(this.capacity);
        this.distances = new Uint8Array(this.capacity);
    }

    /**
     * Rounds a number up to the nearest power of two.
     * @param n - Number to round
     * @returns Nearest power of two >= n
     */
    private nextPowerOfTwo(n: number): number {
        if (n <= 0) return 1;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        return n + 1;
    }

    /**
     * Fast hash function optimized for different key types.
     * Uses FNV-1a for strings and a Murmur3-like algorithm for numbers.
     * @param key - Key to hash
     * @returns 32-bit unsigned hash value
     */
    private fastHash(key: K): number {
        if (typeof key === 'string') {
            // FNV-1a hash for strings
            let hash = 0x811c9dc5;
            for (let i = 0; i < key.length; i++) {
                hash ^= key.charCodeAt(i);
                hash = Math.imul(hash, 0x01000193);
            }
            return hash >>> 0;
        }

        if (typeof key === 'number') {
            // Murmur3-like mixing for numbers
            let hash = key | 0;
            hash = Math.imul(hash ^ (hash >>> 16), 0x85ebca6b);
            hash = Math.imul(hash ^ (hash >>> 13), 0xc2b2ae35);
            return (hash ^ (hash >>> 16)) >>> 0;
        }

        // Fallback: convert to string and hash
        const str = String(key);
        let hash = 0x811c9dc5;
        for (let i = 0; i < str.length; i++) {
            hash ^= str.charCodeAt(i);
            hash = Math.imul(hash, 0x01000193);
        }
        return hash >>> 0;
    }

    /**
     * Sets a key-value pair in the map using Robin Hood hashing.
     * Automatically resizes if load factor exceeded or probe distance > 255.
     * @param key - Key to set
     * @param value - Value to associate with key
     */
    public set(key: K, value: V): void {
        // Resize if load factor exceeded
        if (this.size_ >= this.capacity * this.loadFactor) {
            this.resize();
        }

        const hash = this.fastHash(key);
        let index = hash & this.mask;
        let distance = 0;
        let insertKey = key;
        let insertValue = value;

        while (true) {
            // Found empty slot: insert here
            if (this.keys[index] === undefined) {
                this.keys[index] = insertKey;
                this.values[index] = insertValue;
                this.distances[index] = distance;
                this.size_++;
                this.maxDistance = Math.max(this.maxDistance, distance);
                return;
            }

            // Found exact key match: update value
            if (this.keysEqual(this.keys[index], key)) {
                this.values[index] = value;
                return;
            }

            // Robin Hood hashing: steal slot if current item has shorter distance
            if (distance > this.distances[index]) {
                [insertKey, this.keys[index]] = [this.keys[index] as K, insertKey];
                [insertValue, this.values[index]] = [this.values[index] as V, insertValue];
                [distance, this.distances[index]] = [this.distances[index], distance];
            }

            // Probe to next slot
            index = (index + 1) & this.mask;
            distance++;

            // Probe distance too large: resize and retry
            if (distance > 255) {
                this.resize();
                this.set(key, value);
                return;
            }
        }
    }

    /**
     * Retrieves the value associated with a key.
     * Uses probe distance to enable early termination when searching.
     * @param key - Key to look up
     * @returns Associated value or undefined if not found
     */
    public get(key: K): V | undefined {
        const hash = this.fastHash(key);
        let index = hash & this.mask;
        let distance = 0;

        // Search while within max distance and slots are occupied
        while (distance <= this.maxDistance && this.keys[index] !== undefined) {
            if (this.keysEqual(this.keys[index], key)) {
                return this.values[index];
            }
            // Early termination: if current item has shorter distance, key not found
            if (distance > this.distances[index]) break;
            index = (index + 1) & this.mask;
            distance++;
        }

        return undefined;
    }

    /**
     * Checks if a key exists in the map.
     * @param key - Key to check
     * @returns True if key exists, false otherwise
     */
    public has(key: K): boolean {
        return this.get(key) !== undefined;
    }

    /**
     * Deletes a key-value pair and performs backward shift to maintain Robin Hood properties.
     * @param key - Key to delete
     * @returns True if deleted, false if not found
     */
    public delete(key: K): boolean {
        const hash = this.fastHash(key);
        let index = hash & this.mask;
        let distance = 0;

        while (distance <= this.maxDistance && this.keys[index] !== undefined) {
            if (this.keysEqual(this.keys[index], key)) {
                // Clear the slot
                this.keys[index] = undefined;
                this.values[index] = undefined;
                this.distances[index] = 0;
                this.size_--;

                // Shift back elements to maintain Robin Hood invariants
                let nextIndex = (index + 1) & this.mask;
                while (this.keys[nextIndex] !== undefined && this.distances[nextIndex] > 0) {
                    const prevIndex = (nextIndex - 1 + this.capacity) & this.mask;
                    this.keys[prevIndex] = this.keys[nextIndex];
                    this.values[prevIndex] = this.values[nextIndex];
                    this.distances[prevIndex] = this.distances[nextIndex] - 1;

                    this.keys[nextIndex] = undefined;
                    this.values[nextIndex] = undefined;
                    this.distances[nextIndex] = 0;

                    nextIndex = (nextIndex + 1) & this.mask;
                }

                return true;
            }
            if (distance > this.distances[index]) break;
            index = (index + 1) & this.mask;
            distance++;
        }

        return false;
    }

    /**
     * Resizes the hash table to double capacity and rehashes all entries.
     * Called when load factor exceeded or probe distance limit reached.
     */
    private resize(): void {
        const oldKeys = this.keys;
        const oldValues = this.values;
        const oldCapacity = this.capacity;

        // Initialize new larger table
        this.capacity *= 2;
        this.mask = this.capacity - 1;
        this.keys = new Array(this.capacity);
        this.values = new Array(this.capacity);
        this.distances = new Uint8Array(this.capacity);
        this.size_ = 0;
        this.maxDistance = 0;

        // Rehash all existing entries
        for (let i = 0; i < oldCapacity; i++) {
            if (oldKeys[i] !== undefined) {
                this.set(oldKeys[i]!, oldValues[i]!);
            }
        }
    }

    /**
     * Clears all entries from the map.
     */
    public clear(): void {
        this.keys.fill(undefined);
        this.values.fill(undefined);
        this.distances.fill(0);
        this.size_ = 0;
        this.maxDistance = 0;
    }

    /**
     * Returns an iterator of [key, value] pairs.
     */
    public *entries(): Generator<[K, V]> {
        for (let i = 0; i < this.capacity; i++) {
            if (this.keys[i] !== undefined) {
                yield [this.keys[i]!, this.values[i]!];
            }
        }
    }

    /**
     * Returns an iterator of keys.
     */
    public *keys_iter(): Generator<K> {
        for (let i = 0; i < this.capacity; i++) {
            if (this.keys[i] !== undefined) {
                yield this.keys[i]!;
            }
        }
    }

    /**
     * Returns an iterator of values.
     */
    public *values_iter(): Generator<V> {
        for (let i = 0; i < this.capacity; i++) {
            if (this.keys[i] !== undefined) {
                yield this.values[i]!;
            }
        }
    }

    /**
     * Iterates over all entries with a callback function.
     * @param callback - Function called with (value, key, map) for each entry
     */
    public forEach(callback: (value: V, key: K, map: HyperHashMap<K, V>) => void): void {
        for (let i = 0; i < this.capacity; i++) {
            if (this.keys[i] !== undefined) {
                callback(this.values[i]!, this.keys[i]!, this);
            }
        }
    }

    /**
     * Compares two keys for equality, handling objects by structure.
     * @param a - First key (may be undefined)
     * @param b - Second key
     * @returns True if keys are equal
     */
    private keysEqual(a: K | undefined, b: K): boolean {
        // Reference equality
        if (a === b) return true;
        if (a === undefined) return false;
        
        // Object comparison by structure
        if (typeof a === 'object' && a !== null && typeof b === 'object' && b !== null) {
            if (a.constructor !== b.constructor) return false;
            return JSON.stringify(a) === JSON.stringify(b);
        }
        
        return false;
    }

    /**
     * Gets the number of stored entries.
     */
    public get size(): number {
        return this.size_;
    }

    /**
     * Gets the current load factor (size / capacity).
     */
    public get loadFactor_(): number {
        return this.size_ / this.capacity;
    }

    /**
     * Gets the current capacity of the hash table.
     */
    public getCapacity(): number {
        return this.capacity;
    }

    /**
     * Gets the maximum probe distance encountered.
     */
    public getMaxDistance(): number {
        return this.maxDistance;
    }

    /**
     * Returns detailed statistics about the hash map state.
     * Useful for performance analysis and optimization.
     */
    public getStats(): {
        size: number;
        capacity: number;
        loadFactor: number;
        maxDistance: number;
        averageProbeLength: number;
    } {
        let totalProbeLength = 0;
        let occupiedSlots = 0;

        for (let i = 0; i < this.capacity; i++) {
            if (this.keys[i] !== undefined) {
                totalProbeLength += this.distances[i];
                occupiedSlots++;
            }
        }

        return {
            size: this.size_,
            capacity: this.capacity,
            loadFactor: this.size_ / this.capacity,
            maxDistance: this.maxDistance,
            averageProbeLength: occupiedSlots > 0 ? totalProbeLength / occupiedSlots : 0
        };
    }
}