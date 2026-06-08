export class FastArray {
    // Internal storage for all pushed items. Removed items remain here until compacted.
    items = [];
    // Indices of items that have been logically removed and should be ignored.
    removedIndices = new Set();
    // Logical count of active items excluding removed entries.
    _length = 0;
    // Add an item to the array and return the new logical length.
    // The item is appended to the underlying buffer, and logical length
    // tracks the number of active entries.
    push(item) {
        this.items.push(item);
        this._length++;
        return this._length;
    }
    // Remove items matching the predicate and return removed elements.
    // Matching entries are marked as logically deleted but remain in the
    // internal buffer until a compact operation runs.
    remove(predicate) {
        const removed = [];
        for (let i = 0; i < this.items.length; i++) {
            if (!this.removedIndices.has(i) && predicate(this.items[i], i)) {
                this.removedIndices.add(i);
                removed.push(this.items[i]);
                this._length--;
            }
        }
        // Compact the buffer when the ratio of removed entries becomes high.
        if (this.removedIndices.size > this.items.length * 0.3) {
            this.compact();
        }
        return removed;
    }
    // Return active items that satisfy the predicate.
    // Skips entries that have been logically removed.
    filter(predicate) {
        const result = [];
        for (let i = 0; i < this.items.length; i++) {
            if (!this.removedIndices.has(i) && predicate(this.items[i], i)) {
                result.push(this.items[i]);
            }
        }
        return result;
    }
    // Find the first active index where the predicate matches.
    // Returns the internal index or -1 if no active item matches.
    findIndex(predicate) {
        for (let i = 0; i < this.items.length; i++) {
            if (!this.removedIndices.has(i) && predicate(this.items[i], i)) {
                return i;
            }
        }
        return -1;
    }
    compact() {
        const newItems = [];
        for (let i = 0; i < this.items.length; i++) {
            if (!this.removedIndices.has(i)) {
                newItems.push(this.items[i]);
            }
        }
        this.items = newItems;
        this.removedIndices.clear();
    }
    toArray() {
        return this.filter(() => true);
    }
    get length() {
        return this._length;
    }
}
