export declare class FastArray<T> {
    private items;
    private removedIndices;
    private _length;
    push(item: T): number;
    remove(predicate: (item: T, index: number) => boolean): T[];
    filter(predicate: (item: T, index: number) => boolean): T[];
    findIndex(predicate: (item: T, index: number) => boolean): number;
    private compact;
    toArray(): T[];
    get length(): number;
}
