using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Fsion
{
    public class DictSlim<TKey, TValue> where TKey : IEquatable<TKey>
    {
        private static readonly int[] InitialBuckets = new int[1];
        private static readonly Entry[] InitialEntries = new Entry[1];
        private int _count;
        private int[] _buckets;
        private Entry[] _entries;

        private struct Entry
        {
            public TKey key;
            public TValue value;
            public int next;
        }

        public DictSlim()
        {
            _buckets = InitialBuckets;
            _entries = InitialEntries;
        }

        public DictSlim(int capacity)
        {
            _buckets = new int[capacity];
            _entries = new Entry[capacity];
        }

        public int Count => _count;

        public ref TValue GetOrAddValueRef(TKey key)
        {
            Entry[] entries = _entries;
            int bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
            for (int i = _buckets[bucketIndex] - 1;
                    (uint)i < (uint)entries.Length; i = entries[i].next)
            {
                if (key.Equals(entries[i].key))
                {
                    return ref entries[i].value;
                }
            }
            return ref AddKey(key, bucketIndex);
        }

        public bool GetOrAddValueRef2(TKey key, ref TValue value)
        {
            Entry[] entries = _entries;
            int bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
            for (int i = _buckets[bucketIndex] - 1;
                    (uint)i < (uint)entries.Length; i = entries[i].next)
            {
                if (key.Equals(entries[i].key))
                {
                    value = entries[i].value;
                    return false;
                }
            }
            value = AddKey(key, bucketIndex);
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private ref TValue AddKey(TKey key, int bucketIndex)
        {
            Entry[] entries = _entries;
            if (_count == entries.Length || entries.Length == 1)
            {
                entries = Resize();
                bucketIndex = key.GetHashCode() & (_buckets.Length - 1);
                // entry indexes were not changed by Resize
            }
            int entryIndex = _count;
            entries[entryIndex].key = key;
            entries[entryIndex].next = _buckets[bucketIndex] - 1;
            _buckets[bucketIndex] = entryIndex + 1;
            _count++;
            return ref entries[entryIndex].value;
        }

        private Entry[] Resize()
        {
            int count = _count;
            int newSize = _entries.Length * 2;
            var entries = new Entry[newSize];
            Array.Copy(_entries, 0, entries, 0, count);

            var newBuckets = new int[entries.Length];
            while (count-- > 0)
            {
                int bucketIndex = entries[count].key.GetHashCode() & (newBuckets.Length - 1);
                entries[count].next = newBuckets[bucketIndex] - 1;
                newBuckets[bucketIndex] = count + 1;
            }

            _buckets = newBuckets;
            _entries = entries;

            return entries;
        }
    }
}