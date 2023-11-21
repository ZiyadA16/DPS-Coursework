# Data Processing Systems Coursework: Three-Way Joins

## Implementation
The algorithms for our three-way joins goes as follow:
```cpp
// Store buffered content of Table A into local variable 
Table table_ba;
for(auto i = 0u; i < buffer->size(); i++) {
    table_ba.emplace_back(buffer2->valueAtFast(i), buffer->valueAtFast(i));
}

// Perform hash-join on Table B and Table C
Table table_01;
table_01.reserve(input0.size() * input1.size());
hash_join(input0, input1, table_01);

// Perform heapsort on table A
heapify(table_ba);
heapsort(table_ba);

// Perform heapsort on table BC
heapify(table_01);
heapsort(table_01);

// Perform merge join on table BC
merge_join(table_ba, table_01, firstResultColumn, secondResultColumn);
```
We decided to perform a join between Table B and C, before joining with Table A. This join-order is more space efficient as Table C is the smaller table, and the intermediate join result between Table B and Table C will be smaller size than, say, the join result between Table A and Table B. 

The hash table is initialised with size about 4 times as big as the number of element in Table C. This parameter is tuned by trial and test to improve the benchmark score. We used linear probing in resolving hash collision. Since values is no longer unique(as in lecture note), the probe side of hash-join will need to be probed until the end of probe chain. 

We used heapsort for the sort-merge-join part, as heapsort allow us to sort the entries in table in-place without allocating additional spaces for sorting. Futhermore, heapsort has time complexity of `O(n log n)` in the worst case scenario, which is best in class for sorting algorithms. We decided to use iterative version of heapify and heapsort to avoid recursive call which might be more expensive. The functions are also inlined to prevent stalling the CPU pipeline.

Finally, we perform merge join between the sorted intermediate table and Table A. The algortihms of merge join is similar as in lecture note, except that we employ a nested-join-like-loop to handle non-unique value. The algorithms advance the entry's pointer of both table until the pointers point to entry with same value, and use a nested loop to join the entries with same value in both tables, before moving the pointer beyond those entries. 

## Results and Performance
The result of benchmark is as follow:
```
------------------------------------------------------------------
Benchmark                        Time             CPU   Iterations
------------------------------------------------------------------
ReferenceBenchmark/1024    1609132 ns       201684 ns         3487
ReferenceBenchmark/4096    6036019 ns       273966 ns         1000
ReferenceBenchmark/8192   18834500 ns       357060 ns         1000
MultiWayBenchmark/1024      909548 ns       298406 ns         3480
MultiWayBenchmark/4096     3369380 ns       466411 ns         1000
MultiWayBenchmark/8192    12570991 ns       700319 ns         1074
```
Using `MultiWayBenchmark/1024 ` as a baseline, the CPU time increases by about `1.5x` when there are 4 times as many entries, and by about `2.3x` when there are 8 times as many entries. This is likely because the larger number of entries requires the use larger hash table, which result in more cache fault when performing random access during hash-join, and more cache line to be scaned when performing the sort-merge-join.
