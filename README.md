# Minimally viable query engine with the Volcano model

This is an implementation of a minimal query engine capable of executing
a subset of your usual SQL operators by following the Volcano model.

The Volcano model often also described as *the classical iterator model* 
initially described in [Volcano - An Extensible and Parallel Query Evaluation System](https://dl.acm.org/doi/10.1109/69.273032)
is a pipelined execution model that describes query execution as a pipeline
of pull based operators, where each operators *pulls* rows from its parent by
calling a `next() -> Row` method.
With this uniform interface for all operators Volcano effectively decouples
inputs from operators.

The core idea is described beautifully in the section `Query Processing` from
the original paper :

```
In Volcano, all algebra operators are implemented as iterators i.e. they support
a simple open-next-close protocol.

Basically, iterators provide the iteration component of a loop, i.e. initialization
increment, loop termination condition, and final housekeeping.
```

Adrian Colyer has a well written article that summarizes the key point of
the original paper in his blog [the morning paper](https://blog.acolyer.org/2015/02/11/encapsulation-of-parallelism-in-the-volcano-query-processing-system/).

The pull based, or iterator based model is not without issue, the cost of
a clean interface is performance. Neumann et al. argue in [Efficiently Compiling Efficient Query Plans
for Modern Hardware](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) that
the pull based model while simplifies analysis and execution implementation
comes at the cost of performance.

The case for mechanical sympathy can be seen in in the fact that when processing
millions of rows, each operator `pull` incurs a function call either via dynamic
dispatch or through a table using a function pointer which tend to compound when
you have millions of rows especially when it comes to branch mis-predictions.

# License

The code is under an [MIT License].
