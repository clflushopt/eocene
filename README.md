# Minimally Viable Query Engine with Interepretation & Runtime Code Generation

This is an implementation of a minimal query engine capable of executing
a subset of your usual SQL operators by following the Volcano model.

We implement both traditional interpreter and runtime code
generation based query execution.

For runtime code generation we actually compile to native code directly to x86
but the query execution can support any backend since we fix the code gen model
across all of them.


## Overview

`eocence` is a demonstration of how a simple query engine can be implemented
based on the iterator model (non-batched tuples).

In **interpretation** mode we currently support the following operators :

* Scan operator which is the starting point of the pipeline.
* Projection operator which selects specific columns from each row.
* Filter operator which runs predicates on rows returning only the ones that satisfy
  the predicate.
* Sort operator which returns rows in sorted order.
* Join operator which implements *Nested Loop Join*.
* Limit operator which sets a cut-off on the number of returned rows.

When the query execution mode is set to **runtime code generation** then only
queries that use scans, projections and filters are currently supported with
the rest left as an exercice to the reader.

## The Volcano Model

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

## Example

The code implements a small query engine with a SQL tokenizer and parser capable
of representing very simple queries, the AST can then be passed to the query engine
which will create a query plan in the form of a pipeline of operators before executing
them.

Below is the code in `main.rs` which runs some select queries.

``` rust

macro_rules! query {
    ($query_str:expr, $data:expr) => {{
        let tokenizer = Tokenizer::new($query_str);
        let q = Parser::new(tokenizer).parse();

        let mut executor = QueryExecutor {};
        let plan = executor.plan(q, $data);
        QueryExecutor::execute(plan)
    }};
}

fn main() {
    // Example data
    let data = vec![
        Row::new(&[
            "1".to_string(),
            "Alice".to_string(),
            "Manager".to_string(),
            "12000".to_string(),
        ]),
        Row::new(&[
            "2".to_string(),
            "Bob".to_string(),
            "Developer".to_string(),
            "10000".to_string(),
        ]),
        Row::new(&[
            "3".to_string(),
            "Charlie".to_string(),
            "Developer".to_string(),
            "9000".to_string(),
        ]),
        Row::new(&[
            "4".to_string(),
            "David".to_string(),
            "Analyst".to_string(),
            "11000".to_string(),
        ]),
        Row::new(&[
            "5".to_string(),
            "Eve".to_string(),
            "Manager".to_string(),
            "13000".to_string(),
        ]),
        Row::new(&[
            "6".to_string(),
            "Frank".to_string(),
            "Developer".to_string(),
            "9500".to_string(),
        ]),
        Row::new(&[
            "7".to_string(),
            "Grace".to_string(),
            "Analyst".to_string(),
            "10500".to_string(),
        ]),
        Row::new(&[
            "8".to_string(),
            "Hannah".to_string(),
            "Developer".to_string(),
            "9800".to_string(),
        ]),
        Row::new(&[
            "9".to_string(),
            "Ivy".to_string(),
            "Manager".to_string(),
            "12500".to_string(),
        ]),
        Row::new(&[
            "10".to_string(),
            "Jack".to_string(),
            "Analyst".to_string(),
            "10200".to_string(),
        ]),
    ];

    let queries = vec![
        (
            "SELECT id FROM example WHERE name = 'Ivy' LIMIT 1",
            vec![Row::new(&["9".to_string()])],
        ),
        (
            "SELECT name FROM employees WHERE role = 'Developer'",
            vec![
                Row::new(&["Bob".to_string()]),
                Row::new(&["Charlie".to_string()]),
                Row::new(&["Frank".to_string()]),
                Row::new(&["Hannah".to_string()]),
            ],
        ),
        (
            "SELECT id FROM employees WHERE salary > 9000 LIMIT 3",
            vec![
                Row::new(&["1".to_string()]),
                Row::new(&["2".to_string()]),
                Row::new(&["4".to_string()]),
            ],
        ),
    ];
    for query in queries {
        let results = query!(query.0, data.clone());
        let expected = query.1;
        assert_eq!(results, expected);
    }
}

```


# License

The code is under an [MIT License](LICENSE).
