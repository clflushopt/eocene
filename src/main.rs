use eocene::operators::{Filter, Limit, Operator, Project, Scan, Sort};
use eocene::row::Row;
use eocene::sql::{Expr, Parser, Query, Tokenizer};

type Comparator = Box<dyn Fn(&Row, &Row) -> std::cmp::Ordering>;

#[derive(Default)]
pub struct QueryExecutor {}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(mut pipeline: Box<dyn Operator>) -> Vec<Row> {
        pipeline.open();
        let mut results = Vec::new();
        while let Some(row) = pipeline.next() {
            results.push(row);
        }
        pipeline.close();
        results
    }
    /// Execute the input query on the given data, assuming a fixed schema.
    ///
    /// `id, name, role, salary`.
    pub fn plan(&mut self, query: Query, data: Vec<Row>) -> Box<dyn Operator> {
        // Start with the Scan operator
        let mut pipeline: Box<dyn Operator> = Box::new(Scan::new(&data));

        // Extract query details
        match query {
            Query::Select {
                columns,
                table: _,
                filter,
                order_by,
                limit,
            } => {
                // Apply the Filter operator if specified
                if let Some(expr) = filter {
                    let filter = move |row: &Row| Self::eval(expr.clone(), row);
                    pipeline = Box::new(Filter::new(pipeline, filter));
                }

                // Apply the Sort operator if specified
                if let Some(ref column) = order_by {
                    let column_index: usize = match column.as_str() {
                        "id" => 0,
                        "name" => 1,
                        "role" => 2,
                        "salary" => 3,
                        _ => unreachable!("expected column name to follow hardcoded schema"),
                    };
                    let sort_fn: Comparator =
                        Box::new(move |a, b| a.get(column_index).cmp(&b.get(column_index)));
                    pipeline = Box::new(Sort::new(pipeline, sort_fn));
                }

                // Apply the Limit operator if specified
                if let Some(limit) = limit {
                    pipeline = Box::new(Limit::new(pipeline, limit as usize));
                }

                // Apply the Project operator to select the desired columns
                let column_indices = columns
                    .iter()
                    .map(|col| match col.as_str() {
                        "id" => 0,
                        "name" => 1,
                        "role" => 2,
                        "salary" => 3,
                        _ => unreachable!("expected column name to follow hardcoded schema"),
                    })
                    .collect::<Vec<_>>();
                pipeline = Box::new(Project::new(pipeline, &column_indices));
            }
        }

        pipeline
    }

    fn resolve(expr: &Expr, row: &Row) -> String {
        match expr {
            Expr::Column(column) => match column.as_str() {
                "id" => row.get(0).unwrap().to_string(),
                "name" => row.get(1).unwrap().to_string(),
                "role" => row.get(2).unwrap().to_string(),
                "salary" => row.get(3).unwrap().to_string(),
                _ => unreachable!("expected column name to follow hardcoded schema got {column}"),
            },
            Expr::Value(value) => value.to_string(),
            Expr::Varchar(varchar) => varchar.clone(),
            _ => todo!("Unimplemented resolver for expression {:?}", expr),
        }
    }

    fn eval(expr: Expr, row: &Row) -> bool {
        match expr {
            // Not sure if this make sense for columns :/.
            Expr::Column(_) => true,
            Expr::Varchar(_) => true,
            Expr::Value(_) => true,
            Expr::And(left, right) => Self::eval(*left, row) && Self::eval(*right, row),
            Expr::Or(left, right) => Self::eval(*left, row) || Self::eval(*right, row),
            Expr::Comparison(left, op, right) => {
                let left_value = Self::resolve(&left, row);
                let right_value = Self::resolve(&right, row);
                match op.as_str() {
                    ">" => left_value.parse::<i64>().unwrap() > right_value.parse::<i64>().unwrap(),
                    "<" => left_value.parse::<i64>().unwrap() < right_value.parse::<i64>().unwrap(),
                    "=" => left_value == right_value,
                    _ => false,
                }
            }
            _ => todo!("Unimplemented evaluator for expression {:?}", expr),
        }
    }
}

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
        (
            "SELECT id FROM employees WHERE salary > 10000 ORDERBY name",
            vec![
                Row::new(&["1".to_string()]),
                Row::new(&["4".to_string()]),
                Row::new(&["5".to_string()]),
                Row::new(&["7".to_string()]),
                Row::new(&["9".to_string()]),
                Row::new(&["10".to_string()]),
            ],
        ),
    ];
    for query in queries {
        let results = query!(query.0, data.clone());
        let expected = query.1;
        assert_eq!(results, expected);
    }
}
