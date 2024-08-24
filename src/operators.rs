//! Implementation of filter, project and scan operators.
use crate::row::Row;

/// The operator trait describes the interface Volcano style operators must
/// implement.
pub trait Operator {
    // Open the iterator for consumption.
    fn open(&mut self);
    // Next returns the next row if one is available otherwise `None`.
    fn next(&mut self) -> Option<Row>;
    // Close the iterator signaling we won't be consuming from it anymore.
    fn close(&self);
}

/// Projection operator returns the projected column from a row.
pub struct Project {
    input: Box<dyn Operator>,
    columns: Vec<usize>,
}

impl Project {
    // Create a new projection operator using an upstream operator and a list
    // of projected columns.
    pub fn new(operator: Box<dyn Operator>, columns: &[usize]) -> Self {
        Self {
            input: operator,
            columns: columns.to_vec(),
        }
    }
}

impl Operator for Project {
    fn open(&mut self) {}

    fn next(&mut self) -> Option<Row> {
        match self.input.next() {
            Some(row) => {
                let columns = self
                    .columns
                    .iter()
                    .filter_map(|&col| Some(row.get(col)?.to_string()))
                    .collect::<Vec<_>>();

                Some(Row::new(&columns))
            }
            None => None,
        }
    }

    fn close(&self) {}
}

/// Scan operator returns a batch of rows, scan is always the first operator
/// in the pipeline as such it is not a consumer.
pub struct Scan {
    rows: std::vec::IntoIter<Row>,
}

impl Scan {
    /// Create a new `Scan` operator over a batch of rows.
    pub fn new(rows: &[Row]) -> Self {
        Self {
            rows: rows.to_vec().into_iter(),
        }
    }
}

impl Operator for Scan {
    fn open(&mut self) {}

    fn next(&mut self) -> Option<Row> {
        self.rows.next()
    }

    fn close(&self) {}
}

/// Filter operator returns the next row that matches the predicate.
pub struct Filter<F>
where
    F: FnOnce(&Row) -> bool,
{
    input: Box<dyn Operator>,
    predicate: F,
}

impl<F> Filter<F>
where
    F: FnOnce(&Row) -> bool + 'static,
{
    /// Creates a new `Filter` operator with the given input upstream operator
    /// and predicate function.
    pub fn new(operator: Box<dyn Operator>, predicate: F) -> Self {
        Self {
            input: operator,
            predicate,
        }
    }
}

impl<F> Operator for Filter<F>
where
    F: Fn(&Row) -> bool + 'static,
{
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        while let Some(row) = self.input.next() {
            if (self.predicate)(&row) {
                return Some(row);
            }
        }
        None
    }

    fn close(&self) {
        self.input.close();
    }
}

/// Limit operator returns the next n-rows.
pub struct Limit {
    input: Box<dyn Operator>,
    limit: usize,
    count: usize,
}

impl Limit {
    /// Creates a new `Limit` operator with the given input operator and limit.
    pub fn new(operator: Box<dyn Operator>, limit: usize) -> Self {
        Self {
            input: operator,
            limit,
            count: 0,
        }
    }
}

impl Operator for Limit {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        if self.count < self.limit {
            if let Some(row) = self.input.next() {
                self.count += 1;
                return Some(row);
            }
        }
        None
    }

    fn close(&self) {
        self.input.close();
    }
}

/// Sort operator sorts the rows and returns them in sorted order.
pub struct Sort {
    sorted_rows: std::vec::IntoIter<Row>,
}

impl Sort {
    pub fn new<Compare: Fn(&Row, &Row) -> std::cmp::Ordering>(
        mut input: Box<dyn Operator>,
        cmp: Compare,
    ) -> Self {
        let mut rows: Vec<Row> = vec![];
        while let Some(row) = input.next() {
            rows.push(row);
        }
        rows.sort_by(&cmp);
        Self {
            sorted_rows: rows.into_iter(),
        }
    }
}

impl Operator for Sort {
    fn open(&mut self) {}

    fn next(&mut self) -> Option<Row> {
        self.sorted_rows.next()
    }

    fn close(&self) {}
}

/// The Join operator combines rows from two input operators based on a join condition.
pub struct Join {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    join_condition: Box<dyn Fn(&Row, &Row) -> bool>,
    left_rows: Vec<Row>,
    right_rows: Vec<Row>,
    left_index: usize,
    right_index: usize,
}

impl Join {
    /// Creates a new `Join` operator.
    pub fn new<F>(left: Box<dyn Operator>, right: Box<dyn Operator>, join_condition: F) -> Self
    where
        F: Fn(&Row, &Row) -> bool + 'static,
    {
        Self {
            left,
            right,
            join_condition: Box::new(join_condition),
            left_rows: vec![],
            right_rows: vec![],
            left_index: 0,
            right_index: 0,
        }
    }

    fn load_left_rows(&mut self) {
        while let Some(row) = self.left.next() {
            self.left_rows.push(row);
        }
    }

    fn load_right_rows(&mut self) {
        while let Some(row) = self.right.next() {
            self.right_rows.push(row);
        }
    }
}

impl Operator for Join {
    fn open(&mut self) {
        self.load_left_rows();
        self.load_right_rows();
        self.left_index = 0;
        self.right_index = 0;
    }

    fn next(&mut self) -> Option<Row> {
        while self.left_index < self.left_rows.len() {
            while self.right_index < self.right_rows.len() {
                let left_row = &self.left_rows[self.left_index];
                let right_row = &self.right_rows[self.right_index];

                if (self.join_condition)(left_row, right_row) {
                    // Create a combined row
                    let mut combined_row = left_row.clone();
                    combined_row.items.extend(right_row.items.clone());
                    self.right_index += 1;
                    return Some(combined_row);
                } else {
                    self.right_index += 1;
                }
            }
            self.right_index = 0;
            self.left_index += 1;
        }
        None
    }

    fn close(&self) {}
}

#[cfg(test)]
mod interface_tests {
    use super::*;

    #[test]
    fn scan() {
        let rows = vec![Row::new(&["1".to_string(), "Alice".to_string()])];
        let mut scan = Box::new(Scan::new(&rows));

        // Interface methods
        scan.open();
        assert!(scan.next().is_some());
        scan.close();
    }

    #[test]
    fn project() {
        let rows = vec![Row::new(&["1".to_string(), "Alice".to_string()])];
        let scan = Box::new(Scan::new(&rows));
        let mut project = Project::new(scan, &[1]);

        // Interface methods
        project.open();
        assert!(project.next().is_some());
        project.close();
    }

    #[test]
    fn filter() {
        let rows = vec![Row::new(&["1".to_string(), "Alice".to_string()])];
        let scan = Box::new(Scan::new(&rows));
        let filter_fn = Box::new(|row: &Row| row.get(0).unwrap() == "1");
        let mut filter = Box::new(Filter::new(scan, filter_fn));

        // Interface methods
        filter.open();
        assert!(filter.next().is_some());
        filter.close();
    }

    #[test]
    fn limit() {
        let rows = vec![Row::new(&["1".to_string(), "Alice".to_string()])];
        let scan = Box::new(Scan::new(&rows));
        let mut limit = Limit::new(scan, 1);

        limit.open();
        assert!(limit.next().is_some());
        limit.close();
    }

    #[test]
    fn sort() {
        let rows = vec![Row::new(&["1".to_string(), "Alice".to_string()])];
        let scan = Box::new(Scan::new(&rows));
        let mut sort = Sort::new(scan, |a, b| a.get(0).cmp(&b.get(0)));

        sort.open();
        assert!(sort.next().is_some());
        sort.close();
    }

    #[test]
    fn join() {
        let left_rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string()]),
        ];
        let right_rows = vec![
            Row::new(&["1".to_string(), "A".to_string()]),
            Row::new(&["2".to_string(), "B".to_string()]),
        ];

        let left = Box::new(Scan::new(&left_rows));
        let right = Box::new(Scan::new(&right_rows));

        // Define a simple join condition
        let join_condition = |left: &Row, right: &Row| left.get(0) == right.get(0);

        // Create the join operator
        let mut join = Join::new(left, right, join_condition);

        // Open the operators
        join.open();

        // Check that the operator conforms to the interface
        assert!(
            join.next().is_some(),
            "Join operator should return some rows"
        );
        assert!(
            join.next().is_some(),
            "Join operator should return some rows"
        );
        assert!(
            join.next().is_none(),
            "Join operator should return no more rows"
        );

        // Close the operators
        join.close();
    }
}

#[cfg(test)]
mod operator_tests {
    use super::*;

    #[test]
    fn scan_operator_returns_all_rows() {
        let rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string()]),
        ];
        let mut scan = Box::new(Scan::new(&rows));

        let mut result = vec![];
        while let Some(row) = scan.next() {
            result.push(row);
        }

        assert_eq!(result, rows);
    }

    #[test]
    fn project_operator_returns_projected_columns() {
        let rows = vec![Row::new(&[
            "1".to_string(),
            "Alice".to_string(),
            "Engineer".to_string(),
        ])];
        let scan = Box::new(Scan::new(&rows));
        let mut project = Project::new(scan, &[1, 2]);

        let projected_row = project.next().unwrap();

        assert_eq!(projected_row.get(0).unwrap(), "Alice");
        assert_eq!(projected_row.get(1).unwrap(), "Engineer");
        assert!(project.next().is_none());
    }

    #[test]
    fn filter_operator_returns_rows_that_match() {
        let rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string()]),
        ];
        let scan = Box::new(Scan::new(&rows));
        let filter_fn = Box::new(|row: &Row| row.get(0).unwrap() == "1");
        let mut filter = Box::new(Filter::new(scan, filter_fn));

        let mut result = vec![];
        while let Some(row) = filter.next() {
            result.push(row);
        }

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0).unwrap(), "1");
        assert_eq!(result[0].get(1).unwrap(), "Alice");
    }

    #[test]
    fn limit_operator_returns_limited_rows() {
        let rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string()]),
        ];
        let scan = Box::new(Scan::new(&rows));
        let mut limit = Limit::new(scan, 1);

        let mut result = vec![];
        while let Some(row) = limit.next() {
            result.push(row);
        }

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0).unwrap(), "1");
        assert_eq!(result[0].get(1).unwrap(), "Alice");
    }

    #[test]
    fn sort_operator_returns_sorted_rows() {
        let rows = vec![
            Row::new(&["9".to_string(), "Larry".to_string()]),
            Row::new(&["8".to_string(), "Peter".to_string()]),
            Row::new(&["7".to_string(), "Ted".to_string()]),
            Row::new(&["6".to_string(), "Carol".to_string()]),
            Row::new(&["5".to_string(), "Daniel".to_string()]),
            Row::new(&["4".to_string(), "Mallory".to_string()]),
            Row::new(&["3".to_string(), "Eve".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string()]),
            Row::new(&["1".to_string(), "Alice".to_string()]),
        ];

        // Sort by the first column (identifier)
        let scan = Box::new(Scan::new(&rows));
        let mut sort_by_id = Sort::new(scan, |a, b| a.get(0).cmp(&b.get(0)));

        let mut result_by_id = vec![];
        while let Some(row) = sort_by_id.next() {
            result_by_id.push(row);
        }

        // Expected sorted order by identifier
        let expected_order_by_id = vec!["1", "2", "3", "4", "5", "6", "7", "8", "9"];

        assert_eq!(
            result_by_id
                .iter()
                .map(|r| r.get(0).unwrap())
                .collect::<Vec<&str>>(),
            expected_order_by_id
        );

        // Sort by the second column (name)
        let scan = Box::new(Scan::new(&rows));
        let mut sort_by_name = Sort::new(scan, |a, b| a.get(1).cmp(&b.get(1)));

        let mut result_by_name = vec![];
        while let Some(row) = sort_by_name.next() {
            result_by_name.push(row);
        }

        // Expected sorted order by name
        let expected_order_by_name = vec![
            "Alice", "Bob", "Carol", "Daniel", "Eve", "Larry", "Mallory", "Peter", "Ted",
        ];

        assert_eq!(
            result_by_name
                .iter()
                .map(|r| r.get(1).unwrap())
                .collect::<Vec<&str>>(),
            expected_order_by_name
        );
    }

    #[test]
    fn join_operator_returns_joined_rows() {
        let left_rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string()]),
        ];
        let right_rows = vec![
            Row::new(&["1".to_string(), "11000".to_string()]),
            Row::new(&["2".to_string(), "24000".to_string()]),
        ];

        let scan_left = Box::new(Scan::new(&left_rows));
        let scan_right = Box::new(Scan::new(&right_rows));

        let join_condition = Box::new(|left: &Row, right: &Row| left.get(0) == right.get(0));
        let mut join = Join::new(scan_left, scan_right, join_condition);

        join.open();

        let mut results = vec![];
        while let Some(row) = join.next() {
            results.push(row);
        }

        let expected = vec![
            Row::new(&[
                "1".to_string(),
                "Alice".to_string(),
                "1".to_string(),
                "11000".to_string(),
            ]),
            Row::new(&[
                "2".to_string(),
                "Bob".to_string(),
                "2".to_string(),
                "24000".to_string(),
            ]),
        ];

        assert_eq!(results, expected);
    }
}

#[cfg(test)]
mod chaining_tests {
    use super::*;

    #[test]
    fn scan_and_project_operators_chain_correctly() {
        let rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string(), "Engineer".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string(), "Manager".to_string()]),
        ];
        let scan = Box::new(Scan::new(&rows));
        let mut project = Project::new(scan, &[1]);

        let mut result = vec![];
        while let Some(row) = project.next() {
            result.push(row);
        }

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0).unwrap(), "Alice");
        assert_eq!(result[1].get(0).unwrap(), "Bob");
    }

    #[test]
    fn filter_and_project_operators_chain_correctly() {
        let rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string(), "Engineer".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string(), "Manager".to_string()]),
        ];
        let scan = Box::new(Scan::new(&rows));
        let filter_fn = Box::new(|row: &Row| row.get(0).unwrap() == "1");
        let filter = Box::new(Filter::new(scan, filter_fn));
        let mut project = Project::new(filter, &[1]);

        let mut result = vec![];
        while let Some(row) = project.next() {
            result.push(row);
        }

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0).unwrap(), "Alice");
    }

    #[test]
    fn limit_and_sort_operators_chain_correctly() {
        let rows = vec![
            Row::new(&["2".to_string(), "Bob".to_string()]),
            Row::new(&["1".to_string(), "Alice".to_string()]),
            Row::new(&["3".to_string(), "Carol".to_string()]),
        ];
        let scan = Box::new(Scan::new(&rows));
        let sort = Box::new(Sort::new(scan, |a, b| a.get(0).cmp(&b.get(0))));
        let mut limit = Limit::new(sort, 2);

        let mut result = vec![];
        while let Some(row) = limit.next() {
            result.push(row);
        }

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0).unwrap(), "1");
        assert_eq!(result[0].get(1).unwrap(), "Alice");
        assert_eq!(result[1].get(0).unwrap(), "2");
        assert_eq!(result[1].get(1).unwrap(), "Bob");
    }

    #[test]
    fn most_operators_chain_correctly() {
        let employee_rows = vec![
            Row::new(&["1".to_string(), "Alice".to_string(), "Manager".to_string()]),
            Row::new(&["2".to_string(), "Bob".to_string(), "Engineer".to_string()]),
            Row::new(&[
                "3".to_string(),
                "Charlie".to_string(),
                "Manager".to_string(),
            ]),
            Row::new(&["4".to_string(), "David".to_string(), "Analyst".to_string()]),
            Row::new(&["5".to_string(), "Eve".to_string(), "Manager".to_string()]),
        ];

        // 1. Scan operator
        let scan = Box::new(Scan::new(&employee_rows));

        // 2. Filter operator to keep only "Manager"
        let filter_condition = |row: &Row| row.get(2) == Some("Manager");
        let filter = Box::new(Filter::new(scan, filter_condition));

        // 3. Sort operator to sort by ID (assuming the ID is in the first column)
        let sort = Sort::new(filter, |a, b| a.get(0).cmp(&b.get(0)));

        // 4. Project operator to project only the Name column (assuming the Name is in the second column)
        let mut project = Project::new(Box::new(sort), &[1]);

        // Open the operators
        project.open();

        // Collect results
        let mut results = vec![];
        while let Some(row) = project.next() {
            results.push(row);
        }

        // Close the operators
        project.close();

        // Expected output
        let expected_results = vec![
            Row::new(&["Alice".to_string()]),
            Row::new(&["Charlie".to_string()]),
            Row::new(&["Eve".to_string()]),
        ];

        // Assert the results are as expected
        assert_eq!(results, expected_results);
    }
}
