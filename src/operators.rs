//! Implementation of filter, project and scan operators.

use crate::row::Row;

/// The operator trait describes the interface Volcano style operators must
/// implement.
pub trait Operator {
    // Open the iterator for consumption.
    fn open(&self);
    // Next returns the next row if one is available otherwise `None`.
    fn next(&mut self) -> Option<Row>;
    // Close the iterator signaling we won't be consuming from it anymore.
    fn close(&self);
}

/// Projection operator returns the projected column from a row.
struct Project<'a> {
    input: &'a mut dyn Operator,
    columns: Vec<usize>,
}

impl<'a> Project<'a> {
    // Create a new projection operator using an upstream operator and a list
    // of projected columns.
    pub fn new<O: Operator>(operator: &'a mut O, columns: &[usize]) -> Self {
        Self {
            input: operator,
            columns: columns.to_vec(),
        }
    }
}

impl<'a> Operator for Project<'a> {
    fn open(&self) {}

    fn next(&mut self) -> Option<Row> {
        match self.input.next() {
            Some(row) => {
                let mut columns = vec![];

                for column in &self.columns {
                    columns.push(row.get(*column).unwrap().to_string());
                }

                Some(Row::new(columns.as_ref()))
            }
            None => None,
        }
    }

    fn close(&self) {}
}

/// Scan operator returns a batch of rows, scan is always the first operator
/// in the pipeline as such it is not a consumer.
///
/// We could implement `Scan` by holding just an iterator on the `rows` `Vec`
/// itself but that would introduce lifetime issues that would require a slightly
/// smarter approach so we just keep an index.
struct Scan {
    rows: Vec<Row>,
    // Index to the next row.
    index: usize,
}

impl Scan {
    /// Create a new `Scan` operator over a batch of rows.
    pub fn new(rows: &[Row]) -> Self {
        Self {
            rows: rows.to_vec(),
            index: 0,
        }
    }
}

impl Operator for Scan {
    fn open(&self) {}

    fn next(&mut self) -> Option<Row> {
        let next = self.rows.get(self.index);
        self.index += 1;
        next.cloned()
    }

    fn close(&self) {}
}

#[cfg(test)]

mod tests {
    use std::borrow::BorrowMut;

    use super::*;

    #[test]
    fn can_build_scan_operator() {
        let row: Vec<String> = vec![
            "1".to_string(),
            "Alice".to_string(),
            "12000".to_string(),
            "dollars".to_string(),
        ];
        let rows = vec![Row::new(row.as_slice())];
        let mut scan = Scan::new(rows.as_slice());
        let mut project = Project::new(scan.borrow_mut(), vec![1].as_slice());

        let item = project.next();

        assert!(item.is_some_and(|item| item.get(0).unwrap() == "Alice"));
    }
}
