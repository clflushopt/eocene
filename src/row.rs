//! Implementation of in-memory generic and simple rows, represented as `Vec<String>`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub items: Vec<String>,
}

impl Row {
    /// Create a new row from a slice of strings.
    pub fn new(items: &[String]) -> Self {
        Self {
            items: items.to_vec(),
        }
    }

    /// Returns item at given index.
    pub fn get(&self, index: usize) -> Option<&str> {
        self.items.get(index).map(|x| x.as_str())
    }
}

/// Rows that only hold `i64` tuple, because dealing with `String` in inline
/// assembly might be a pita we just assume all columns are numbers, certainly
/// all important databases grown up use have `i64`. You woulnd't put a `f64`
/// in there would you now ?
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Int64Row {
    pub items: Vec<i64>,
}

impl Int64Row {
    /// Create a new row from a slice of `i64`.
    pub fn new(items: &[i64]) -> Self {
        Self {
            items: items.to_vec(),
        }
    }

    /// Return item at given index without bound checks.
    pub fn get(&self, index: usize) -> i64 {
        *self.items.get(index).unwrap()
    }
}
