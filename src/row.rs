//! Implementation of in-memory rows, represented as `Vec<String>`.

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
