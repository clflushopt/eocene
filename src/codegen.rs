//! Implementation of runtime code generation for query execution.

/// Index of the column in a row, used as an alias for the column name.
type ColumnIndex = usize;

/// Binary operations supported in `WHERE` clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOperator {
    Equal,
    GreaterThan,
    LesserThan,
}

/// As much as I would like to re-use `sql::Expr` I don't want to deal with
/// string based comparison, so let's focus on supporting just the subset we
/// care about, nice 64 bit signed integers.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expr {
    Column(ColumnIndex),
    Value(i64),
    Comparison(BinaryOperator, Box<Expr>, Box<Expr>),
}

impl Expr {
    /// Compile an expression to native code.
    fn compile(&self) {}
}

/// Operator defines the atoms used to represent query plans that are compiled
/// to native code.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Operator {
    Project(ColumnIndex),
    Scan,
    Filter(Expr),
}

/// Query plans in the codegen model are very similar to the interpreter based model.
///
/// Except that here our operators are not implemented but defined, the implementation
/// will be the code generated later at runtime.
struct QueryPlan {
    pipeline: Vec<Operator>,
}

impl QueryPlan {
    /// Create a new query plan.
    fn new() -> Self {
        Self { pipeline: vec![] }
    }

    /// Push a new operator to the plan, in the iterator model the pipeline does not
    /// create a DAG (I think) so this can be seen as a linear sequence of operators
    /// where downstream pulls from upstream.
    fn push(&mut self, operator: Operator) {
        self.pipeline.push(operator)
    }

    /// Compile the query plan to a native code buffer.
    fn compile(&self) {
        todo!("Unimplemented query compilation !")
    }
}

#[cfg(test)]
mod tests {
    use dynasmrt::{dynasm, DynasmApi, DynasmLabelApi, ExecutableBuffer};

    pub struct JitCompiler {
        assembler: dynasmrt::x64::Assembler,
    }

    impl JitCompiler {
        pub fn new() -> Self {
            JitCompiler {
                assembler: dynasmrt::x64::Assembler::new().unwrap(),
            }
        }

        pub fn compile_filter(&mut self) -> ExecutableBuffer {
            let entry_point = self.assembler.offset();

            dynasm!(self.assembler
                ; .arch x64
                // Prologue: setting up the stack frame
                ; push rbp
                ; mov rbp, rsp

                // Load the salary value from the row (Vec<i64>) into rax
                ; mov rax, QWORD [rdi + 3 * 8] // rdi holds the pointer to the Vec<i64>, [rdi + 3 * 8] is salary

                // Compare salary with 9000
                ; cmp rax, 9000
                // Jump to the `fail` label if the salary is not greater than 9000
                ; jle >fail

                // Success: return 1 (true)
                ; mov rax, 1
                ; jmp >end

                // Fail: return 0 (false)
                ; fail:
                ; mov rax, 0

                // Epilogue: restore stack frame and return
                ; end:
                ; mov rsp, rbp
                ; pop rbp
                ; ret
            );

            self.assembler.finalize().unwrap()
        }
    }

    fn main() {
        let mut compiler = JitCompiler::new();
        let filter_fn = compiler.compile_filter();

        let rows = vec![vec![1, 2, 3, 10000], vec![1, 2, 3, 8000]];

        let filter: fn(*const i64) -> i64 =
            unsafe { std::mem::transmute(filter_fn.ptr(entry_point)) };

        for row in &rows {
            let result = filter(row.as_ptr());
            println!("Row: {:?}, Passed: {}", row, result == 1);
        }
    }
}
