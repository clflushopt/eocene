//! Implementation of runtime code generation for query execution.
use crate::row::{self, Int64Row};
use dynasmrt::{aarch64::Assembler, dynasm, AssemblyOffset, DynasmApi, ExecutableBuffer};

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

#[derive(Debug)]
struct CompiledQueryPlan(AssemblyOffset, ExecutableBuffer);

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
}

struct QueryCompiler {
    plan: QueryPlan,
    rows: Vec<Int64Row>,
}

impl QueryCompiler {
    /// Create a new query compiler.
    fn new(plan: QueryPlan, rows: Vec<Int64Row>) -> Self {
        Self { plan, rows }
    }

    /// Compile the query plan and return a compiled query plan which is a tuple
    /// of an entry point and executable machine code.
    fn compile(&self) -> CompiledQueryPlan {
        // Create a new assembler.
        let mut assembler = dynasmrt::x64::Assembler::new().unwrap();
        let mut entry_point = assembler.offset();

        CompiledQueryPlan(entry_point, assembler.finalize().unwrap())
    }

    /// Compile the `scan` operator.
    fn scan(&mut self, assembler: &mut Assembler) {
        let entry_point = assembler.offset();

        // The rows are assumed to be packed, I guess.
        let row_data = self.rows.as_mut_ptr();
        let row_count = self.rows.len();

        // Scan is the entry point of the pipeline, which means all downstream
        // operators end up calling it.
        dynasm!(assembler
            ; .arch x64
            ; push rbp
            ; mov rbp, rsp
            ; mov rdi, QWORD row_data as _
            ; mov rcx, row_count as _
        );
    }
}

#[cfg(test)]
mod tests {
    use dynasmrt::{dynasm, AssemblyOffset, DynasmApi, DynasmLabelApi, ExecutableBuffer};

    pub struct JitCompiler {}

    impl JitCompiler {
        pub fn new() -> Self {
            JitCompiler {}
        }

        pub fn compile_filter(&mut self) -> (AssemblyOffset, ExecutableBuffer) {
            let mut assembler = dynasmrt::x64::Assembler::new().unwrap();
            let entry_point = assembler.offset();

            dynasm!(assembler
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

            (entry_point, assembler.finalize().unwrap())
        }
    }
    #[test]
    fn can_compile_basic_filter() {
        let mut compiler = JitCompiler::new();
        let (entry_point, filter_fn) = compiler.compile_filter();

        let rows = vec![vec![1, 2, 3, 10000], vec![1, 2, 3, 8000]];

        let filter: fn(*const i64) -> i64 =
            unsafe { std::mem::transmute(filter_fn.ptr(entry_point)) };

        for row in &rows {
            let result = filter(row.as_ptr());
            println!("Row: {:?}, Passed: {}", row, result == 1);
        }
    }
}

#[cfg(test)]
mod scan_tests {
    use dynasmrt::{dynasm, AssemblyOffset, DynasmApi, DynasmLabelApi, ExecutableBuffer};

    pub struct JitCompiler {
        row_data: *const i64,
        row_count: usize,
    }

    impl JitCompiler {
        pub fn new(row_data: *const i64, row_count: usize) -> Self {
            JitCompiler {
                row_data,
                row_count,
            }
        }

        pub fn compile_project(
            &mut self,
            column: usize,
            data: &mut [i64],
        ) -> (AssemblyOffset, ExecutableBuffer) {
            let mut assembler = dynasmrt::x64::Assembler::new().unwrap();
            let entry_point = assembler.offset();
            let data_ptr = data.as_ptr();
            let stride_size = self.row_count / 4;
            dynasm!(assembler
                ; .arch x64
                // Load src vector.
                ; mov rsi, QWORD self.row_data as _
                // Load dst vector.
                ; mov rdi, QWORD data_ptr as _
                // Initialize index (RCX) to 0
                ; mov rcx, 0
                ; ->loop_start:
                // Compare index with data length
                ; cmp rcx, stride_size as i64 as i32
                // If index >= length, exit loop
                ; jge >exit
                // Save row index in RBX.
                // Copy from the projected column from src to dst
                ; mov rax, [rsi + column as i64 as i32  * 8]
                ; mov [rdi + rcx * 8], rax
                // Increment index into `src`.
                // Increment index into `dst`.
                ; inc rcx
                // Repeat.
                ; jmp ->loop_start
                ; exit:
                ; ret
            );

            let buffer = assembler.finalize().unwrap();

            (entry_point, buffer)
        }

        pub fn compile_scan(&mut self, data: &mut [i64]) -> (AssemblyOffset, ExecutableBuffer) {
            let mut assembler = dynasmrt::x64::Assembler::new().unwrap();
            let entry_point = assembler.offset();
            let data_ptr = data.as_ptr();

            dynasm!(assembler
                ; .arch x64
                ; mov rsi, QWORD self.row_data as _ // Load src vector.
                ; mov rdi, QWORD data_ptr as _ // Load dst vector.
                ; mov rcx, 0                                // Initialize index (RSI) to 0
                ; ->loop_start:
                ; cmp rcx, self.row_count as i64 as i32       // Compare index with data length
                ; jge >exit                                 // If index >= length, exit loop
                ; mov rax, [rsi + rcx * 8]                  // Copy from src to dst
                ; mov [rdi + rcx * 8], rax
                 // Here, you can add instructions to process each row
                ; add rcx, 1                                // Increment index
                ; jmp ->loop_start                          // Repeat loop
                ; exit:
                ; ret                                       // Return from function
            );

            let buffer = assembler.finalize().unwrap();

            (entry_point, buffer)
        }
    }

    #[test]
    fn can_build_scan_pipeline() {
        let rows = vec![
            vec![1, 2, 3, 4000],
            vec![1, 2, 3, 8000],
            vec![1, 2, 3, 12000],
        ];

        // Flatten the rows into a single buffer
        let flat_rows: Vec<i64> = rows.into_iter().flatten().collect();

        let mut compiler = JitCompiler::new(flat_rows.as_ptr(), flat_rows.len());
        let mut data = vec![0; flat_rows.len()];
        let (entry_point, buffer) = compiler.compile_scan(data.as_mut_slice());

        println!("Entry point: {:?}", entry_point);

        // Execute the compiled code
        let exec_fn: extern "C" fn() -> () =
            unsafe { std::mem::transmute(buffer.ptr(entry_point)) };
        exec_fn();
        println!("Data: {:?}", data);

        let mut data = vec![0; 4];
        let (entry_point, buffer) = compiler.compile_project(3, data.as_mut_slice());

        println!("Entry point: {:?}", entry_point);
        // Execute the compiled code
        let exec_fn: extern "C" fn() -> () =
            unsafe { std::mem::transmute(buffer.ptr(entry_point)) };
        exec_fn();
        println!("Data: {:?}", data)
    }
}
