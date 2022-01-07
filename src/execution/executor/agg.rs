use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::parser::ast::AggAction;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

enum Reducer {
    Count(CountReducer),
    Max(MaxReducer),
    Avg(AvgReducer),
    Sum(SumReducer),
}

impl Reducer {
    pub fn reduce(&mut self, datum: Datum) {
        match self {
            Self::Count(r) => r.reduce(datum),
            Self::Max(r) => r.reduce(datum),
            Self::Avg(r) => r.reduce(datum),
            Self::Sum(r) => r.reduce(datum),
        }
    }
    pub fn get(&self) -> Datum {
        match self {
            Self::Count(r) => r.get(),
            Self::Max(r) => r.get(),
            Self::Avg(r) => r.get(),
            Self::Sum(r) => r.get(),
        }
    }
}

struct CountReducer {
    cnt: usize,
}

struct MaxReducer {
    max: Datum,
}

struct AvgReducer {
    cnt: usize,
    sum: Datum,
}

struct SumReducer {
    sum: Datum,
}

impl CountReducer {
    pub fn reduce(&mut self, _: Datum) {
        self.cnt += 1;
    }
    pub fn get(&self) -> Datum {
        (self.cnt as i32).into()
    }
    pub fn new() -> Self {
        Self { cnt: 0 }
    }
}

impl MaxReducer {
    pub fn reduce(&mut self, datum: Datum) {
        if datum > self.max {
            self.max = datum;
        }
    }
    pub fn get(&self) -> Datum {
        self.max.clone()
    }
    pub fn new(datum: Datum) -> Self {
        Self { max: datum }
    }
}

impl AvgReducer {
    pub fn reduce(&mut self, datum: Datum) {
        self.cnt += 1;
        self.sum = self.sum.clone() + datum;
    }
    pub fn get(&self) -> Datum {
        self.sum.clone() / self.cnt
    }
    pub fn new(datum: Datum) -> Self {
        Self { cnt: 0, sum: datum }
    }
}

impl SumReducer {
    pub fn reduce(&mut self, datum: Datum) {
        self.sum = self.sum.clone() + datum;
    }
    pub fn get(&self) -> Datum {
        self.sum.clone()
    }
    pub fn new(datum: Datum) -> Self {
        Self { sum: datum }
    }
}

pub struct AggExecutor {
    action: AggAction,
    expr: ExprImpl,
    child: Box<ExecutorImpl>,
    reducer: Reducer,
    bpm: BufferPoolManagerRef,
    executed: bool,
}

impl AggExecutor {
    pub fn new(
        action: AggAction,
        expr: ExprImpl,
        child: ExecutorImpl,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            child: Box::new(child),
            reducer: match action {
                AggAction::Cnt => Reducer::Count(CountReducer::new()),
                AggAction::Sum => Reducer::Sum(SumReducer::new(match expr.return_type() {
                    DataType::Int(_) => 0i32.into(),
                    DataType::Float(_) => 0f32.into(),
                    _ => todo!(),
                })),
                AggAction::Avg => Reducer::Avg(AvgReducer::new(match expr.return_type() {
                    DataType::Int(_) => 0i32.into(),
                    DataType::Float(_) => 0f32.into(),
                    _ => todo!(),
                })),
                AggAction::Max => Reducer::Max(MaxReducer::new(match expr.return_type() {
                    DataType::Int(_) => (-2147483648i32).into(),
                    DataType::Float(_) => f32::NEG_INFINITY.into(),
                    _ => todo!(),
                })),
            },
            expr,
            action,
            bpm,
            executed: false,
        }
    }
}

impl Executor for AggExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            while let Some(slice) = self.child.execute()? {
                let datums = self.expr.eval(Some(&slice));
                for datum in datums {
                    self.reducer.reduce(datum)
                }
            }
            self.executed = true;
            let res = self.reducer.get();
            let mut output = Slice::new(self.bpm.clone(), self.schema());
            output.insert(&[res])?;
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            self.expr.return_type(),
            self.action.to_string(),
        )]))
    }
}
