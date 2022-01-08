use crate::datum::Datum;
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::parser::ast::AggAction;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use itertools::Itertools;
use std::rc::Rc;

#[derive(Clone)]
enum Reducer {
    Count(CountReducer),
    Max(MaxReducer),
    Avg(AvgReducer),
    Sum(SumReducer),
}

impl From<(AggAction, Datum)> for Reducer {
    fn from(action_and_init: (AggAction, Datum)) -> Self {
        match action_and_init.0 {
            AggAction::No | AggAction::Max => Reducer::Max(MaxReducer::new(action_and_init.1)),
            AggAction::Sum => Reducer::Sum(SumReducer::new(action_and_init.1)),
            AggAction::Cnt => Reducer::Count(CountReducer::new(1)),
            AggAction::Avg => Reducer::Avg(AvgReducer::new(action_and_init.1)),
        }
    }
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

#[derive(Clone)]
struct CountReducer {
    cnt: usize,
}

#[derive(Clone)]
struct MaxReducer {
    max: Datum,
}

#[derive(Clone)]
struct AvgReducer {
    cnt: usize,
    sum: Datum,
}

#[derive(Clone)]
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
    pub fn new(cnt: usize) -> Self {
        Self { cnt }
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
    child: Box<ExecutorImpl>,
    reducers: Vec<Vec<(Datum, Reducer)>>,
    exprs_with_action: Vec<(ExprImpl, AggAction)>,
    group_by_expr: Option<ExprImpl>,
    bpm: BufferPoolManagerRef,
    buffer: Vec<Vec<Datum>>,
    executed: bool,
}

impl AggExecutor {
    pub fn new(
        exprs_with_action: Vec<(ExprImpl, AggAction)>,
        group_by_expr: Option<ExprImpl>,
        child: ExecutorImpl,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            child: Box::new(child),
            reducers: vec![vec![]; exprs_with_action.len()],
            exprs_with_action,
            group_by_expr,
            buffer: vec![],
            bpm,
            executed: false,
        }
    }
}

impl Executor for AggExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            while let Some(slice) = self.child.execute()? {
                let group_by = self.group_by_expr.as_mut().map(|e| e.eval(Some(&slice)));
                let datums_per_expr = self
                    .exprs_with_action
                    .iter()
                    .map(|(e, _)| e.eval(Some(&slice)))
                    .collect_vec();
                let actions = self
                    .exprs_with_action
                    .iter()
                    .map(|(_, a)| a.clone())
                    .collect_vec();
                for ((datums, action), reducers) in datums_per_expr
                    .into_iter()
                    .zip(actions)
                    .zip(self.reducers.iter_mut())
                {
                    for (idx, datum) in datums.iter().enumerate() {
                        let key = if let Some(group_by) = group_by.as_ref() {
                            group_by[idx].clone()
                        } else {
                            0i32.into()
                        };
                        if let Some(r) =
                            reducers.iter_mut().find(|(d, _)| *d == key).map(|(_, r)| r)
                        {
                            r.reduce(datum.clone());
                        } else {
                            reducers.push((key, Reducer::from((action.clone(), datum.clone()))))
                        }
                    }
                }
            }
            self.executed = true;
            self.reducers
                .iter_mut()
                .for_each(|column| column.sort_by(|a, b| a.0.cmp(&b.0)));
            let columns: Vec<Vec<Datum>> = self
                .reducers
                .iter()
                .map(|column| column.iter().map(|(_, r)| r.get()).collect_vec())
                .collect_vec();
            let len = columns[0].len();
            for idx in 0..len {
                let tuple: Vec<Datum> = columns.iter().map(|c| c[idx].clone()).collect_vec();
                self.buffer.push(tuple)
            }
            self.buffer.reverse();
        }
        let mut output = Slice::new(self.bpm.clone(), self.schema());
        while !self.buffer.is_empty() {
            if output.insert(self.buffer.last().unwrap()).is_ok() {
                self.buffer.pop();
            } else {
                break;
            }
        }
        if output.count() == 0 {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
    fn schema(&self) -> SchemaRef {
        let type_and_names = self
            .exprs_with_action
            .iter()
            .map(|(e, a)| match a {
                AggAction::No => (e.return_type(), e.to_string()),
                a => (e.return_type(), format!("{}({})", a.to_string(), e)),
            })
            .collect_vec();
        Rc::new(Schema::from_slice(&type_and_names))
    }
}
