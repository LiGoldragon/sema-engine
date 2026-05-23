use std::marker::PhantomData;

use signal_sema::SemaOperation;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{RecordKey, SnapshotIdentifier, TableName, TableReference};

#[derive(Debug, Clone)]
pub struct QueryPlan<RecordValue> {
    table: TableReference<RecordValue>,
    filter: QueryFilter,
    read_plan: ReadPlan<RecordValue>,
}

impl<RecordValue> QueryPlan<RecordValue> {
    pub fn all(table: TableReference<RecordValue>) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::all_rows(),
        }
    }

    pub fn key(table: TableReference<RecordValue>, key: RecordKey) -> Self {
        Self {
            table,
            filter: QueryFilter::Key(key.clone()),
            read_plan: ReadPlan::by_key(key),
        }
    }

    pub fn key_range(table: TableReference<RecordValue>, range: KeyRange) -> Self {
        Self {
            table,
            filter: QueryFilter::KeyRange(range.clone()),
            read_plan: ReadPlan::by_key_range(range),
        }
    }

    pub fn filtered(table: TableReference<RecordValue>, predicate: PredicatePlan) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::filter(ReadPlan::all_rows(), predicate),
        }
    }

    pub fn constrain(table: TableReference<RecordValue>, unify: UnificationPlan) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::constrain(unify),
        }
    }

    pub fn project(table: TableReference<RecordValue>, fields: FieldSelection) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::project(ReadPlan::all_rows(), fields),
        }
    }

    pub fn aggregate(table: TableReference<RecordValue>, reducer: AggregatePlan) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::aggregate(ReadPlan::all_rows(), reducer),
        }
    }

    pub fn infer(table: TableReference<RecordValue>, rules: RuleSetRef) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::infer(ReadPlan::all_rows(), rules),
        }
    }

    pub fn recurse(table: TableReference<RecordValue>, mode: RecursionMode) -> Self {
        Self {
            table,
            filter: QueryFilter::All,
            read_plan: ReadPlan::recurse(ReadPlan::all_rows(), mode),
        }
    }

    pub fn table(&self) -> &TableReference<RecordValue> {
        &self.table
    }

    pub fn filter(&self) -> &QueryFilter {
        &self.filter
    }

    pub fn read_plan(&self) -> &ReadPlan<RecordValue> {
        &self.read_plan
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum QueryFilter {
    All,
    Key(RecordKey),
    KeyRange(KeyRange),
}

impl QueryFilter {
    pub fn accepts(&self, key: &RecordKey) -> bool {
        match self {
            Self::All => true,
            Self::Key(expected) => expected == key,
            Self::KeyRange(range) => range.contains(key),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct KeyRange {
    start: Option<RecordKey>,
    end: Option<RecordKey>,
}

impl KeyRange {
    pub fn new(start: Option<RecordKey>, end: Option<RecordKey>) -> Self {
        Self { start, end }
    }

    pub fn from(start: RecordKey) -> Self {
        Self::new(Some(start), None)
    }

    pub fn through(end: RecordKey) -> Self {
        Self::new(None, Some(end))
    }

    pub fn between(start: RecordKey, end: RecordKey) -> Self {
        Self::new(Some(start), Some(end))
    }

    pub fn contains(&self, key: &RecordKey) -> bool {
        self.start.as_ref().is_none_or(|start| key >= start)
            && self.end.as_ref().is_none_or(|end| key <= end)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPlan<RecordValue> {
    node: ReadPlanNode,
    record: PhantomData<fn() -> RecordValue>,
}

impl<RecordValue> ReadPlan<RecordValue> {
    pub fn all_rows() -> Self {
        Self::new(ReadPlanNode::AllRows)
    }

    pub fn by_key(key: RecordKey) -> Self {
        Self::new(ReadPlanNode::ByKey(key))
    }

    pub fn by_key_range(range: KeyRange) -> Self {
        Self::new(ReadPlanNode::ByKeyRange(range))
    }

    pub fn filter(source: Self, predicate: PredicatePlan) -> Self {
        Self::new(ReadPlanNode::Filter {
            source: Box::new(source.node),
            predicate,
        })
    }

    pub fn constrain(unify: UnificationPlan) -> Self {
        Self::new(ReadPlanNode::Constrain { unify })
    }

    pub fn project(source: Self, fields: FieldSelection) -> Self {
        Self::new(ReadPlanNode::Project {
            source: Box::new(source.node),
            fields,
        })
    }

    pub fn aggregate(source: Self, reducer: AggregatePlan) -> Self {
        Self::new(ReadPlanNode::Aggregate {
            source: Box::new(source.node),
            reducer,
        })
    }

    pub fn infer(source: Self, rules: RuleSetRef) -> Self {
        Self::new(ReadPlanNode::Infer {
            source: Box::new(source.node),
            rules,
        })
    }

    pub fn recurse(seed: Self, mode: RecursionMode) -> Self {
        Self::new(ReadPlanNode::Recurse {
            seed: Box::new(seed.node),
            mode,
        })
    }

    pub fn operator(&self) -> ReadOperator {
        self.node.operator()
    }

    pub fn node(&self) -> &ReadPlanNode {
        &self.node
    }

    fn new(node: ReadPlanNode) -> Self {
        Self {
            node,
            record: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadPlanNode {
    AllRows,
    ByKey(RecordKey),
    ByKeyRange(KeyRange),
    Filter {
        source: Box<ReadPlanNode>,
        predicate: PredicatePlan,
    },
    Constrain {
        unify: UnificationPlan,
    },
    Project {
        source: Box<ReadPlanNode>,
        fields: FieldSelection,
    },
    Aggregate {
        source: Box<ReadPlanNode>,
        reducer: AggregatePlan,
    },
    Infer {
        source: Box<ReadPlanNode>,
        rules: RuleSetRef,
    },
    Recurse {
        seed: Box<ReadPlanNode>,
        mode: RecursionMode,
    },
}

impl ReadPlanNode {
    pub fn operator(&self) -> ReadOperator {
        match self {
            Self::AllRows => ReadOperator::AllRows,
            Self::ByKey(_) => ReadOperator::ByKey,
            Self::ByKeyRange(_) => ReadOperator::ByKeyRange,
            Self::Filter { .. } => ReadOperator::Filter,
            Self::Constrain { .. } => ReadOperator::Constrain,
            Self::Project { .. } => ReadOperator::Project,
            Self::Aggregate { .. } => ReadOperator::Aggregate,
            Self::Infer { .. } => ReadOperator::Infer,
            Self::Recurse { .. } => ReadOperator::Recurse,
        }
    }
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
)]
#[rkyv(derive(Debug))]
pub enum ReadOperator {
    AllRows,
    ByKey,
    ByKeyRange,
    Filter,
    Constrain,
    Project,
    Aggregate,
    Infer,
    Recurse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicatePlan {
    expression: String,
}

impl PredicatePlan {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
        }
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSelection {
    fields: Vec<String>,
}

impl FieldSelection {
    pub fn named(fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnificationPlan {
    bindings: Vec<String>,
}

impl UnificationPlan {
    pub fn new(bindings: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bindings: bindings.into_iter().map(Into::into).collect(),
        }
    }

    pub fn bindings(&self) -> &[String] {
        &self.bindings
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatePlan {
    reducer: String,
}

impl AggregatePlan {
    pub fn new(reducer: impl Into<String>) -> Self {
        Self {
            reducer: reducer.into(),
        }
    }

    pub fn reducer(&self) -> &str {
        &self.reducer
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSetRef {
    name: String,
}

impl RuleSetRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecursionMode {
    edge: String,
}

impl RecursionMode {
    pub fn new(edge: impl Into<String>) -> Self {
        Self { edge: edge.into() }
    }

    pub fn edge(&self) -> &str {
        &self.edge
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySnapshot<RecordValue> {
    operation: SemaOperation,
    table: TableName,
    snapshot: SnapshotIdentifier,
    records: Vec<RecordValue>,
}

impl<RecordValue> QuerySnapshot<RecordValue> {
    pub fn new(
        operation: SemaOperation,
        table: TableName,
        snapshot: SnapshotIdentifier,
        records: Vec<RecordValue>,
    ) -> Self {
        Self {
            operation,
            table,
            snapshot,
            records,
        }
    }

    pub fn operation(&self) -> SemaOperation {
        self.operation
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn records(&self) -> &[RecordValue] {
        &self.records
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationReceipt {
    operation: SemaOperation,
    table: TableName,
    snapshot: SnapshotIdentifier,
    record_count: usize,
}

impl ValidationReceipt {
    pub fn new(
        operation: SemaOperation,
        table: TableName,
        snapshot: SnapshotIdentifier,
        record_count: usize,
    ) -> Self {
        Self {
            operation,
            table,
            snapshot,
            record_count,
        }
    }

    pub fn operation(&self) -> SemaOperation {
        self.operation
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }
}
