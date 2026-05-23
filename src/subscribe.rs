use std::any::Any;
use std::sync::{Arc, Mutex};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_sema::SemaOperation;

use crate::{QueryFilter, QueryPlan, QuerySnapshot, RecordKey, SnapshotIdentifier, TableName};

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[rkyv(derive(Debug))]
pub struct SubscriptionIdentifier(u64);

impl SubscriptionIdentifier {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn first() -> Self {
        Self(1)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionHandle {
    id: SubscriptionIdentifier,
    table: TableName,
    snapshot: SnapshotIdentifier,
}

impl SubscriptionHandle {
    pub fn new(id: SubscriptionIdentifier, table: TableName, snapshot: SnapshotIdentifier) -> Self {
        Self {
            id,
            table,
            snapshot,
        }
    }

    pub fn id(&self) -> SubscriptionIdentifier {
        self.id
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct SubscriptionRegistration {
    id: SubscriptionIdentifier,
    table_name: String,
    filter: QueryFilter,
    snapshot: SnapshotIdentifier,
}

impl SubscriptionRegistration {
    pub fn new(handle: SubscriptionHandle, filter: QueryFilter) -> Self {
        Self {
            id: handle.id(),
            table_name: handle.table().as_str().to_owned(),
            filter,
            snapshot: handle.snapshot(),
        }
    }

    pub fn id(&self) -> SubscriptionIdentifier {
        self.id
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn filter(&self) -> &QueryFilter {
        &self.filter
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionReceipt<RecordValue> {
    handle: SubscriptionHandle,
    initial: InitialSnapshot<RecordValue>,
}

impl<RecordValue> SubscriptionReceipt<RecordValue> {
    pub fn new(handle: SubscriptionHandle, initial: InitialSnapshot<RecordValue>) -> Self {
        Self { handle, initial }
    }

    pub fn handle(&self) -> SubscriptionHandle {
        self.handle
    }

    pub fn initial(&self) -> &InitialSnapshot<RecordValue> {
        &self.initial
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionEvent<RecordValue> {
    InitialSnapshot(InitialSnapshot<RecordValue>),
    Delta(SubscriptionDelta<RecordValue>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialSnapshot<RecordValue> {
    handle: SubscriptionHandle,
    snapshot: QuerySnapshot<RecordValue>,
}

impl<RecordValue> InitialSnapshot<RecordValue> {
    pub fn new(handle: SubscriptionHandle, snapshot: QuerySnapshot<RecordValue>) -> Self {
        Self { handle, snapshot }
    }

    pub fn handle(&self) -> SubscriptionHandle {
        self.handle
    }

    pub fn snapshot(&self) -> &QuerySnapshot<RecordValue> {
        &self.snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionDelta<RecordValue> {
    handle: SubscriptionHandle,
    kind: DeltaKind,
    table: TableName,
    key: RecordKey,
    snapshot: SnapshotIdentifier,
    record: RecordValue,
}

impl<RecordValue> SubscriptionDelta<RecordValue> {
    pub fn new(
        handle: SubscriptionHandle,
        kind: DeltaKind,
        table: TableName,
        key: RecordKey,
        snapshot: SnapshotIdentifier,
        record: RecordValue,
    ) -> Self {
        Self {
            handle,
            kind,
            table,
            key,
            snapshot,
            record,
        }
    }

    pub fn handle(&self) -> SubscriptionHandle {
        self.handle
    }

    pub fn kind(&self) -> DeltaKind {
        self.kind
    }

    pub fn table(&self) -> &TableName {
        &self.table
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }

    pub fn record(&self) -> &RecordValue {
        &self.record
    }
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
)]
#[rkyv(derive(Debug))]
pub enum DeltaKind {
    Assert,
    Mutate,
    Retract,
}

impl DeltaKind {
    pub fn operation(&self) -> SemaOperation {
        match self {
            Self::Assert => SemaOperation::Assert,
            Self::Mutate => SemaOperation::Mutate,
            Self::Retract => SemaOperation::Retract,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkError {
    message: String,
}

impl SinkError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionDeliveryFailure {
    handle: SubscriptionHandle,
    message: String,
}

impl SubscriptionDeliveryFailure {
    pub fn new(handle: SubscriptionHandle, error: SinkError) -> Self {
        Self {
            handle,
            message: error.message,
        }
    }

    pub fn handle(&self) -> SubscriptionHandle {
        self.handle
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionDeliveryMode {
    Detached,
    Inline,
}

pub trait SubscriptionSink<RecordValue>: Send + Sync {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Detached
    }

    fn deliver(&self, event: SubscriptionEvent<RecordValue>) -> Result<(), SinkError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceRange {
    start: SnapshotIdentifier,
    end: Option<SnapshotIdentifier>,
}

impl SequenceRange {
    pub fn from(start: SnapshotIdentifier) -> Self {
        Self { start, end: None }
    }

    pub fn between(start: SnapshotIdentifier, end: SnapshotIdentifier) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }

    pub fn contains(&self, snapshot: SnapshotIdentifier) -> bool {
        snapshot >= self.start && self.end.is_none_or(|end| snapshot <= end)
    }
}

pub(crate) struct SubscriptionRegistry {
    entries: Mutex<Vec<Arc<dyn ErasedSubscription>>>,
}

impl SubscriptionRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn add<RecordValue>(
        &self,
        entry: ActiveSubscription<RecordValue>,
    ) -> crate::Result<()>
    where
        RecordValue: Clone + Send + Sync + 'static,
    {
        self.entries
            .lock()
            .map_err(|_| crate::Error::SubscriptionRegistryPoisoned)?
            .push(Arc::new(entry));
        Ok(())
    }

    pub(crate) fn deliver_delta<RecordValue>(
        &self,
        kind: DeltaKind,
        table: TableName,
        key: &RecordKey,
        snapshot: SnapshotIdentifier,
        record: &RecordValue,
    ) -> crate::Result<()>
    where
        RecordValue: Clone + Send + Sync + 'static,
    {
        let entries = self
            .entries
            .lock()
            .map_err(|_| crate::Error::SubscriptionRegistryPoisoned)?
            .clone();
        let record = record as &dyn Any;
        for entry in entries {
            entry.deliver_delta(kind, table, key, snapshot, record);
        }
        Ok(())
    }
}

pub(crate) struct ActiveSubscription<RecordValue> {
    handle: SubscriptionHandle,
    plan: QueryPlan<RecordValue>,
    sink: Arc<dyn SubscriptionSink<RecordValue>>,
}

impl<RecordValue> ActiveSubscription<RecordValue> {
    pub(crate) fn new(
        handle: SubscriptionHandle,
        plan: QueryPlan<RecordValue>,
        sink: Arc<dyn SubscriptionSink<RecordValue>>,
    ) -> Self {
        Self { handle, plan, sink }
    }

    fn matches(&self, key: &RecordKey) -> bool {
        self.plan.filter().accepts(key)
    }
}

trait ErasedSubscription: Send + Sync {
    fn deliver_delta(
        &self,
        kind: DeltaKind,
        table: TableName,
        key: &RecordKey,
        snapshot: SnapshotIdentifier,
        record: &dyn Any,
    );
}

impl<RecordValue> ErasedSubscription for ActiveSubscription<RecordValue>
where
    RecordValue: Clone + Send + Sync + 'static,
{
    fn deliver_delta(
        &self,
        kind: DeltaKind,
        table: TableName,
        key: &RecordKey,
        snapshot: SnapshotIdentifier,
        record: &dyn Any,
    ) {
        if *self.plan.table().name() != table || !self.matches(key) {
            return;
        }

        let Some(record) = record.downcast_ref::<RecordValue>() else {
            return;
        };

        let delta = SubscriptionDelta::new(
            self.handle,
            kind,
            table,
            key.clone(),
            snapshot,
            record.clone(),
        );
        let event = SubscriptionEvent::Delta(delta);
        match self.sink.delivery_mode() {
            SubscriptionDeliveryMode::Detached => {
                let sink = Arc::clone(&self.sink);
                drop(std::thread::spawn(move || {
                    let _ = sink.deliver(event);
                }));
            }
            SubscriptionDeliveryMode::Inline => {
                let _ = self.sink.deliver(event);
            }
        }
    }
}
