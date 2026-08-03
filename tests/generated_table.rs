use sema_engine::{Engine, EngineOpen, SchemaHash, SchemaVersion, TableName, TableSpecification};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
struct Domain(String);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
struct StoredRecord {
    identifier: u64,
    body: String,
}

struct Records;

impl TableSpecification for Records {
    type Record = StoredRecord;
    type Key = Domain;

    const TABLE_NAME: TableName = TableName::new("records");
    const FAMILY_NAME: &'static str = "zRecordsStableIdentity";
    const SCHEMA_HASH: SchemaHash = SchemaHash::new([7; 32]);
}

#[test]
fn generated_style_table_specification_writes_and_reads_a_typed_domain_key() {
    let temporary = tempfile::tempdir().expect("fresh Sema directory");
    let path = temporary.path().join("generated-table.sema");
    let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1)))
        .expect("fresh redb-backed Sema opens");
    engine
        .register_table(Records::descriptor())
        .expect("generated table registers");

    let domain = Domain("software/code-generation".to_owned());
    let stored = StoredRecord {
        identifier: 17,
        body: "typed value".to_owned(),
    };
    engine
        .assert_keyed(Records::assertion(&domain, stored.clone()).expect("typed assertion"))
        .expect("record commits through sema-engine");
    let snapshot = engine
        .match_records(Records::query(&domain).expect("typed key query"))
        .expect("record reads through sema-engine");

    assert_eq!(snapshot.records(), std::slice::from_ref(&stored));
    drop(engine);

    let mut reopened = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1)))
        .expect("redb-backed Sema reopens");
    reopened
        .register_table(Records::descriptor())
        .expect("generated table re-registers");
    assert_eq!(
        reopened
            .match_records(Records::query(&domain).expect("typed reopen query"))
            .expect("persisted record reads")
            .records(),
        &[stored],
    );
}
