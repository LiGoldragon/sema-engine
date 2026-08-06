use sema_engine::{Engine, EngineOpen, SchemaVersion, TableSpecification};

mod support;

#[allow(non_camel_case_types)]
mod generated {
    include!("fixtures/generated_sema_table.rs");
}

use generated::{z2VKo1, z2VKo2, z2VKo3};

#[test]
fn checked_sema_source_and_generated_rust_are_fresh() {
    let generated = support::generated_sema_table();
    generated
        .write_or_check(support::UPDATE_VARIABLE)
        .expect("checked Sema and Rust projections are fresh");
    let table_identity = support::generated_identity(102);
    assert_eq!(z2VKo3::TABLE_NAME.as_str(), table_identity.as_str());
}

#[test]
fn generated_domain_snapshot_table_writes_and_reads_a_typed_domain_key() {
    let temporary = tempfile::tempdir().expect("fresh Sema directory");
    let path = temporary.path().join("generated-table.sema");
    let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1)))
        .expect("fresh redb-backed Sema opens");
    engine
        .register_table(z2VKo3::descriptor())
        .expect("generated table registers");

    let domain = z2VKo1::new("software/code-generation".to_owned());
    assert_eq!(
        z2VKo3::record_key(&domain)
            .expect("generated source key projection")
            .to_owned_string(),
        "software/code-generation"
    );
    let domain_snapshot = z2VKo2 {
        field_0: domain.clone(),
        field_1: 17,
        field_2: "typed value".to_owned(),
    };
    engine
        .assert_keyed(z2VKo3::assertion(&domain, domain_snapshot.clone()).expect("typed assertion"))
        .expect("record commits through sema-engine");
    let snapshot = engine
        .match_records(z2VKo3::query(&domain).expect("typed key query"))
        .expect("record reads through sema-engine");

    assert_eq!(snapshot.records(), std::slice::from_ref(&domain_snapshot));
    drop(engine);

    let mut reopened = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1)))
        .expect("redb-backed Sema reopens");
    reopened
        .register_table(z2VKo3::descriptor())
        .expect("generated table re-registers");
    assert_eq!(
        reopened
            .match_records(z2VKo3::query(&domain).expect("typed reopen query"))
            .expect("persisted record reads")
            .records(),
        &[domain_snapshot],
    );
}
