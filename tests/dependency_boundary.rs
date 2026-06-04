use std::fs;
use std::path::{Path, PathBuf};

use signal_sema::SemaOperation;

struct RepositoryFixture {
    root: PathBuf,
}

impl RepositoryFixture {
    fn current() -> Self {
        Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        }
    }

    fn cargo_toml(&self) -> String {
        fs::read_to_string(self.root.join("Cargo.toml")).expect("Cargo.toml is readable")
    }

    fn has_file(&self, path: impl AsRef<Path>) -> bool {
        self.root.join(path).exists()
    }
}

#[test]
fn sema_engine_ships_no_daemon_binary() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    assert!(!cargo.contains("[[bin]]"));
    assert!(!fixture.has_file("src/main.rs"));
}

#[test]
fn sema_engine_carries_repo_local_intent_context() {
    let fixture = RepositoryFixture::current();

    assert!(fixture.has_file("INTENT.md"));
    assert!(fixture.has_file("ARCHITECTURE.md"));
    assert!(fixture.has_file("AGENTS.md"));
    assert!(fixture.has_file("skills.md"));
}

#[test]
fn sema_engine_has_no_runtime_or_text_dependencies() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    for forbidden in [
        "kameo",
        "tokio",
        "nota-codec",
        "signal-persona",
        "persona-router",
        "persona-mind",
    ] {
        assert!(
            !cargo.contains(forbidden),
            "Cargo.toml must not contain {forbidden}"
        );
    }
}

#[test]
fn sema_engine_does_not_depend_on_raw_redb_directly() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    assert!(!cargo.contains("redb ="));
}

#[test]
fn sema_engine_depends_on_kernel_signal_core_and_signal_sema_by_git() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    assert!(cargo.contains("https://github.com/LiGoldragon/sema.git"));
    assert!(cargo.contains("https://github.com/LiGoldragon/signal-core.git"));
    assert!(cargo.contains("https://github.com/LiGoldragon/signal-sema.git"));
    assert!(!cargo.contains("path = \"../sema\""));
    assert!(!cargo.contains("path = \"../signal-core\""));
    assert!(!cargo.contains("path = \"../signal-sema\""));
}

#[test]
fn sema_engine_owns_read_plan_vocabulary() {
    let fixture = RepositoryFixture::current();
    let source =
        fs::read_to_string(fixture.root.join("src/query.rs")).expect("query source is readable");
    let cargo = fixture.cargo_toml();

    for operator in ["Constrain", "Project", "Aggregate", "Infer", "Recurse"] {
        assert!(
            source.contains(operator),
            "{operator} must live in sema-engine read-plan vocabulary"
        );
    }
    assert!(cargo.contains("signal-sema"));
}

#[test]
fn signal_sema_operation_set_is_closed_at_six_operations_without_atomic() {
    let operations = [
        SemaOperation::Assert,
        SemaOperation::Mutate,
        SemaOperation::Retract,
        SemaOperation::Match,
        SemaOperation::Subscribe,
        SemaOperation::Validate,
    ];

    assert_eq!(operations.len(), 6);
    for operation in operations {
        assert!(SemaOperationWitness::accepts(operation));
    }
}

struct SemaOperationWitness;

impl SemaOperationWitness {
    fn accepts(operation: SemaOperation) -> bool {
        match operation {
            SemaOperation::Assert
            | SemaOperation::Mutate
            | SemaOperation::Retract
            | SemaOperation::Match
            | SemaOperation::Subscribe
            | SemaOperation::Validate => true,
        }
    }
}
