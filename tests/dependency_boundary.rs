use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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

    fn cargo_tree(&self, arguments: &[&str]) -> String {
        let output = Command::new(env!("CARGO"))
            .arg("tree")
            .args(arguments)
            .current_dir(&self.root)
            .output()
            .expect("cargo tree runs");
        assert!(
            output.status.success(),
            "cargo tree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("cargo tree output is UTF-8")
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
fn sema_engine_carries_repo_local_direction_context() {
    let fixture = RepositoryFixture::current();

    // Durable direction lives in ARCHITECTURE.md; there is no per-repo
    // INTENT.md.
    assert!(!fixture.has_file("INTENT.md"));
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
fn sema_engine_normal_dependency_tree_has_no_nota_next() {
    let fixture = RepositoryFixture::current();
    let tree = fixture.cargo_tree(&["--edges", "normal", "--no-default-features"]);

    assert!(
        !tree.contains("nota-next") && !tree.contains("nota_next"),
        "sema-engine normal dependency tree must not contain nota-next:\n{tree}"
    );
}

#[test]
fn sema_engine_does_not_depend_on_raw_redb_directly() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    assert!(!cargo.contains("redb ="));
}

#[test]
fn sema_engine_depends_on_storage_kernel_frame_kernel_and_signal_sema_by_git() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    assert!(cargo.contains("https://github.com/LiGoldragon/sema.git"));
    assert!(cargo.contains("https://github.com/LiGoldragon/signal-frame.git"));
    assert!(cargo.contains("https://github.com/LiGoldragon/signal-sema.git"));
    assert!(!cargo.contains("path = \"../sema\""));
    assert!(!cargo.contains("path = \"../signal-frame\""));
    assert!(!cargo.contains("path = \"../signal-sema\""));
    assert!(
        !cargo.contains("signal-core"),
        "sema-engine must not depend on the retired signal-core crate"
    );
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
