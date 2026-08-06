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

    fn cargo_lock(&self) -> String {
        fs::read_to_string(self.root.join("Cargo.lock")).expect("Cargo.lock is readable")
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
fn sema_engine_normal_dependency_tree_is_one_exact_runtime_family() {
    let fixture = RepositoryFixture::current();
    let duplicates =
        fixture.cargo_tree(&["--edges", "normal", "--no-default-features", "--duplicates"]);
    assert!(
        duplicates.trim().is_empty(),
        "sema-engine normal graph contains duplicate package families:\n{duplicates}"
    );

    let tree = fixture.cargo_tree(&["--edges", "normal", "--no-default-features"]);
    for build_time_only in ["schema-rust", "sema-translator", "core-ethos", "core-nomos"] {
        assert!(
            !tree.contains(build_time_only),
            "{build_time_only} must remain source-generation machinery, not runtime substrate:\n{tree}"
        );
    }
}

#[test]
fn strict_sema_generation_uses_one_exact_published_producer_train() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();
    let lock = fixture.cargo_lock();

    for exact_dependency in [
        "core-ethos = { git = \"https://github.com/LiGoldragon/core-ethos.git\", rev = \"43b48c779c54ee9f05cbcc111d5d88074b162461\" }",
        "core-nomos = { git = \"https://github.com/LiGoldragon/core-nomos.git\", rev = \"7b60721d199551b648d42a49934a2f0ef950c595\" }",
        "rust-logos = { git = \"https://github.com/LiGoldragon/rust-logos.git\", rev = \"081e99596826b15e2ff7f1356ae8d797b18aeffc\" }",
        "schema-rust = { git = \"https://github.com/LiGoldragon/schema-rust.git\", rev = \"664335240a40728826cfaa09e3100cd867031912\", default-features = false }",
        "sema-translator = { git = \"https://github.com/LiGoldragon/sema-translator.git\", rev = \"287fbd728a05b1a6be1dc8a28bcf3ca06d9916b3\", default-features = false }",
        "signal-frame = { git = \"https://github.com/LiGoldragon/signal-frame.git\", rev = \"8aa0bcaeb29fe9e461a11706a469638d2fd109ac\", default-features = false }",
        "signal-sema-translator = { git = \"https://github.com/LiGoldragon/signal-sema-translator.git\", rev = \"3f41813dd63904c7e2b3da4382eff64ed1bf12fe\" }",
    ] {
        assert!(
            cargo.contains(exact_dependency),
            "strict Sema consumer omitted exact producer {exact_dependency}"
        );
    }
    for sole_package in [
        "core-ethos",
        "core-logos",
        "core-nomos",
        "protos",
        "rust-logos",
        "schema-rust",
        "sema-translator",
        "signal-frame",
        "signal-sema-translator",
    ] {
        assert_eq!(
            lock.matches(&format!("name = \"{sole_package}\"")).count(),
            1,
            "strict Sema generation admitted more than one {sole_package} source"
        );
    }
    assert!(
        !lock.contains("name = \"schema-language\""),
        "the deleted pre-bootstrap schema world must not enter the Sema proof graph"
    );
}

#[test]
fn current_sema_source_is_the_only_live_schema_document() {
    let fixture = RepositoryFixture::current();

    assert!(fixture.has_file("schema/witness.sema"));
    assert!(fixture.has_file("tests/fixtures/generated_sema_table.rs"));
    assert!(!fixture.has_file("schema/sema-engine.concept.schema"));
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
