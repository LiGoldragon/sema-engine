use std::fs;
use std::path::{Path, PathBuf};

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
fn sema_engine_depends_on_kernel_and_signal_core_by_git() {
    let fixture = RepositoryFixture::current();
    let cargo = fixture.cargo_toml();

    assert!(cargo.contains("https://github.com/LiGoldragon/sema.git"));
    assert!(cargo.contains("https://github.com/LiGoldragon/signal-core.git"));
    assert!(!cargo.contains("path = \"../sema\""));
    assert!(!cargo.contains("path = \"../signal-core\""));
}

#[test]
fn signal_core_roots_do_not_own_read_plan_vocabulary() {
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
    assert!(cargo.contains("signal-core"));
}
