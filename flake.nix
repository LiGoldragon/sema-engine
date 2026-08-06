{
  description = "sema-engine — typed database verb engine over sema";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-analyzer"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        repoContextFile = path: _type: builtins.elem (baseNameOf path) [
          "AGENTS.md"
          "ARCHITECTURE.md"
          "skills.md"
        ];
        schemaRoot = toString ./schema;
        protosSource = path: _type:
          let candidate = toString path; in
          candidate == schemaRoot || pkgs.lib.hasPrefix "${schemaRoot}/" candidate;
        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            (craneLib.filterCargoSources path type)
            || (repoContextFile path type)
            || (protosSource path type);
        };
        commonArgs = {
          inherit src;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        scriptApplication = name: script: pkgs.writeShellApplication {
          name = "sema-engine-${name}";
          runtimeInputs = [
            toolchain
          ];
          text = ''
            exec "${script}" "$@"
          '';
        };
        testScript = scriptApplication "test" ./scripts/test;
        testDependencyBoundaryScript = scriptApplication "test-dependency-boundary" ./scripts/test-dependency-boundary;
        testEngineScript = scriptApplication "test-engine" ./scripts/test-engine;
        testOperationLogScript = scriptApplication "test-operation-log" ./scripts/test-operation-log;
        testSubscriptionsScript = scriptApplication "test-subscriptions" ./scripts/test-subscriptions;
      in
      {
        packages = {
          default = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
          });

          test = testScript;
          test-dependency-boundary = testDependencyBoundaryScript;
          test-engine = testEngineScript;
          test-operation-log = testOperationLogScript;
          test-subscriptions = testSubscriptionsScript;
        };

        apps = {
          default = {
            type = "app";
            program = "${testScript}/bin/sema-engine-test";
            meta.description = "Run sema-engine's full test suite";
          };

          test = {
            type = "app";
            program = "${testScript}/bin/sema-engine-test";
            meta.description = "Run sema-engine's full test suite";
          };

          test-dependency-boundary = {
            type = "app";
            program = "${testDependencyBoundaryScript}/bin/sema-engine-test-dependency-boundary";
            meta.description = "Run sema-engine's architectural dependency witnesses";
          };

          test-engine = {
            type = "app";
            program = "${testEngineScript}/bin/sema-engine-test-engine";
            meta.description = "Run sema-engine's registered-record execution witnesses";
          };

          test-operation-log = {
            type = "app";
            program = "${testOperationLogScript}/bin/sema-engine-test-operation-log";
            meta.description = "Run sema-engine's operation-log and snapshot cursor witnesses";
          };

          test-subscriptions = {
            type = "app";
            program = "${testSubscriptionsScript}/bin/sema-engine-test-subscriptions";
            meta.description = "Run sema-engine's subscription delivery witnesses";
          };
        };

        checks = {
          build = craneLib.cargoBuild (commonArgs // {
            inherit cargoArtifacts;
          });

          test = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
          });

          test-dependency-boundary = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test dependency_boundary";
          });

          test-engine = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test engine";
          });

          test-operation-log = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test operation_log";
          });

          test-staging = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test staging";
          });

          test-subscriptions = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            cargoTestExtraArgs = "--test subscriptions";
          });

          doc = craneLib.cargoDoc (commonArgs // {
            inherit cargoArtifacts;
            RUSTDOCFLAGS = "-D warnings";
          });

          fmt = craneLib.cargoFmt {
            inherit src;
          };

          clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- -D warnings";
          });
        };

        devShells.default = pkgs.mkShell {
          name = "sema-engine";
          packages = [
            pkgs.jujutsu
            pkgs.pkg-config
            toolchain
          ];
        };
      }
    );
}
