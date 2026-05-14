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
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        src = craneLib.cleanCargoSource ./.;
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
      in
      {
        packages = {
          default = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
          });

          test = testScript;
          test-dependency-boundary = testDependencyBoundaryScript;
          test-engine = testEngineScript;
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
