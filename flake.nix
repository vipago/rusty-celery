{
  description = "rust-celery development environment";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustPkg = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml);
        buildDependencies = with pkgs; [
          openssl.dev
          pkg-config
          gcc
          rustPkg
        ];
        runCargoTestWithReport = pkgs.writeScriptBin "cargo-test-with-report" ''
          mkdir junit-reports
          ${rustPkg}/bin/cargo test -- -Z unstable-options --format json --report-time | tee results.json | ${pkgs.cargo2junit}/bin/cargo2junit | tee junit-reports/TEST-all.xml
        '';
        
      in with pkgs; {
        devShells = {
          default = mkShell {
            name = "rust-celery-dev";
            buildInputs = buildDependencies ++ [
              doppler
              nixfmt
            ];
          };
          github-actions = mkShell {
            name = "rust-celery-github-actions";
            buildInputs = buildDependencies ++ [ runCargoTestWithReport ];
          };
        };
      });
}
