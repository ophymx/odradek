//! `cargo xtask ci` — every CI check a laptop can run, in one command.
//!
//! This exists because of a specific way the workspace can be broken
//! while every local command says it is fine. `cargo test` and `cargo
//! clippy` use whatever toolchain is installed, which is newer than the
//! 1.85 this workspace promises; a language feature from 1.88 therefore
//! compiles cleanly here and fails only in the MSRV job, after a push.
//! That happened, and the documented local loop — `cargo test
//! --workspace` — could not have caught it.
//!
//! The fix is a command rather than a note in a README, on the grounds
//! this repository keeps arriving at from the other direction: a check
//! that depends on somebody remembering is a check that holds right up
//! until it does not.
//!
//! What it deliberately does **not** run is the two jobs that need
//! something a laptop may not have: the real-broker conformance matrix
//! and the third-party client matrix (Docker, and minutes), and the
//! aarch64 CRC job (a different architecture). Those are named in the
//! summary rather than silently omitted, because a green run here is
//! not a promise that CI passes — it is a promise about the half that
//! does not need a broker.

use std::process::Command;

use anyhow::{Result, bail};

use crate::workspace_root;

/// The toolchain `rust-version` in the workspace manifest promises.
///
/// Read from the manifest rather than written twice: the whole point of
/// this task is that the number here and the number CI enforces cannot
/// drift apart.
fn declared_msrv(manifest: &str) -> Option<String> {
    manifest
        .lines()
        .find_map(|line| line.trim().strip_prefix("rust-version"))
        .and_then(|rest| rest.split('"').nth(1))
        .map(str::to_owned)
}

/// One check, as CI runs it.
struct Step {
    /// What this is for, in the summary.
    name: &'static str,
    /// The program to run.
    program: String,
    args: Vec<String>,
    /// Environment to set for this step only.
    env: Vec<(&'static str, &'static str)>,
}

fn step(name: &'static str, args: &[&str]) -> Step {
    Step {
        name,
        program: env!("CARGO").to_owned(),
        args: args.iter().map(|a| (*a).to_owned()).collect(),
        env: Vec::new(),
    }
}

impl Step {
    fn env(mut self, key: &'static str, value: &'static str) -> Step {
        self.env.push((key, value));
        self
    }
}

pub fn ci(args: &[String]) -> Result<()> {
    let root = workspace_root();
    let manifest = std::fs::read_to_string(root.join("Cargo.toml"))?;
    let msrv = declared_msrv(&manifest);
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();

    let mut steps = vec![
        step("fmt", &["fmt", "--check"]),
        // `--locked` throughout, as CI does: without it a newer
        // semver-compatible dependency can resolve here and not there,
        // which turns a reviewed lockfile into a suggestion.
        step(
            "clippy",
            &[
                "clippy",
                "--workspace",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ],
        ),
        step("test", &["test", "--workspace", "--locked"]),
        step("docs", &["doc", "--workspace", "--no-deps", "--locked"])
            .env("RUSTDOCFLAGS", "-D warnings"),
        // A doc link to a cfg'd-out item is only an error in the
        // configuration that omits it, so the run above cannot see
        // those.
        step(
            "docs (no kafka)",
            &[
                "doc",
                "-p",
                "odradek-web-core",
                "--no-deps",
                "--no-default-features",
                "--locked",
            ],
        )
        .env("RUSTDOCFLAGS", "-D warnings"),
        step(
            "hardware crc",
            &[
                "test",
                "-p",
                "odradek-protocol",
                "--features",
                "hardware-crc",
                "--locked",
            ],
        ),
        step(
            "lean client",
            &[
                "check",
                "-p",
                "odradek-client",
                "--no-default-features",
                "--locked",
            ],
        ),
        step(
            "lean acceptance",
            &[
                "check",
                "-p",
                "odradek-acceptance",
                "--no-default-features",
                "--locked",
            ],
        ),
        step(
            "lean web transports",
            &[
                "check",
                "-p",
                "odradek-web-sse",
                "-p",
                "odradek-web-ws",
                "--no-default-features",
                "--locked",
            ],
        ),
    ];

    // The step this task was written for. Placed last among the
    // compiling ones so a plain mistake is reported before a
    // version-specific one.
    match &msrv {
        Some(version) if has_toolchain(version) => steps.push(Step {
            name: "msrv",
            // Through `rustup run`, not `cargo +1.85`. `env!("CARGO")`
            // is the absolute path to one toolchain's cargo binary, and
            // a `+toolchain` directive means nothing to it — only the
            // rustup shim on PATH understands those. The first version
            // of this step got that wrong and "failed" for it, which is
            // at least the right direction to be wrong in.
            program: "rustup".to_owned(),
            args: [
                "run".into(),
                version.clone(),
                "cargo".into(),
                "check".into(),
                "--workspace".into(),
                "--all-targets".into(),
                "--locked".into(),
            ]
            .into(),
            env: Vec::new(),
        }),
        Some(version) => eprintln!(
            "note: skipping the msrv check — rustup has no {version} toolchain.\n\
             Install it with `rustup toolchain install {version}`; until then this \
             command cannot tell you what CI's MSRV job will say."
        ),
        None => eprintln!("note: skipping the msrv check — no rust-version in Cargo.toml"),
    }

    let mut failed = Vec::new();
    if filters.is_empty() || filters.iter().any(|f| "codegen".contains(f.as_str())) {
        eprintln!("=== codegen ===");
        if let Err(e) = codegen_is_in_sync(&root) {
            eprintln!("{e:#}");
            failed.push("codegen");
        }
    }
    for s in &steps {
        if !filters.is_empty() && !filters.iter().any(|f| s.name.contains(f.as_str())) {
            continue;
        }
        eprintln!("=== {} ===", s.name);
        let mut cmd = Command::new(&s.program);
        cmd.current_dir(&root).args(&s.args);
        for (key, value) in &s.env {
            cmd.env(key, value);
        }
        let status = cmd.status()?;
        if !status.success() {
            failed.push(s.name);
        }
    }

    eprintln!(
        "\nnot run here: real-broker conformance and the client matrix \
         (`cargo xtask conformance`, `cargo xtask client-matrix` — both need docker), \
         and the aarch64 crc job."
    );
    if !failed.is_empty() {
        bail!("{} check(s) failed: {}", failed.len(), failed.join(", "));
    }
    eprintln!("all checks passed");
    Ok(())
}

/// Regenerate the message types and check nothing moved.
///
/// Scoped to the generated directory rather than the whole tree, which
/// is the difference between this and CI's version. CI runs on a clean
/// checkout and can afford `git diff --exit-code`; here there is
/// usually other work in progress, and failing "codegen" because of an
/// unrelated unstaged edit would teach people to ignore it.
fn codegen_is_in_sync(root: &std::path::Path) -> Result<()> {
    crate::codegen()?;
    let generated = "crates/odradek-protocol/src/messages";
    let fmt = Command::new(env!("CARGO"))
        .current_dir(root)
        .args(["fmt"])
        .status()?;
    if !fmt.success() {
        bail!("rustfmt failed on the regenerated code");
    }
    let diff = Command::new("git")
        .current_dir(root)
        .args(["diff", "--exit-code", "--", generated])
        .status()?;
    if !diff.success() {
        bail!("{generated} is not what the schemas generate; commit the regenerated files");
    }
    Ok(())
}

/// Whether rustup has `version` installed.
fn has_toolchain(version: &str) -> bool {
    let Ok(out) = Command::new("rustup").args(["toolchain", "list"]).output() else {
        return false;
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|line| line.starts_with(version))
}

#[cfg(test)]
mod tests {
    use super::declared_msrv;

    #[test]
    fn the_msrv_is_read_from_the_manifest() {
        let manifest = "[workspace.package]\nedition = \"2024\"\nrust-version = \"1.85\"\n";
        assert_eq!(declared_msrv(manifest).as_deref(), Some("1.85"));
    }

    /// The real one, so a manifest that stops declaring an MSRV — or
    /// declares it somewhere this parser does not look — fails here
    /// rather than silently skipping the check it exists for.
    #[test]
    fn the_real_manifest_declares_one() {
        let manifest = std::fs::read_to_string(crate::workspace_root().join("Cargo.toml")).unwrap();
        assert!(
            declared_msrv(&manifest).is_some(),
            "no rust-version found; `cargo xtask ci` would skip the msrv check"
        );
    }
}
