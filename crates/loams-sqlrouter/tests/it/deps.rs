/// The kernel crate cannot do I/O: no async runtime, network, database or
/// metastore client may appear in its dependencies (§31 §7.1, ruling 5).
#[test]
fn kernel_crate_has_no_io_dependencies() {
    let manifest =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).unwrap();
    let deps = manifest
        .split("[dependencies]")
        .nth(1)
        .and_then(|s| s.split("\n[").next())
        .expect("a [dependencies] table");
    let forbidden = [
        "tokio",
        "reqwest",
        "hyper",
        "tikv-client",
        "tokio-postgres",
        "mysql_async",
        "sqlx",
        "async-std",
        "smol",
        "mio",
        "loams-tikv",
        "loams-meta",
    ];
    for line in deps
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let name = line.split(['=', ' ', '.']).next().unwrap_or_default();
        assert!(
            !forbidden.contains(&name),
            "the kernel crate must not depend on {name}"
        );
    }
}
