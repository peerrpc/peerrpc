// Build script for peerrpc-signal.
//
// Generates tonic + prost types for the v2 signaling proto so the
// Remote backend can speak the same wire format as the Go and TS
// clients.

use std::path::PathBuf;

fn main() {
    // Only build when the remote feature is enabled; otherwise the
    // tonic-build dependency is not even present.
    if std::env::var("CARGO_FEATURE_REMOTE").is_err() {
        return;
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto_root = root.join("../../proto");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set");

    tonic_build::configure()
        .build_server(false)
        .out_dir(&out_dir)
        .compile_protos(
            &["peerrpc/signaling/v2/signaling.proto"]
                .iter()
                .map(|s| proto_root.join(s).to_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            &[proto_root.to_str().unwrap()],
        )
        .expect("failed to compile v2 signaling proto");

    println!("cargo:rerun-if-changed={}", proto_root.display());
}
