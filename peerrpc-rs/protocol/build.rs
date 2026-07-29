// Build script: compiles the proto files into Rust types via prost.
//
// The proto files live at ../../proto/ relative to this crate.
// We configure prost-build to output the correct module hierarchy:
//   peerrpc.* → peerrpc::
//   peerrpc.signaling.* → peerrpc.signaling::
//   google.rpc.* → google.rpc::

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto_root = root.join("../../proto");

    let protos = [
        proto_root.join("peerrpc/peerrpc.proto"),
        proto_root.join("peerrpc/signaling/signaling.proto"),
        proto_root.join("google/rpc/status.proto"),
        proto_root.join("google/protobuf/any.proto"),
    ];

    println!("cargo:rerun-if-changed={}", proto_root.display());

    prost_build::Config::new()
        .compile_protos(
            &protos
                .iter()
                .map(|p| p.to_str().unwrap())
                .collect::<Vec<_>>(),
            &[proto_root.to_str().unwrap()],
        )
        .expect("failed to compile protos");
}
