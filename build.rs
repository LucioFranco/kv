const PROTO: &'static str = "proto/kv/kv.proto";

fn main() {
    tonic_build::compile_protos(PROTO).unwrap();

    // prevent needing to rebuild if files (or deps) haven't changed
    println!("cargo:rerun-if-changed={}", PROTO);
}
