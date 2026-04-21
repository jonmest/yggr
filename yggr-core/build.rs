fn main() -> std::io::Result<()> {
    let proto = "src/transport/protobuf/raft.proto";
    println!("cargo:rerun-if-changed={proto}");
    prost_build::compile_protos(&[proto], &["src/transport/protobuf"])
}
