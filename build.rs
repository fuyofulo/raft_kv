fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["src/proto/raft.proto"], &["src/proto"])
        .expect("failed to compile proto");
}
