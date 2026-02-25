//! Compiles gRPC (ugnos) and Prometheus remote-write (prompb) protos into Rust.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/ugnos/v1/ugnos.proto"], &["proto"])?;

    // Prometheus Remote Write: wire-format compatible protos (no gRPC).
    prost_build::compile_protos(
        &["proto/prompb/types.proto", "proto/prompb/remote.proto"],
        &["proto"],
    )?;
    Ok(())
}
