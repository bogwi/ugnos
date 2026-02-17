//! Compiles `proto/ugnos/v1/ugnos.proto` into Rust for gRPC server and client.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/ugnos/v1/ugnos.proto"], &["proto"])?;
    Ok(())
}
