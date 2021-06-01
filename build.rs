fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/store.proto")?;
    tonic_build::compile_protos("proto/replication.proto")?;
    Ok(())
}
