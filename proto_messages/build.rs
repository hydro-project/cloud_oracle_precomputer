use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&[
        "./proto/run.proto",
        "./proto/decision.proto",
        "./proto/assignment.proto",
        "./proto/optimal_by_optimizer.proto",
        "./proto/scheme.proto",
        "./proto/tier_advise.proto",
        "./proto/wrapper.proto",
        ],
    &["./proto/"])?;
    Ok(())
}