use std::io::Result;
fn main() -> Result<()> {
    let mut compiler = prost_build::Config::new();
    
    #[cfg(feature = "python-module")]
    compiler.message_attribute(".", "#[pyclass(set_all, get_all)]");

    compiler.compile_protos(&[
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