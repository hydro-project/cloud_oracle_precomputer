fn main() {
    /* if let Some(python_home) = std::env::var_os("CONDA_PREFIX") {
        //println!("cargo:rustc-link-search=native={}/lib", python_home.to_str());
        println!("cargo:rustc-flags=-l dylib=yourlib -L /path/to/your/library");
        //RUSTFLAGS="-C link-args=-Wl,-rpath,$CONDA_PREFIX/lib"
        //println!("cargo:rustc-link-lib=dylib=yourlib");
    } */

    // Get the CONDA_PREFIX path
    let conda_prefix = std::env::var("CONDA_PREFIX").unwrap();
    // Construct the rpath flag
    let rpath_flag = format!("-Wl,-rpath,{}", conda_prefix);

    // Print the flags for cargo
    //println!("cargo:rustc-link-search=native={}", conda_prefix);
    //println!("cargo:rustc-flags=-C link-args={}", rpath_flag);
    println!("cargo:rustc-link-arg=-Wl,-rpath={}", conda_prefix);
    
}
