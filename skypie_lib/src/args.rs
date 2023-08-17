use clap::{Parser};
use std::path::PathBuf;

#[derive(Debug, Parser)]
pub struct Args {
    /// Selector for regions
    #[clap(short, long)]
    pub region_selector: String,

    /// Replication factor
    #[clap(long)]
    pub replication_factor: usize,

    /// Output file name
    #[clap(short = 'o', long)]
    pub output_file_name: Option<PathBuf>,
    
    /// Output file name for candidates
    #[clap(long)]
    pub output_candidates_file_name: Option<PathBuf>,

    /// Batch size
    #[clap(short = 'b', long)]
    pub batch_size: usize,

    /// Network file
    #[clap(short = 'n', long)]
    pub network_file: PathBuf,

    /// Object store file
    #[clap(short = 's', long)]
    pub object_store_file: PathBuf,

    /// Output the candidates
    #[clap(long)]
    pub output_candidates: bool,

    #[clap(long)]
    pub redundancy_elimination_workers: u32,

    #[clap(long, short)]
    pub experiment_name: String,

    #[clap(long)]
    pub executor_name: Option<String>,

    #[clap(long)]
    pub influx_host: Option<String>,
}