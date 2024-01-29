use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
pub struct Args {
    /// Regex selector for regions
    #[clap(short, long)]
    pub region_selector: String,

    /// Regex selector for regions
    #[clap(long)]
    pub object_store_selector: String,

    /// Replication factor
    #[clap(long)]
    pub replication_factor: usize,

    /// Maximal replication factor
    #[clap(long)]
    pub replication_factor_max: Option<usize>,

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

    #[clap(long)]
    pub latency_file: Option<PathBuf>,

    #[clap(long)]
    pub latency_slo: Option<f64>,

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

    #[clap(long)]
    pub worker_id: usize,

    #[clap(long)]
    pub num_workers: usize,

    #[clap(long)]
    pub optimizer: Option<String>,

    #[clap(long)]
    pub use_clarkson: bool
}
