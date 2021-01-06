use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WhaleConfig {
    pub config_path: PathBuf,
    pub log_path: PathBuf,
    pub his_path: PathBuf,
    pub server_url: String,
}

impl Default for WhaleConfig {
    fn default() -> Self {
        Self {
            config_path: dirs::home_dir().unwrap().join(".whale.toml"),
            log_path: dirs::cache_dir().unwrap().join("whale.log"),
            his_path: dirs::cache_dir().unwrap().join("whale.his"),
            server_url: "0.0.0.0:3697".to_string(),
        }
    }
}

impl WhaleConfig {
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            log_path: dirs::cache_dir().unwrap().join("whale.log"),
            his_path: dirs::cache_dir().unwrap().join("whale.his"),
            server_url: "0.0.0.0:3697".to_string(),
        }
    }
}
