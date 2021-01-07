use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use log::{debug, error};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseConfig {
    pub config_path: PathBuf,
    pub log_path: PathBuf,
    pub his_path: PathBuf,
    pub server_url: String,
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self {
            config_path: dirs::home_dir().unwrap().join(".whale"),
            log_path: dirs::cache_dir().unwrap().join("whale.log"),
            his_path: dirs::cache_dir().unwrap().join("whale.his"),
            server_url: "0.0.0.0:3697".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhaleConfig {
    pub base: BaseConfig,
}

impl Default for WhaleConfig {
    fn default() -> Self {
        Self {
            base: BaseConfig::default(),
        }
    }
}

impl WhaleConfig {
    pub fn new(config_path: PathBuf) -> WhaleConfig {
        let mut file = File::open(&config_path).unwrap();
        let mut contents = String::new();
        match file.read_to_string(&mut contents) {
            Ok(_) => debug!("Load config from [{}]", &config_path.display()),
            Err(err) => error!("Failed to read config from[{}]", err),
        };
        let config: WhaleConfig = toml::from_str(contents.as_str()).unwrap();

        let mut new = WhaleConfig::default();
        match config {
            WhaleConfig { base, .. } => match base {
                BaseConfig {
                    config_path,
                    log_path,
                    his_path,
                    server_url,
                    ..
                } => {
                    new.base.config_path = config_path;
                    new.base.log_path = log_path;
                    new.base.his_path = his_path;
                    new.base.server_url = server_url;
                }
            },
        }
        return new;
    }
}
