use serde::Deserialize;

use wg_2024::network::NodeId;

pub const MAIN_CONFIG_FILE: &str = "examples/main.toml";
pub const CONFIGURATIONS_DIR: &str = "examples/config/";

#[derive(Debug, Clone, Deserialize)]
pub struct Drone {
    pub id: NodeId,
    pub connected_node_ids: Vec<NodeId>,
    pub pdr: f32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Client {
    pub id: NodeId,
    pub connected_drone_ids: Vec<NodeId>,
    pub client_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Server {
    pub id: NodeId,
    pub connected_drone_ids: Vec<NodeId>,
    pub server_type: String,
    pub base_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub drone: Vec<Drone>,
    pub client: Vec<Client>,
    pub server: Vec<Server>,
}

#[derive(Deserialize)]
pub struct Configurations {
    pub configuration: Vec<Configuration>,
}

#[derive(Deserialize)]
pub struct Configuration {
    pub id: u8,
    pub name: String,
    pub description: String,
    pub file_name: String,
}
