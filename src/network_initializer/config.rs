use serde::Deserialize;

use wg_2024::network::NodeId;

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
