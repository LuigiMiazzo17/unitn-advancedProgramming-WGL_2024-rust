use log::error;
use std::fmt::Display;
use std::fs;
use wg_2024::network::NodeId;

use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase", tag = "node_type", content = "sub_type")]
pub enum NodeType {
    Drone(Option<DroneType>),
    Client(ClientType),
    Server(ServerType),
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ClientType {
    Web,
    Chat,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ServerType {
    Content,
    Communication,
}

#[derive(Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DroneType {
    Rust,
    BagelBomber,
    BobryWLocie,
    DRONE,
    LedronJames,
    Lockheedrustin,
    RustDoIt,
    RustRoveri,
    Rustbusters,
    RustyDrones,
    Skylink,
}

impl Display for DroneType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DroneType::RustyDrones => write!(f, "Rusty Drones"),
            DroneType::Lockheedrustin => write!(f, "Lockheed Rustin"),
            DroneType::Rustbusters => write!(f, "Rust Busters"),
            DroneType::Skylink => write!(f, "Sky Link"),
            DroneType::BagelBomber => write!(f, "Bagel Bomber"),
            DroneType::BobryWLocie => write!(f, "Bober"),
            DroneType::RustRoveri => write!(f, "Rust Roveri"),
            DroneType::RustDoIt => write!(f, "Rust Do It"),
            DroneType::LedronJames => write!(f, "Ledron James"),
            DroneType::DRONE => write!(f, "D.R.O.N.E"),
            DroneType::Rust => write!(f, "Rust"),
        }
    }
}

#[derive(Deserialize)]
pub struct NodeData {
    pub label: String,
    pub node_type: String,
    pub neighbours: Vec<(NodeId, String, String)>,
    pub pdr: Option<f32>,
    pub stats: Option<(u32, u32)>,
}

#[derive(Deserialize)]
pub struct Edge {
    pub from_id: u8,
    pub to_id: u8,
}

#[derive(Deserialize)]
pub struct Pdr {
    pub pdr: f32,
}

#[derive(Serialize)]
pub struct Configuration {
    pub id: u8,
    pub name: String,
    pub description: String,
    pub is_active: bool,
}

#[derive(Deserialize, Debug)]
pub struct ConfigurationChange {
    pub id: u8,
}

// Convert drone dependency name to display name
// e.g., "wg_drone_bobry_w_locie" -> "Bobry W Locie"
pub fn format_drone_name(dep_name: &str) -> String {
    if let Some(drone_part) = dep_name.strip_prefix("wg_drone_") {
        drone_part
            .split('_')
            .map(|word| {
                if word.is_empty() {
                    String::new()
                } else {
                    let mut chars = word.chars();
                    match chars.next() {
                        None => String::new(),
                        Some(first) => {
                            first.to_uppercase().collect::<String>()
                                + &chars.as_str().to_lowercase()
                        }
                    }
                }
            })
            .filter(|word| !word.is_empty())
            .collect::<Vec<String>>()
            .join(" ")
    } else {
        dep_name.to_string()
    }
}

// Helper function to read image file and convert to base64 data URL
pub fn image_to_base64(file_path: &str) -> Option<String> {
    match fs::read(file_path) {
        Ok(image_data) => {
            let base64_data = general_purpose::STANDARD.encode(&image_data);
            // Determine MIME type based on file extension
            let mime_type = if file_path.ends_with(".png") {
                "image/png"
            } else if file_path.ends_with(".jpg") || file_path.ends_with(".jpeg") {
                "image/jpeg"
            } else {
                "image/png" // default
            };
            Some(format!("data:{};base64,{}", mime_type, base64_data))
        }
        Err(e) => {
            error!("Failed to read image file {}: {}", file_path, e);
            None
        }
    }
}

// fn old_main() -> anyhow::Result<()> {
//     env_logger::init();

//     let config = parse_config("examples/config/base.toml")?;

//     let (controller_drones, controller_server, drone_event_recv, mut d_handles, mut s_handles) =
//         spawn_network(config)?;

//     let mut controller = SimulationController {
//         drones: controller_drones,
//         servers: controller_server,
//         drone_event_recv,
//         d_handles,
//         s_handles,
//     };

//     thread::sleep(Duration::from_secs(1));

//     // controller_server
//     //     .get(&10)
//     //     .unwrap()
//     //     .send(NodeCommand::SendMessage((
//     //         20,
//     //         Message::Request(Request::WritePublicFile("file2".to_string(), "ciaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaduhapjfbewphbgerpauhorbfhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhho".to_string())),
//     //     )))?;

//     controller_server
//         .get(&20)
//         .unwrap()
//         .send(NodeCommand::SendMessage(
//             SimControllerMessage::SendMessageToPeer(
//                 10,
//                 Message::Request(Request::ServerType))))?;

//     // controller_server
//     //     .get(&10)
//     //     .unwrap()
//     //     .send(NodeCommand::SendMessage(
//     //         SimControllerMessage::SendMessageToPeer(
//     //             20,
//     //             Message::Request(Request::DeleteChat(6852741936126412825)),
//     //         ),
//     //     ))?;

//     thread::sleep(Duration::from_secs(2));

//     controller.crash_all()?;

//     for c in controller_server.iter() {
//         c.1.send(NodeCommand::Quit)?;
//     }

//     while let Some(handle) = s_handles.pop() {
//         let _ = handle.join();
//     }

//     Ok(())
// }
