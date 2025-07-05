use log::error;
use std::collections::HashSet;

use crossbeam::channel::Sender;
use wg_2024::controller::DroneCommand;
use wg_2024::network::NodeId;
use wg_2024::packet::Packet;

use crate::network::NodeCommand;
use crate::utils::ClientType;
use crate::utils::ServerType;

pub struct Drone {
    id: NodeId,
    cmd_send: Sender<DroneCommand>,
    pdr: f32,
    group_name: String,
    neighbours: HashSet<NodeId>,
}

pub trait NetworkObject {
    fn get_neighbours(&self) -> &HashSet<NodeId>;
    fn add_neighbour(&mut self, connected_id: NodeId, pkg_sender: Sender<Packet>);
    fn remove_neighbour(&mut self, connected_id: NodeId) -> Result<(), String>;
    fn get_label(&self) -> String;
    fn get_type_string(&self) -> String;
}

impl Drone {
    pub fn new(id: NodeId, cmd_send: Sender<DroneCommand>, pdr: f32, group_name: String) -> Self {
        Self {
            id,
            cmd_send,
            pdr,
            group_name,
            neighbours: HashSet::new(),
        }
    }

    pub fn get_cmd_send(&self) -> &Sender<DroneCommand> {
        &self.cmd_send
    }

    pub fn get_pdr(&self) -> f32 {
        self.pdr
    }

    pub fn set_pdr(&mut self, pdr: f32) -> Result<(), String> {
        if !(0.0..=1.0).contains(&pdr) {
            return Err(format!("Invalid PDR value: {}", pdr));
        }
        if let Err(e) = self.cmd_send.send(DroneCommand::SetPacketDropRate(pdr)) {
            return Err(format!(
                "Failed to set PDR for drone {}: {}",
                self.group_name, e
            ));
        };
        self.pdr = pdr;
        Ok(())
    }
}

impl NetworkObject for Drone {
    fn get_neighbours(&self) -> &HashSet<NodeId> {
        &self.neighbours
    }

    fn add_neighbour(&mut self, neighbour: NodeId, pkg_sender: Sender<Packet>) {
        if !self.neighbours.insert(neighbour) {
            error!(
                "Neighbour {} already exists in drone {}",
                neighbour, self.group_name
            );
        }
        self.cmd_send
            .send(DroneCommand::AddSender(neighbour, pkg_sender))
            .unwrap();
    }

    fn remove_neighbour(&mut self, connected_id: NodeId) -> Result<(), String> {
        if self.neighbours.remove(&connected_id) {
            self.cmd_send
                .send(DroneCommand::RemoveSender(connected_id))
                .unwrap();
            Ok(())
        } else {
            Err(format!(
                "Neighbour {} not found in drone {}",
                connected_id, self.group_name
            ))
        }
    }

    fn get_label(&self) -> String {
        format!("Drone {} ({})", self.id, self.group_name)
    }

    fn get_type_string(&self) -> String {
        "drone".to_string()
    }
}

pub struct Server {
    id: NodeId,
    cmd_send: Sender<NodeCommand>,
    server_type: ServerType,
    neighbours: HashSet<NodeId>,
}

impl Server {
    pub fn new(id: NodeId, cmd_send: Sender<NodeCommand>, server_type: ServerType) -> Self {
        Self {
            id,
            cmd_send,
            server_type,
            neighbours: HashSet::new(),
        }
    }

    pub fn get_cmd_send(&self) -> &Sender<NodeCommand> {
        &self.cmd_send
    }
}

impl NetworkObject for Server {
    fn get_neighbours(&self) -> &HashSet<NodeId> {
        &self.neighbours
    }

    fn add_neighbour(&mut self, neighbour: NodeId, pkg_channel: Sender<Packet>) {
        if !self.neighbours.insert(neighbour) {
            error!("Neighbour {} already exists for this server", neighbour);
        }
        self.cmd_send
            .send(NodeCommand::AddNeighbour((neighbour, pkg_channel)))
            .unwrap();
    }

    fn remove_neighbour(&mut self, connected_id: NodeId) -> Result<(), String> {
        if self.neighbours.remove(&connected_id) {
            self.cmd_send
                .send(NodeCommand::RemoveNeighbour(connected_id))
                .unwrap();
            Ok(())
        } else {
            Err(format!("Neighbour {} not found in server", connected_id))
        }
    }

    fn get_label(&self) -> String {
        format!(
            "Server {} ({})",
            self.id,
            match self.server_type {
                ServerType::Communication => "Communication Server",
                ServerType::Content => "Content Server",
            }
        )
    }

    fn get_type_string(&self) -> String {
        "server".to_string()
    }
}

pub struct Client {
    id: NodeId,
    cmd_send: Sender<NodeCommand>,
    client_type: ClientType,
    neighbours: HashSet<NodeId>,
}

impl Client {
    pub fn new(id: NodeId, cmd_send: Sender<NodeCommand>, client_type: ClientType) -> Self {
        Self {
            id,
            cmd_send,
            client_type,
            neighbours: HashSet::new(),
        }
    }

    pub fn get_cmd_send(&self) -> &Sender<NodeCommand> {
        &self.cmd_send
    }
}

impl NetworkObject for Client {
    fn get_neighbours(&self) -> &HashSet<NodeId> {
        &self.neighbours
    }

    fn add_neighbour(&mut self, neighbour: NodeId, pkg_channel: Sender<Packet>) {
        if !self.neighbours.insert(neighbour) {
            error!("Neighbour {} already exists for this server", neighbour);
        }
        self.cmd_send
            .send(NodeCommand::AddNeighbour((neighbour, pkg_channel)))
            .unwrap();
    }

    fn remove_neighbour(&mut self, connected_id: NodeId) -> Result<(), String> {
        if self.neighbours.remove(&connected_id) {
            self.cmd_send
                .send(NodeCommand::RemoveNeighbour(connected_id))
                .unwrap();
            Ok(())
        } else {
            Err(format!("Neighbour {} not found in server", connected_id))
        }
    }

    fn get_label(&self) -> String {
        format!(
            "Client {} ({})",
            self.id,
            match self.client_type {
                ClientType::Web => "Web Client",
                ClientType::Chat => "Chat Client",
            }
        )
    }

    fn get_type_string(&self) -> String {
        "client".to_string()
    }
}
