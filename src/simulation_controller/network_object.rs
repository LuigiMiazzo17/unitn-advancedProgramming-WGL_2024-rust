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
    cmd_send: Sender<DroneCommand>,
    pdr: f32,
    group_name: String,
    neighbours: HashSet<NodeId>,
}

pub trait NetworkObject {
    fn get_neighbours(&self) -> &HashSet<NodeId>;
    fn add_neighbour(&mut self, connected_id: NodeId, pkg_sender: Sender<Packet>);
}

impl Drone {
    pub fn new(cmd_send: Sender<DroneCommand>, pdr: f32, group_name: String) -> Self {
        Self {
            cmd_send,
            pdr,
            group_name,
            neighbours: HashSet::new(),
        }
    }

    pub fn get_cmd_send(&self) -> &Sender<DroneCommand> {
        &self.cmd_send
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
}

pub struct Server {
    cmd_send: Sender<NodeCommand>,
    server_type: ServerType,
    neighbours: HashSet<NodeId>,
}

impl Server {
    pub fn new(cmd_send: Sender<NodeCommand>, server_type: ServerType) -> Self {
        Self {
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
}

pub struct Client {
    cmd_send: Sender<NodeCommand>,
    client_type: ClientType,
    neighbours: HashSet<NodeId>,
}

impl Client {
    pub fn new(cmd_send: Sender<NodeCommand>, client_type: ClientType) -> Self {
        Self {
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
}
