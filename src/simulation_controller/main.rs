use crate::network::message::{Message, Request};
use crate::network::{ClientControlMessage, Node, NodeCommand, SimControllerMessage};
use crate::simulation_controller::network_object::{Client, Drone, NetworkObject, Server};
use crate::utils::*;

use crossbeam::channel::{Receiver, Sender, unbounded};
use log::{error, info};
use std::collections::HashMap;
use std::thread;
use std::thread::JoinHandle;
use wg_2024::controller::{DroneCommand, DroneEvent};
use wg_2024::drone::Drone as DroneTrait;
use wg_2024::network::NodeId;
use wg_2024::packet::Packet;
use wg_2024_rust::drone::RustDrone;

const LOG_TARGET: &str = "simulation_controller";

/// Central simulation controller that manages all network nodes and their communication channels.
///
/// This controller maintains bidirectional communication with network nodes:
/// - **Commands to nodes**: `drones` and `servers` contain senders for sending commands from controller to nodes
/// - **Events from nodes**: `drone_event_recv` receives events/responses from drones back to controller
///
/// Thread handles (`d_handles`, `s_handles`) allow graceful shutdown of spawned node threads.
pub struct SimulationController {
    /// Commands from controller to drones - HashMap<NodeId, Sender<DroneCommand>>
    pub drones: HashMap<NodeId, Drone>,

    /// Commands from controller to servers - HashMap<NodeId, Sender<NodeCommand>
    pub servers: HashMap<NodeId, Server>,

    /// Commands from controller to clients - HashMap<NodeId, Sender<NodeCommand>
    pub clients: HashMap<NodeId, Client>,

    // Events from clients to controller - Sender<ClientControlMessage>
    pub client_event_send: Sender<ClientControlMessage>,

    // Events from clients to controller - Sender<ClientControlMessage>
    pub client_event_recv: Receiver<ClientControlMessage>,

    /// Events from drones to controller - Sender<DroneEvent>
    pub drone_event_send: Sender<DroneEvent>,

    /// Events from the drones - Receiver<DroneEvent>
    pub drone_event_recv: Receiver<DroneEvent>,

    /// Thread handles for spawned drone processes
    pub d_handles: Vec<JoinHandle<()>>,

    /// Thread handles for spawned server processes
    pub s_handles: Vec<JoinHandle<()>>,

    /// Thread handles for spawned client processes
    pub c_handles: Vec<JoinHandle<()>>,

    /// Packet channels for communication with nodes - HashMap<NodeId, (Sender<Packet>, Receiver<Packet>)>
    pub packet_channels: HashMap<NodeId, Sender<Packet>>,
}

impl SimulationController {
    fn get_id(&self) -> u8 {
        let mut id: u8 = 1;
        while self.drones.contains_key(&id)
            || self.servers.contains_key(&id)
            || self.clients.contains_key(&id)
        {
            id += 1;
        }
        id
    }

    pub fn crash_all(&mut self) -> anyhow::Result<()> {
        for (_, drone) in self.drones.iter() {
            drone.get_cmd_send().send(DroneCommand::Crash)?;
        }
        Ok(())
    }

    pub fn send_message(&self, id1: NodeId, id2: NodeId) -> anyhow::Result<()> {
        info!(target: LOG_TARGET, "Sending message from {} to {}", id1, id2);
        self.servers
            .get(&id1)
            .unwrap()
            .get_cmd_send()
            .send(NodeCommand::SendMessage(
                SimControllerMessage::SendMessageToPeer(id2, Message::Request(Request::ServerType)),
            ))?;
        Ok(())
    }

    fn get_net_obj_from_id(&self, id: NodeId) -> Option<&dyn NetworkObject> {
        if let Some(drone) = self.drones.get(&id) {
            Some(drone)
        } else if let Some(server) = self.servers.get(&id) {
            Some(server)
        } else if let Some(client) = self.clients.get(&id) {
            Some(client)
        } else {
            None
        }
    }

    fn get_net_obj_from_id_mut(&mut self, id: NodeId) -> Option<&mut dyn NetworkObject> {
        if let Some(drone) = self.drones.get_mut(&id) {
            Some(drone)
        } else if let Some(server) = self.servers.get_mut(&id) {
            Some(server)
        } else if let Some(client) = self.clients.get_mut(&id) {
            Some(client)
        } else {
            None
        }
    }

    fn get_packet_sender(&self, id: NodeId) -> Option<Sender<Packet>> {
        self.packet_channels.get(&id).cloned()
    }

    pub fn add_edge(&mut self, from_id: NodeId, to_id: NodeId) -> Result<(), String> {
        info!(target: LOG_TARGET, "Adding edge from {} to {}", from_id, to_id);

        if from_id == to_id {
            error!(target: LOG_TARGET, "Cannot add edge from node {} to itself!", from_id);
            return Err(format!("Cannot add edge from node {} to itself!", from_id));
        }

        let from_pkg_sender = self.get_packet_sender(from_id);
        let to_pkg_sender = self.get_packet_sender(to_id);

        match (from_pkg_sender, to_pkg_sender) {
            (Some(from_pkg_sender), Some(to_pkg_sender)) => {
                self.get_net_obj_from_id_mut(from_id)
                    .unwrap()
                    .add_neighbour(to_id, to_pkg_sender);
                self.get_net_obj_from_id_mut(to_id)
                    .unwrap()
                    .add_neighbour(from_id, from_pkg_sender);
                Ok(())
            }
            (Some(_), None) => {
                error!(target: LOG_TARGET, "Node with ID {} not found!", to_id);
                Err(format!("Node with ID {} not found!", from_id))
            }
            (None, Some(_)) => {
                error!(target: LOG_TARGET, "Node with ID {} not found!", to_id);
                Err(format!("Node with ID {} not found!", to_id))
            }
            (None, None) => {
                error!(target: LOG_TARGET, "Nodes with IDs {} and {} not found!", from_id, to_id);
                Err(format!(
                    "Nodes with IDs {} and {} not found!",
                    from_id, to_id
                ))
            }
        }
    }

    pub fn delete_edge(&mut self, from_id: NodeId, to_id: NodeId) -> Result<(), String> {
        info!(target: LOG_TARGET, "Deleting edge from {} to {}", from_id, to_id);

        if from_id == to_id {
            error!(target: LOG_TARGET, "Cannot delete edge from node {} to itself!", from_id);
            return Err(format!(
                "Cannot delete edge from node {} to itself!",
                from_id
            ));
        }

        let res_from = match self.get_net_obj_from_id_mut(from_id) {
            Some(from) => from.remove_neighbour(to_id),
            None => {
                error!(target: LOG_TARGET, "Node with ID {} not found!", from_id);
                Err(format!("Node with ID {} not found!", from_id))
            }
        };

        let res_to = match self.get_net_obj_from_id_mut(to_id) {
            Some(to) => to.remove_neighbour(from_id),
            None => {
                error!(target: LOG_TARGET, "Node with ID {} not found!", to_id);
                Err(format!("Node with ID {} not found!", to_id))
            }
        };

        match (res_from, res_to) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(e), Ok(())) | (Ok(()), Err(e)) => Err(e),
            (Err(e), Err(f)) => Err(format!("{}; {}", e, f)),
        }
    }

    pub fn get_node_data(
        &self,
        id: NodeId,
    ) -> Result<(String, Vec<(NodeId, String, String)>, Option<f32>), String> {
        let node = self.get_net_obj_from_id(id);

        if let Some(node) = node {
            let neighbours: Vec<(NodeId, String, String)> = node
                .get_neighbours()
                .iter()
                .map(|id| {
                    (
                        *id,
                        self.get_net_obj_from_id(*id).unwrap().get_label(),
                        self.get_net_obj_from_id(*id).unwrap().get_type_string(),
                    )
                })
                .collect();
            let label = node.get_label();

            Ok((label, neighbours, None))
        } else {
            error!(target: LOG_TARGET, "Node with ID {} not found!", id);
            Err(format!("Node with ID {} not found!", id))
        }
    }

    pub fn delete_node(&mut self, id: NodeId) -> anyhow::Result<()> {
        info!(target: LOG_TARGET, "Deleting node with ID {}", id);

        // Remove the node from the packet channels
        self.packet_channels.remove(&id);

        // Remove the node from drones, servers, or clients
        if let Some(drone) = self.drones.remove(&id) {
            for neighbour_id in drone.get_neighbours() {
                if let Some(neighbour) = self.get_net_obj_from_id_mut(*neighbour_id) {
                    if let Err(e) = neighbour.remove_neighbour(id) {
                        error!(target: LOG_TARGET, "Failed to remove neighbour {} from drone {}: {}", id, neighbour_id, e);
                    }
                }
            }
            drone.get_cmd_send().send(DroneCommand::Crash)?;
            Ok(())
        } else if let Some(server) = self.servers.remove(&id) {
            for neighbour_id in server.get_neighbours() {
                if let Some(neighbour) = self.get_net_obj_from_id_mut(*neighbour_id) {
                    if let Err(e) = neighbour.remove_neighbour(id) {
                        error!(target: LOG_TARGET, "Failed to remove neighbour {} from drone {}: {}", id, neighbour_id, e);
                    }
                }
            }
            server.get_cmd_send().send(NodeCommand::Quit)?;

            Ok(())
        } else if let Some(client) = self.clients.remove(&id) {
            for neighbour_id in client.get_neighbours() {
                if let Some(neighbour) = self.get_net_obj_from_id_mut(*neighbour_id) {
                    if let Err(e) = neighbour.remove_neighbour(id) {
                        error!(target: LOG_TARGET, "Failed to remove neighbour {} from client {}: {}", id, neighbour_id, e);
                    }
                }
            }
            client.get_cmd_send().send(NodeCommand::Quit)?;
            Ok(())
        } else {
            error!(target: LOG_TARGET, "Node with ID {} not found!", id);
            Err(anyhow::anyhow!("Node with ID {} not found!", id))
        }
    }

    /// Add a new drone to the simulation
    pub fn add_drone(&mut self) -> anyhow::Result<u8> {
        let id = self.get_id();
        let (controller_drone_send, controller_drone_recv) = unbounded();
        let node_event_send: Sender<DroneEvent> = self.drone_event_send.clone();
        // Create packet channel for the new server
        let (packet_send, packet_recv) = unbounded();

        let drone = Drone::new(
            id,
            controller_drone_send.clone(),
            1.0,
            String::new(), //TODO: Set group name if needed
        );

        self.packet_channels.insert(id, packet_send);

        // Insert server controller before spawning thread
        self.drones.insert(id, drone);

        self.d_handles
            .push(
                thread::Builder::new()
                    .name(format!("drone{}", id))
                    .spawn(move || {
                        let mut drone = RustDrone::new(
                            id,
                            node_event_send,
                            controller_drone_recv,
                            packet_recv,
                            HashMap::new(),
                            1.0,
                        );

                        drone.run();
                    })?,
            );

        Ok(id)
    }

    /// Add a new server to the simulation
    pub fn add_server(&mut self, server_type: ServerType) -> anyhow::Result<u8> {
        let id = self.get_id();
        let (controller_server_send, controller_server_recv) = unbounded();

        // Create packet channel for the new server
        let (packet_send, packet_recv) = unbounded();
        self.packet_channels.insert(id, packet_send);

        // Insert server controller before spawning thread
        self.servers.insert(
            id,
            Server::new(id, controller_server_send, server_type.clone()),
        );

        self.s_handles
            .push(thread::Builder::new().name(format!("server{}", id)).spawn(
            move || {
                let mut server = match server_type {
                    ServerType::Communication => Node::new_communication_server(
                        id,
                        controller_server_recv,
                        packet_recv,
                        String::from("/tmp/rust"),
                    ),
                    _ => {
                        info!(target: LOG_TARGET, "Creating content server with ID: {}", id);
                        Node::new_content_server(
                            id,
                            controller_server_recv,
                            packet_recv,
                            String::from("/tmp/rust"),
                        )
                    }
                };

                server.run();
            },
        )?);

        Ok(id) // Return the ID of the newly created server
    }

    pub fn add_client(&mut self, client_type: ClientType) -> anyhow::Result<u8> {
        let id = self.get_id();
        let (controller_client_send, controller_client_recv) = unbounded();

        // Create packet channel for the new server
        let (packet_send, packet_recv) = unbounded();
        self.packet_channels.insert(id, packet_send);

        // Insert server controller before spawning thread
        self.clients.insert(
            id,
            Client::new(id, controller_client_send, client_type.clone()),
        );

        let client_event_send = self.client_event_send.clone();
        self.c_handles
            .push(thread::Builder::new().name(format!("client{}", id)).spawn(
            move || {
                let mut client = match client_type {
                    ClientType::Web => Node::new_browser_client(
                        id,
                        controller_client_recv,
                        packet_recv,
                        client_event_send,
                    ),
                    _ => {
                        info!(target: LOG_TARGET, "Creating content server with ID: {}", id);
                        Node::new_chat_client(
                            id,
                            controller_client_recv,
                            packet_recv,
                            client_event_send,
                        )
                    }
                };

                client.run();
            },
        )?);

        Ok(id) // Return the ID of the newly created server
    }
}
