use crate::network::message::{Message, Request};
use crate::network::{ClientControlMessage, Node, NodeCommand, SimControllerMessage};
use crate::network_initializer::Config;
use crate::network_initializer::{CONFIGURATIONS_DIR, MAIN_CONFIG_FILE};
use crate::simulation_controller::network_object::{Client, Drone, NetworkObject, Server};
use crate::utils::*;

use crossbeam::channel::{Receiver, Sender, unbounded};
use log::{debug, error, info};
use std::collections::{HashMap, HashSet};
use std::thread::JoinHandle;
use std::{fs, thread};

use wg_2024::controller::{DroneCommand, DroneEvent};
use wg_2024::drone::Drone as DroneTrait;
use wg_2024::network::NodeId;
use wg_2024::packet::Packet;
use wg_2024_rust::drone::RustDrone as Rust;
use wg_drone_bagel_bomber::BagelBomber as BagelBomberDrone;
use wg_drone_bobry_w_locie::drone::BoberDrone;
use wg_drone_d_r_o_n_e::MyDrone as D_R_O_N_E_Drone;
use wg_drone_ledron_james::Drone as LedronJamesDrone;
use wg_drone_lockheedrustin::LockheedRustin as LockheedRustinDrone;
use wg_drone_rust_do_it::RustDoIt as RustDoItDrone;
use wg_drone_rust_roveri::RustRoveri as RustRoveriDrone;
use wg_drone_rustbusters::RustBustersDrone;
use wg_drone_rusty_drones::RustyDrone as Rusty_Drones_Drone;
use wg_drone_skylink::SkyLinkDrone;

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

    /// Thread handles for spawned drone processes
    pub d_handles: Vec<JoinHandle<()>>,

    /// Thread handles for spawned server processes
    pub s_handles: Vec<JoinHandle<()>>,

    /// Thread handles for spawned client processes
    pub c_handles: Vec<JoinHandle<()>>,

    /// Packet channels for communication with nodes - HashMap<NodeId, Sender<Packet>>
    pub packet_channels: HashMap<NodeId, Sender<Packet>>,

    /// Active configuration
    active_configuration: u8,
}

impl SimulationController {
    pub fn new(config_id: u8) -> Result<Self, String> {
        let (client_event_send, client_event_recv) = unbounded();

        let mut sim = Self {
            drones: HashMap::new(),
            servers: HashMap::new(),
            clients: HashMap::new(),
            client_event_send,
            client_event_recv,
            d_handles: Vec::new(),
            s_handles: Vec::new(),
            c_handles: Vec::new(),
            packet_channels: HashMap::new(),
            active_configuration: config_id,
        };

        sim.spawn_network_from_config(config_id)?;

        Ok(sim)
    }

    fn destroy_netowrk(&mut self) -> Result<(), String> {
        for (id, mut drone) in std::mem::take(&mut self.drones).into_iter() {
            info!(target: LOG_TARGET, "Crashing drone with ID: {}", id);
            drone.crash()?;

            if let Err(e) = drone.get_cmd_send().send(DroneCommand::Crash) {
                error!(target: LOG_TARGET, "Failed to crash drone {}: {}", id, e);
                return Err(format!("Failed to crash drone {}: {}", id, e));
            }
        }

        for (id, server) in std::mem::take(&mut self.servers).iter_mut() {
            info!(target: LOG_TARGET, "Quitting server with ID: {}", id);
            if let Err(e) = server.get_cmd_send().send(NodeCommand::Quit) {
                error!(target: LOG_TARGET, "Failed to quit server {}: {}", id, e);
                return Err(format!("Failed to quit server {}: {}", id, e));
            }
        }

        for (id, client) in std::mem::take(&mut self.clients).iter_mut() {
            info!(target: LOG_TARGET, "Quitting client with ID: {}", id);
            if let Err(e) = client.get_cmd_send().send(NodeCommand::Quit) {
                error!(target: LOG_TARGET, "Failed to quit client {}: {}", id, e);
                return Err(format!("Failed to quit client {}: {}", id, e));
            }
        }

        self.d_handles.clear();
        self.s_handles.clear();
        self.c_handles.clear();

        self.packet_channels.clear();

        Ok(())
    }

    pub fn spawn_network_from_config(&mut self, id: u8) -> Result<(), String> {
        if let Err(e) = self.destroy_netowrk() {
            error!(target: LOG_TARGET, "Failed to destroy existing network: {}", e);
            return Err(format!("Failed to destroy existing network: {}", e));
        }

        fn safe_edge_add(hs: &mut HashSet<(NodeId, NodeId)>, edge: (NodeId, NodeId)) {
            if hs.contains(&(edge.1, edge.0)) {
                debug!(target: LOG_TARGET, "Edge {:?} already exists, skipping.", edge);
            } else {
                hs.insert(edge);
            }
        }

        let configurations = self.get_available_configurations();
        let path = match configurations.configuration.iter().find(|c| c.id == id) {
            Some(config) => {
                format!("{}{}", CONFIGURATIONS_DIR, config.file_name)
            }
            None => {
                error!(target: LOG_TARGET, "Configuration with ID {} not found!", id);
                return Err(format!("Configuration with ID {} not found!", id));
            }
        };

        let file_str = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) => {
                error!(target: LOG_TARGET, "Failed to read configuration file: {}", e);
                return Err(format!("Failed to read configuration file: {}", e));
            }
        };
        let config: Config = match toml::from_str(&file_str) {
            Ok(config) => config,
            Err(e) => {
                error!(target: LOG_TARGET, "Failed to parse configuration file: {}", e);
                return Err(format!("Failed to parse configuration file: {}", e));
            }
        };

        let mut edges = HashSet::new();
        for drone_cfg in config.drone.into_iter() {
            if let Err(e) = self.add_drone(Some(drone_cfg.id), None, Some(drone_cfg.pdr)) {
                error!(target: LOG_TARGET, "Failed to add drone {}: {}", drone_cfg.id, e);
                return Err(format!("Failed to add drone {}: {}", drone_cfg.id, e));
            }
            for connected_id in drone_cfg.connected_node_ids.into_iter() {
                safe_edge_add(&mut edges, (drone_cfg.id, connected_id));
            }
        }

        for server_cfg in config.server.into_iter() {
            let server_type = match server_cfg.server_type.as_str() {
                "Content" => ServerType::Content,
                "Communication" => ServerType::Communication,
                _ => panic!("Unknown server type: {}", server_cfg.server_type),
            };

            if let Err(e) = self.add_server(Some(server_cfg.id), server_type) {
                error!(target: LOG_TARGET, "Failed to add server {}: {}", server_cfg.id, e);
                return Err(format!("Failed to add server {}: {}", server_cfg.id, e));
            }

            for connected_id in server_cfg.connected_drone_ids.into_iter() {
                safe_edge_add(&mut edges, (server_cfg.id, connected_id));
            }
        }

        for client_cfg in config.client.into_iter() {
            let client_type = match client_cfg.client_type.as_str() {
                "ChatClient" => ClientType::Chat,
                "WebBrowser" => ClientType::Web,
                _ => panic!("Unknown client type: {}", client_cfg.client_type),
            };

            if let Err(e) = self.add_client(Some(client_cfg.id), client_type) {
                error!(target: LOG_TARGET, "Failed to add client {}: {}", client_cfg.id, e);
                return Err(format!("Failed to add client {}: {}", client_cfg.id, e));
            }

            for connected_id in client_cfg.connected_drone_ids.into_iter() {
                safe_edge_add(&mut edges, (client_cfg.id, connected_id));
            }
        }

        for edge in edges.into_iter() {
            if let Err(e) = self.add_edge(edge.0, edge.1) {
                debug!("Failed to add edge {:?}: {}", edge, e);
                return Err(format!("Failed to add edge {:?}: {}", edge, e));
            }
        }

        self.active_configuration = id;

        Ok(())
    }

    fn get_id(&self, preference: Option<NodeId>) -> u8 {
        let mut id = preference.unwrap_or(0);
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

    pub fn send_request(&self, id1: NodeId, id2: NodeId, request: Request) -> anyhow::Result<()> {
        info!(target: LOG_TARGET, "Sending message from {} to {}", id1, id2);
        let client = match self.clients.get(&id1) {
            Some(client) => client,
            None => {
                error!(target: LOG_TARGET, "Client with ID {} not found!", id1);
                return Err(anyhow::anyhow!("Client with ID {} not found!", id1));
            }
        };

        client.get_cmd_send().send(NodeCommand::SendMessage(
            SimControllerMessage::SendMessageToPeer(id2, Message::Request(request)),
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

    pub fn get_node_data(&mut self, id: NodeId) -> Result<NodeData, String> {
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
            let node_type = node.get_type_string();

            let (pdr, stats) = if let Some(d) = self.drones.get_mut(&id) {
                d.update_events();
                (Some(d.get_pdr()), Some(d.get_stats()))
            } else {
                (None, None)
            };
            let subtype = if let Some(s) = self.servers.get_mut(&id) {
                Some(s.get_subtype_string())
            } else {
                self.clients.get_mut(&id).map(|c| c.get_subtype_string())
            };

            Ok(NodeData {
                label,
                node_type,
                neighbours,
                pdr,
                stats,
                subtype,
            })
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

            // TODO: Join hanlde
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

    fn choose_a_drone(
        &self,
        id: NodeId,
        event_send: Sender<DroneEvent>,
        packet_recv: Receiver<Packet>,
        packet_send: HashMap<NodeId, Sender<Packet>>,
        pdr: f32,
        group: Option<DroneType>,
    ) -> (Box<dyn DroneTrait>, Sender<DroneCommand>, DroneType) {
        // we need to check all the drones in self.drones, and return the one who has the least
        // occurrences of the same type
        let (controller_send, controller_recv) = unbounded();

        let group = match group {
            Some(g) => g,
            None => {
                let mut drone_types_count: HashMap<DroneType, usize> = HashMap::new();

                drone_types_count.insert(DroneType::RustyDrones, 0);
                drone_types_count.insert(DroneType::BagelBomber, 0);
                drone_types_count.insert(DroneType::BobryWLocie, 0);
                drone_types_count.insert(DroneType::DRONE, 0);
                drone_types_count.insert(DroneType::LedronJames, 0);
                drone_types_count.insert(DroneType::Lockheedrustin, 0);
                drone_types_count.insert(DroneType::RustDoIt, 0);
                drone_types_count.insert(DroneType::Rustbusters, 0);
                drone_types_count.insert(DroneType::RustRoveri, 0);
                drone_types_count.insert(DroneType::Skylink, 0);
                drone_types_count.insert(DroneType::Rust, 0);

                for drone in self.drones.values() {
                    *drone_types_count.entry(drone.get_group()).or_insert(0) += 1;
                }

                drone_types_count
                    .iter()
                    .min_by_key(|&(_, count)| count)
                    .map(|(group, _)| group)
                    .unwrap_or(&DroneType::RustyDrones)
                    .clone()
            }
        };

        let drone: Box<dyn DroneTrait> = match group {
            DroneType::BagelBomber => Box::new(BagelBomberDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::BobryWLocie => Box::new(BoberDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::DRONE => Box::new(D_R_O_N_E_Drone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::LedronJames => Box::new(LedronJamesDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::Lockheedrustin => Box::new(LockheedRustinDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::RustDoIt => Box::new(RustDoItDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::Rustbusters => Box::new(RustBustersDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::RustRoveri => Box::new(RustRoveriDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::RustyDrones => Box::new(Rusty_Drones_Drone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::Skylink => Box::new(SkyLinkDrone::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
            DroneType::Rust => Box::new(Rust::new(
                id,
                event_send,
                controller_recv,
                packet_recv,
                packet_send,
                pdr,
            )),
        };

        info!(target: LOG_TARGET, "Chose drone type: {}", group);
        (drone, controller_send, group)
    }

    /// Add a new drone to the simulation
    pub fn add_drone(
        &mut self,
        id: Option<NodeId>,
        group: Option<DroneType>,
        pdr: Option<f32>,
    ) -> anyhow::Result<u8> {
        let id = self.get_id(id);
        let pdr = pdr.unwrap_or(1.0);
        // Create packet channel for the new server
        let (packet_send, packet_recv) = unbounded();
        let (event_send, event_recv) = unbounded();

        let (mut drone_obj, controller_drone_send, group) =
            self.choose_a_drone(id, event_send, packet_recv, HashMap::new(), pdr, group);

        self.packet_channels.insert(id, packet_send);

        // Insert server controller before spawning thread
        self.drones.insert(
            id,
            Drone::new(id, controller_drone_send, pdr, event_recv, group),
        );

        self.d_handles
            .push(
                thread::Builder::new()
                    .name(format!("drone{}", id))
                    .spawn(move || {
                        drone_obj.run();
                    })?,
            );

        Ok(id)
    }

    /// Add a new server to the simulation
    pub fn add_server(
        &mut self,
        id: Option<NodeId>,
        server_type: ServerType,
    ) -> anyhow::Result<u8> {
        let id = self.get_id(id);
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

    pub fn add_client(
        &mut self,
        id: Option<NodeId>,
        client_type: ClientType,
    ) -> anyhow::Result<u8> {
        let id = self.get_id(id);
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

    pub fn set_pdr(&mut self, id: NodeId, pdr: f32) -> Result<(), String> {
        if let Some(drone) = self.drones.get_mut(&id) {
            if let Err(e) = drone.set_pdr(pdr) {
                error!(target: LOG_TARGET, "Failed to set PDR for drone {}: {}", id, e);
                Err(format!("Failed to set PDR for drone {}: {}", id, e))
            } else {
                Ok(())
            }
        } else {
            error!(target: LOG_TARGET, "Drone with ID {} not found!", id);
            Err(format!("Drone with ID {} not found!", id))
        }
    }

    fn get_available_configurations(&self) -> crate::network_initializer::Configurations {
        let file_str = match fs::read_to_string(MAIN_CONFIG_FILE) {
            Ok(content) => content,
            Err(e) => {
                error!(target: LOG_TARGET, "Failed to read configuration file: {}", e);
                panic!("Failed to read configuration file: {}", e);
            }
        };

        let conf: Result<crate::network_initializer::Configurations, _> = toml::from_str(&file_str);
        match conf {
            Ok(config) => config,
            Err(e) => {
                error!(target: LOG_TARGET, "Failed to parse configuration file: {}", e);
                panic!("Failed to parse configuration file: {}", e);
            }
        }
    }

    pub fn get_deserializable_configurations(&self) -> Vec<crate::utils::Configuration> {
        let conf = self.get_available_configurations();

        let mut deserializable_configurations = Vec::new();
        for configuration in conf.configuration {
            deserializable_configurations.push(crate::utils::Configuration {
                is_active: configuration.id == self.active_configuration,
                id: configuration.id,
                name: configuration.name,
                description: configuration.description,
            });
        }

        info!(target: LOG_TARGET, "Loaded {} configurations", deserializable_configurations.len());
        deserializable_configurations
    }
}
