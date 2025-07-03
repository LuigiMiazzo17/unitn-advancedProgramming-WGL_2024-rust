use crossbeam::channel::{unbounded, Receiver, Sender};
use std::collections::HashMap;
use std::thread::JoinHandle;
use wg_2024::controller::{DroneCommand, DroneEvent};
use wg_2024::network::NodeId;
use crate::network::message::{Message, Request};
use crate::network::{Node, NodeCommand, SimControllerMessage};
use std::thread;
use wg_2024::packet::Packet;
use wg_2024_rust::drone::RustDrone;
use wg_2024::drone::Drone;
use crate::utils::*;

/// Central simulation controller that manages all network nodes and their communication channels.
/// 
/// This controller maintains bidirectional communication with network nodes:
/// - **Commands to nodes**: `drones` and `servers` contain senders for sending commands from controller to nodes
/// - **Events from nodes**: `drone_event_recv` receives events/responses from drones back to controller
/// 
/// Thread handles (`d_handles`, `s_handles`) allow graceful shutdown of spawned node threads.
pub struct SimulationController {
    /// Commands from controller to drones - HashMap<NodeId, Sender<DroneCommand>>
    pub drones: HashMap<NodeId, Sender<DroneCommand>>,
    
    /// Commands from controller to servers - HashMap<NodeId, Sender<NodeCommand>>  
    pub servers: HashMap<NodeId, Sender<NodeCommand>>,
    
    /// Events from drones to controller - Sender<DroneEvent>
    pub drone_event_send: Sender<DroneEvent>,

    /// Events from the drones - Receiver<DroneEvent>
    pub drone_event_recv: Receiver<DroneEvent>,
    
    /// Thread handles for spawned drone processes
    pub d_handles: Vec<JoinHandle<()>>,
    
    /// Thread handles for spawned server processes  
    pub s_handles: Vec<JoinHandle<()>>,

    /// Packet channels for communication with nodes - HashMap<NodeId, (Sender<Packet>, Receiver<Packet>)>
    pub packet_channels: HashMap<NodeId, (Sender<Packet>, Receiver<Packet>)>,
}

impl SimulationController {
    fn get_id(&self) -> u8 {
        let mut id: u8 = 1;
        while self.drones.contains_key(&id) || self.servers.contains_key(&id) {
            id += 1;
        }
        id
    }
    pub fn crash_all(&mut self) -> anyhow::Result<()> {
        for (_, sender) in self.drones.iter() {
            sender.send(DroneCommand::Crash)?;
        }
        Ok(())
    }
    
    /// Get a reference to a drone's command sender
    pub fn get_drone(&self, id: &NodeId) -> Option<&Sender<DroneCommand>> {
        self.drones.get(id)
    }
    
    /// Get a reference to a server's command sender
    pub fn get_server(&self, id: &NodeId) -> Option<&Sender<NodeCommand>> {
        self.servers.get(id)
    }

    pub fn send_message(
        &self,
        id1: NodeId,
        id2: NodeId,
    ) -> anyhow::Result<()> {
        println!("🚨🚨🚨Sending message from {} to {}", id1, id2);
        self.servers
        .get(&id1)
        .unwrap()
        .send(NodeCommand::SendMessage(
            SimControllerMessage::SendMessageToPeer(
                id2,
                Message::Request(Request::ServerType))))?;
        Ok(())
    }

    pub fn add_edge(&mut self, from: NodeId, to: NodeId) -> anyhow::Result<()> {
        match self.servers.get(&from) {
            Some(sender) => {
                sender.send(NodeCommand::AddNeighbour((to, self.packet_channels[&to].0.clone())))?;
            },
            None => { println!("Server {} not found", from);}
        }
        match self.drones.get(&from) {
            Some(sender) => {
                sender.send(DroneCommand::AddSender(to, self.packet_channels[&to].0.clone()))?;
            },
            None => { println!("Drone {} not found", from); }
        }
        match self.servers.get(&to) {
            Some(sender) => {
                sender.send(NodeCommand::AddNeighbour((from, self.packet_channels[&from].0.clone())))?;
            },
            None => { println!("Server {} not found", to);}
        }
        match self.drones.get(&to) {
            Some(sender) => {
                sender.send(DroneCommand::AddSender(from, self.packet_channels[&from].0.clone()))?;
            },
            None => { println!("Drone {} not found", to);}
        }
        Ok(())
    }
    
    /// Add a new drone to the simulation
    pub fn add_drone(&mut self) -> anyhow::Result<()> {
        let id = self.get_id();
        let (controller_drone_send, controller_drone_recv) = unbounded();
        let node_event_send: Sender<DroneEvent> = self.drone_event_send.clone();
        // Create packet channel for the new server
        let (packet_send, packet_recv) = unbounded();
        self.packet_channels.insert(id, (packet_send, packet_recv.clone()));
        
        // Insert server controller before spawning thread
        self.drones.insert(id, controller_drone_send);
        
        self.d_handles.push(
            thread::Builder::new()
                .name(format!("drone{}", id))
                .spawn(move || {
                    let mut drone = RustDrone::new(
                        id,
                        node_event_send,
                        controller_drone_recv,
                        packet_recv,
                        HashMap::new(),
                        0.0,  // Example PDR value, can be adjusted
                    );

                    drone.run();
                }
            )?
        );
        
        Ok(())  // Return the ID of the newly created server
    }
    
    /// Add a new server to the simulation
    pub fn add_server(&mut self, server_type: ServerType) -> anyhow::Result<()> {
        let id = self.get_id();
        let (controller_server_send, controller_server_recv) = unbounded();
        
        // Create packet channel for the new server
        let (packet_send, packet_recv) = unbounded();
        self.packet_channels.insert(id, (packet_send, packet_recv.clone()));
        
        // Insert server controller before spawning thread
        self.servers.insert(id, controller_server_send);
        
        self.s_handles.push(
            thread::Builder::new()
                .name(format!("server{}", id))
                .spawn(move || {
                    let mut server = match server_type {
                        ServerType::Communication => {
                            Node::new_communication_server(
                                id,
                                controller_server_recv,
                                packet_recv,
                                String::from("/tmp/rust"),
                            )
                        },
                        _ => {
                            println!("Creating content server with ID: {}", id);
                            Node::new_content_server(
                                id,
                                controller_server_recv,
                                packet_recv,
                                String::from("/tmp/rust"),
                            )
                        },
                    };

                    server.run();
                })?
        );
        
        Ok(())  // Return the ID of the newly created server
    }
}
