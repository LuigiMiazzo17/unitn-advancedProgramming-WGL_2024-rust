use crossbeam::channel::{Sender, unbounded};
use log::debug;
use std::collections::HashMap;
use std::fs;
use std::thread;
use wg_2024::packet::Packet;

use crate::network::Node;
use crate::network_initializer::config::Config;
use crate::simulation_controller::{
    SimulationController,
    network_object::{
        Client as SimCntClient, Drone as SimCntDrone, NetworkObject, Server as SimCntServer,
    },
};
use crate::utils::ClientType;
use crate::utils::ServerType;

use wg_2024_rust::drone::RustDrone;

use wg_2024::drone::Drone;

pub fn parse_config(file: &str) -> anyhow::Result<Config> {
    let file_str = fs::read_to_string(file)?;
    let conf = toml::from_str(&file_str)?;
    debug!("Loaded config: {:?}", conf);
    Ok(conf)
}

#[allow(clippy::type_complexity)]
pub fn spawn_network(config: Config) -> anyhow::Result<SimulationController> {
    let mut drones = HashMap::new();
    let (drone_event_send, drone_event_recv) = unbounded();

    let mut servers = HashMap::new();
    let mut clients = HashMap::new();
    let (client_event_send, client_event_recv) = unbounded();

    let mut packet_channels = HashMap::new();
    for drone in config.drone.iter() {
        packet_channels.insert(drone.id, unbounded());
    }
    for client in config.client.iter() {
        packet_channels.insert(client.id, unbounded());
    }
    for server in config.server.iter() {
        packet_channels.insert(server.id, unbounded());
    }

    let mut d_handles = Vec::new();
    for drone_cfg in config.drone.into_iter() {
        let (controller_drone_send, controller_drone_recv) = unbounded();

        let node_event_send = drone_event_send.clone();
        let packet_recv = packet_channels[&drone_cfg.id].1.clone();

        d_handles.push(
            thread::Builder::new()
                .name(format!("drone{}", drone_cfg.id))
                .spawn(move || {
                    let mut d = RustDrone::new(
                        drone_cfg.id,
                        node_event_send,
                        controller_drone_recv,
                        packet_recv,
                        HashMap::new(),
                        drone_cfg.pdr,
                    );

                    d.run();
                })?,
        );

        let mut drone = SimCntDrone::new(
            drone_cfg.id,
            controller_drone_send,
            drone_cfg.pdr,
            "".to_string(), //TODO: Figure out this
        );

        for connected_id in drone_cfg.connected_node_ids {
            drone.add_neighbour(connected_id, packet_channels[&connected_id].0.clone());
        }

        drones.insert(drone_cfg.id, drone);
    }

    let mut s_handles = Vec::new();
    for server_cfg in config.server.into_iter() {
        // controller
        let (controller_server_send, controller_server_recv) = unbounded();
        // packet
        let packet_recv = packet_channels[&server_cfg.id].1.clone();
        let server_type = match server_cfg.server_type.as_str() {
            "Content" => ServerType::Content,
            "Communication" => ServerType::Communication,
            _ => panic!("Unknown server type: {}", server_cfg.server_type),
        };
        let server_type_clone = server_type.clone();

        s_handles.push(
            thread::Builder::new()
                .name(format!("server{}", server_cfg.id))
                .spawn(move || {
                    let mut server = match server_type_clone {
                        ServerType::Content => Node::new_content_server(
                            server_cfg.id,
                            controller_server_recv,
                            packet_recv,
                            server_cfg.base_path,
                        ),
                        ServerType::Communication => Node::new_communication_server(
                            server_cfg.id,
                            controller_server_recv,
                            packet_recv,
                            server_cfg.base_path,
                        ),
                    };

                    server.run();
                })?,
        );

        let mut server = SimCntServer::new(server_cfg.id, controller_server_send, server_type);

        for connected_id in server_cfg.connected_drone_ids.iter() {
            server.add_neighbour(*connected_id, packet_channels[connected_id].0.clone());
        }

        servers.insert(server_cfg.id, server);
    }

    let mut c_handles = Vec::new();
    for client_cfg in config.client.into_iter() {
        // controller
        let (controller_client_send, controller_client_recv) = unbounded();
        // packet
        let packet_recv = packet_channels[&client_cfg.id].1.clone();
        let client_controller_send = client_event_send.clone();
        let client_type = match client_cfg.client_type.as_str() {
            "ChatClient" => ClientType::Chat,
            "WebBrowser" => ClientType::Web,
            _ => panic!("Unknown client type: {}", client_cfg.client_type),
        };
        let client_type_clone = client_type.clone();

        c_handles.push(
            thread::Builder::new()
                .name(format!("client{}", client_cfg.id))
                .spawn(move || {
                    let mut client = match client_type_clone {
                        ClientType::Chat => Node::new_chat_client(
                            client_cfg.id,
                            controller_client_recv,
                            packet_recv,
                            client_controller_send,
                        ),
                        ClientType::Web => Node::new_browser_client(
                            client_cfg.id,
                            controller_client_recv,
                            packet_recv,
                            client_controller_send,
                        ),
                    };

                    client.run();
                })?,
        );

        let mut client = SimCntClient::new(client_cfg.id, controller_client_send, client_type);

        for connected_id in client_cfg.connected_drone_ids.iter() {
            client.add_neighbour(*connected_id, packet_channels[connected_id].0.clone());
        }

        clients.insert(client_cfg.id, client);
    }

    let packet_channels = packet_channels
        .into_iter()
        .map(|(id, (send, _))| (id, (send)))
        .collect::<HashMap<u8, Sender<Packet>>>();

    Ok(SimulationController {
        drones,
        servers,
        clients,
        client_event_send,
        client_event_recv,
        drone_event_send,
        drone_event_recv,
        d_handles,
        s_handles,
        c_handles,
        packet_channels,
    })
}
