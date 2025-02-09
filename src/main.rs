use std::thread;
use std::time::Duration;

use wg_2024_rust_group::network::message::{Message, Request};
use wg_2024_rust_group::network::NodeCommand;
use wg_2024_rust_group::network_initializer::{parse_config, spawn_network};
use wg_2024_rust_group::simulation_controller::SimulationController;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config = parse_config("examples/config/base.toml")?;

    let (controller_drones, controller_server, node_event_recv, mut d_handles, mut s_handles) =
        spawn_network(config)?;

    let mut controller = SimulationController {
        drones: controller_drones,
        node_event_recv,
    };

    thread::sleep(Duration::from_secs(1));

    // controller_server
    //     .get(&10)
    //     .unwrap()
    //     .send(NodeCommand::SendMessage((
    //         20,
    //         Message::Request(Request::WritePublicFile("file2".to_string(), "ciaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaduhapjfbewphbgerpauhorbfhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhho".to_string())),
    //     )))?;

    controller_server
        .get(&10)
        .unwrap()
        .send(NodeCommand::SendMessage((
            20,
            Message::Request(Request::GetPrivateFile("cia2o".to_string())),
        )))?;

    thread::sleep(Duration::from_secs(1));

    controller.crash_all()?;

    while let Some(handle) = d_handles.pop() {
        let _ = handle.join();
    }

    for c in controller_server.iter() {
        c.1.send(NodeCommand::Quit)?;
    }

    while let Some(handle) = s_handles.pop() {
        let _ = handle.join();
    }

    Ok(())
}
