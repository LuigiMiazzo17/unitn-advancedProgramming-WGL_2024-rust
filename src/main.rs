use std::thread;
use std::time::Duration;

use axum::http::StatusCode;
use std::fs;
use unitn_advancedProgramming_WGL_2024_rust::network_initializer::{parse_config, spawn_network};
use unitn_advancedProgramming_WGL_2024_rust::server::ContentServer;
use unitn_advancedProgramming_WGL_2024_rust::simulation_controller::SimulationController;

use axum::{
    extract::{Json as ExtractJson, State, rejection::JsonRejection},
    response::{IntoResponse, Json},
    routing::{Router, get, post},
};
use bincode::de;
use log::debug;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use unitn_advancedProgramming_WGL_2024_rust::utils::*;

// Request structures for API endpoints
#[derive(serde::Deserialize)]
struct AddNodeRequest {
    node_name: String,
    node_type: String,
}

#[derive(serde::Deserialize)]
struct AddEdgeRequest {
    from_id: u8,
    to_id: u8,
}

// Shared state structure - wraps the simulation controller for thread-safe access
#[derive(Clone)]
struct AppState {
    simulation_controller: Arc<Mutex<SimulationController>>,
}

async fn get_topology() -> Json<Value> {
    // Try to parse the actual config file
    match parse_config("examples/config/base.toml") {
        Ok(config) => {
            let mut nodes = Vec::new();
            let mut edges = Vec::new();

            // Add drones
            for drone in &config.drone {
                nodes.push(json!({
                    "data": {
                        "id": format!("drone_{}", drone.id),
                        "label": format!("Drone {}", drone.id),
                        "type": "drone"
                    }
                }));

                // Add edges from drone to connected nodes
                for connected_id in &drone.connected_node_ids {
                    edges.push(json!({
                        "data": {
                            "id": format!("edge_{}_{}", drone.id, connected_id),
                            "source": format!("drone_{}", drone.id),
                            "target": format!("server_{}", connected_id)
                        }
                    }));
                }
            }

            // Add servers
            for server in &config.server {
                nodes.push(json!({
                    "data": {
                        "id": format!("server_{}", server.id),
                        "label": format!("Server {} ({})", server.id, server.server_type),
                        "type": "server"
                    }
                }));
            }

            // Add clients
            for client in &config.client {
                nodes.push(json!({
                    "data": {
                        "id": format!("client_{}", client.id),
                        "label": format!("Client {}", client.id),
                        "type": "client"
                    }
                }));

                // Add edges from client to connected drones
                for drone_id in &client.connected_drone_ids {
                    edges.push(json!({
                        "data": {
                            "id": format!("edge_{}_{}", client.id, drone_id),
                            "source": format!("client_{}", client.id),
                            "target": format!("drone_{}", drone_id)
                        }
                    }));
                }
            }

            Json(json!({
                "nodes": nodes,
                "edges": edges
            }))
        }
        Err(_) => {
            // Fallback sample data if config file not found
            Json(json!({
                "error": {"ERROR": "Config file not found"},
            }))
        }
    }
}

async fn get_nodes() -> Json<Value> {
    let mut nodes = Vec::new();

    // Get base64 images for different node types
    let drone_image = image_to_base64("assets/images/drone/drone.png")
        .unwrap_or_else(|| "data:image/png;base64,".to_string());
    let chat_client_image = image_to_base64("assets/images/client/chat-client.png")
        .unwrap_or_else(|| "data:image/png;base64,".to_string());
    let web_client_image = image_to_base64("assets/images/client/web-client.png")
        .unwrap_or_else(|| "data:image/png;base64,".to_string());
    let media_server_image = image_to_base64("assets/images/server/media-server.png")
        .unwrap_or_else(|| "data:image/png;base64,".to_string());
    let text_server_image = image_to_base64("assets/images/server/text-server.png")
        .unwrap_or_else(|| "data:image/png;base64,".to_string());

    // Read and parse Cargo.toml to extract drone dependencies
    match fs::read_to_string("Cargo.toml") {
        Ok(cargo_content) => {
            match cargo_content.parse::<toml::Value>() {
                Ok(cargo_toml) => {
                    if let Some(dependencies) =
                        cargo_toml.get("dependencies").and_then(|d| d.as_table())
                    {
                        // Extract drone dependencies
                        nodes.push(json!({
                            "name": "Rust",
                            "type": "drone",
                            "image": drone_image
                        }));
                        println!("Rust");
                        for (dep_name, _) in dependencies {
                            if dep_name.starts_with("wg_drone_") {
                                // Convert drone name to PascalCase display name
                                let display_name = format_drone_name(dep_name);
                                println!("{}", display_name);
                                nodes.push(json!({
                                    "name": display_name,
                                    "type": "drone",
                                    "image": drone_image
                                }));
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Error parsing Cargo.toml: {}", e);
                }
            }
        }
        Err(e) => {
            println!("Error reading Cargo.toml: {}", e);
        }
    }

    // Add predefined clients
    nodes.push(json!({
        "name": "Chat Client",
        "type": "client",
        "image": chat_client_image
    }));

    nodes.push(json!({
        "name": "Web Client",
        "type": "client",
        "image": web_client_image
    }));

    // Add predefined servers
    nodes.push(json!({
        "name": "Media Server",
        "type": "server",
        "image": media_server_image
    }));

    nodes.push(json!({
        "name": "Text Server",
        "type": "server",
        "image": text_server_image
    }));

    Json(json!({ "nodes": nodes }))
}

async fn send_message(State(state): State<AppState>, Json(ids): Json<Edge>) -> impl IntoResponse {
    debug!("Sending message from {} to {}", ids.0, ids.1);
    // Access the simulation controller
    let controller = state.simulation_controller.lock().unwrap();
    match controller.send_message(ids.0, ids.1) {
        Ok(()) => {
            return (
                StatusCode::OK,
                format!("Message sent from {} to {}", ids.0, ids.1),
            );
        }
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Source node {} does not exist", e),
            );
        }
    }
}

async fn add_edge(State(state): State<AppState>, Json(ids): Json<Edge>) -> impl IntoResponse {
    debug!("Adding edge from {} to {}", ids.0, ids.1);
    // Access the simulation controller
    let mut controller = state.simulation_controller.lock().unwrap();

    // Check if both nodes exist
    let from_exists =
        controller.drones.contains_key(&ids.0) || controller.servers.contains_key(&ids.1);
    let to_exists =
        controller.drones.contains_key(&ids.0) || controller.servers.contains_key(&ids.1);

    if !from_exists {
        return (
            StatusCode::BAD_REQUEST,
            format!("Source node {} does not exist", &ids.0),
        );
    }

    if !to_exists {
        return (
            StatusCode::BAD_REQUEST,
            format!("Source node {} does not exist", &ids.1),
        );
    }

    // Actually add the edge
    match controller.add_edge(ids.0, ids.1) {
        Ok(()) => (
            StatusCode::CREATED,
            format!("Edge {} -> {} added successfully", ids.0, ids.1),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to add edge: {}", e),
        ),
    }
}

async fn add_node(
    State(state): State<AppState>,
    payload: Result<Json<NodeType>, JsonRejection>,
) -> impl IntoResponse {
    debug!("Adding node with payload: {:?}", payload);
    let node_type = match payload {
        Ok(Json(node_type)) => node_type,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", err));
        }
    };

    let mut controller = state.simulation_controller.lock().unwrap();

    match node_type {
        NodeType::Drone(drone_type) => match controller.add_drone() {
            Ok(()) => (
                StatusCode::CREATED,
                format!("Drone of type '{:?}' created", drone_type),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create drone: {}", e),
            ),
        },

        NodeType::Server(server_type) => match controller.add_server(server_type.clone()) {
            Ok(()) => (
                StatusCode::CREATED,
                format!("Server of type '{:?}' created", server_type),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create server: {}", e),
            ),
        },

        NodeType::Client(client_type) => (
            StatusCode::OK,
            format!(
                "Client of type '{:?}' registered (spawning not implemented)",
                client_type
            ),
        ),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config = parse_config("examples/config/base.toml")?;

    let (
        controller_drones,
        controller_server,
        controller_client,
        client_controller_recv,
        node_event_send,
        node_event_recv,
        mut d_handles,
        mut s_handles,
        mut c_handles,
        packet_channels,
    ) = spawn_network(config)?;

    // Create the simulation controller with all network state
    let simulation_controller = SimulationController {
        drones: controller_drones,
        servers: controller_server,
        drone_event_send: node_event_send,
        drone_event_recv: node_event_recv,
        d_handles,
        s_handles,
        packet_channels,
    };

    let app_state: AppState = AppState {
        simulation_controller: Arc::new(Mutex::new(simulation_controller)),
    };

    let app = Router::new()
        .route("/api/topology", get(get_topology))
        .route("/api/nodes", get(get_nodes).post(add_node))
        .route("/api/edges", post(add_edge))
        .route("/api/messages", post(send_message))
        .with_state(app_state)
        .fallback_service(ServeDir::new("./dist").append_index_html_on_directories(true));

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
