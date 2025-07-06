use axum::http::StatusCode;
use log::{debug, error, info, trace};
use std::collections::HashSet;
use std::fs;
use unitn_advancedProgramming_WGL_2024_rust::simulation_controller::{
    SimulationController, network_object::NetworkObject,
};

use axum::{
    extract::{Path, State, rejection::JsonRejection},
    response::{IntoResponse, Json},
    routing::{Router, get, patch, post},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use unitn_advancedProgramming_WGL_2024_rust::utils::*;

// Shared state structure - wraps the simulation controller for thread-safe access
#[derive(Clone)]
struct AppState {
    simulation_controller: Arc<Mutex<SimulationController>>,
}

async fn get_topology(State(state): State<AppState>) -> Json<Value> {
    // Try to parse the actual config file
    let controller = state.simulation_controller.lock().unwrap();
    trace!("Fetching topology from simulation controller");

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut edges_json = Vec::new();

    for (id, drone) in &controller.drones {
        nodes.push(json!({
            "data": {
                "id": format!("{}", id),
                "label": format!("Drone {}", id),
                "type": "drone"
            }
        }));

        // Add edges from drone to connected nodes
        for connected_id in drone.get_neighbours() {
            edges.push((id, connected_id));
        }
    }

    for (id, server) in &controller.servers {
        nodes.push(json!({
            "data": {
                "id": format!("{}", id),
                "label": format!("Server {}", id),
                "type": "server"
            }
        }));

        // Add edges from server to connected drones
        for connected_id in server.get_neighbours() {
            edges.push((id, connected_id));
        }
    }

    for (id, client) in &controller.clients {
        nodes.push(json!({
            "data": {
                "id": format!("{}", id),
                "label": format!("Client {}", id),
                "type": "client"
            }
        }));

        // Add edges from client to connected drones
        for connected_id in client.get_neighbours() {
            edges.push((id, connected_id));
        }
    }

    // Cleanup edges removing duplicates, considering that edges are bidirectional
    for &(src, dest) in &edges {
        if !edges.contains(&(dest, src)) {
            panic!("Edge from {} to {} exists but not the reverse", src, dest);
        }
    }

    let mut unique = HashSet::new();
    for &(a, b) in &edges {
        let normalized = if a < b { (a, b) } else { (b, a) };
        unique.insert(normalized);
    }

    let edges: Vec<_> = unique.into_iter().collect();

    for &(src, dest) in &edges {
        edges_json.push(json!({
            "data": {
                "id": format!("{}-{}", src, dest),
                "source": format!("{}", src),
                "target": format!("{}", dest)
            }
        }));
    }

    Json(json!({
        "nodes": nodes,
        "edges": edges_json
    }))
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
                        for (dep_name, _) in dependencies {
                            if dep_name.starts_with("wg_drone_") {
                                // Convert drone name to PascalCase display name
                                let display_name = format_drone_name(dep_name);
                                debug!("Found drone dependency: {}", display_name);
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
                    error!("Error parsing Cargo.toml: {}", e);
                }
            }
        }
        Err(e) => {
            error!("Error reading Cargo.toml: {}", e);
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

async fn get_node(State(state): State<AppState>, Path(id): Path<u8>) -> impl IntoResponse {
    info!("Fetching node with ID: {}", id);
    // Access the simulation controller
    let controller = state.simulation_controller.lock().unwrap();

    match controller.get_node_data(id) {
        Ok(node_data) => {
            let neighbours: Vec<Value> = node_data
                .neighbours
                .into_iter()
                .map(|(id, label, n_type)| json!({ "id": id, "label": label, "type": n_type }))
                .collect();
            let mut node_json = json!({
                "label": node_data.label,
                "type": node_data.node_type,
                "neighbours": neighbours,
            });
            match node_data.pdr {
                Some(pdr_value) => {
                    node_json
                        .as_object_mut()
                        .unwrap()
                        .insert("packet_drop_rate".to_string(), pdr_value.into());
                }
                None => {
                    debug!("No PDR data available for node {}", id);
                }
            }
            (StatusCode::OK, Json(node_json))
        }
        Err(e) => {
            error!("Error fetching node with ID {}: {}", id, e);
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Node with ID {} not found", id) })),
            )
        }
    }
}

async fn set_pdr(
    State(state): State<AppState>,
    Path(id): Path<u8>,
    Json(pdr): Json<Pdr>,
) -> impl IntoResponse {
    info!("Setting PDR for node with ID: {}", id);
    // Access the simulation controller
    let mut controller = state.simulation_controller.lock().unwrap();

    // Set the PDR for the node
    match controller.set_pdr(id, pdr.pdr) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "message": format!("PDR set for node {}", id)
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": format!("Failed to set PDR for node {}: {}", id, e)
            })),
        ),
    }
}

async fn send_message(State(state): State<AppState>, Json(edge): Json<Edge>) -> impl IntoResponse {
    debug!("Sending message from {} to {}", edge.from_id, edge.to_id);
    // Access the simulation controller
    let controller = state.simulation_controller.lock().unwrap();
    match controller.send_message(edge.from_id, edge.to_id) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "message": format!("Message sent from {} to {}", edge.from_id, edge.to_id)
            })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Failed to send message: {}", e)
            })),
        ),
    }
}

async fn delete_node(State(state): State<AppState>, Path(id): Path<u8>) -> impl IntoResponse {
    info!("Crashing drone with ID: {}", id);
    // Access the simulation controller
    let mut controller = state.simulation_controller.lock().unwrap();

    // Attempt to crash the drone
    match controller.delete_node(id) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "message": format!("Drone {} crashed successfully", id)
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": format!("Failed to crash drone {}: {}", id, e)
            })),
        ),
    }
}

async fn add_edge(State(state): State<AppState>, Json(edge): Json<Edge>) -> impl IntoResponse {
    info!("Adding edge from {} to {}", edge.from_id, edge.to_id);
    // Access the simulation controller
    let mut controller = state.simulation_controller.lock().unwrap();

    // Add the edge
    match controller.add_edge(edge.from_id, edge.to_id) {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({
                "message": format!("Edge added from {} to {}", edge.from_id, edge.to_id)
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": format!("Failed to add edge: {}", e)
            })),
        ),
    }
}

async fn delete_edge(State(state): State<AppState>, Json(edge): Json<Edge>) -> impl IntoResponse {
    info!("Deleting edge from {} to {}", edge.from_id, edge.to_id);
    // Access the simulation controller
    let mut controller = state.simulation_controller.lock().unwrap();

    // Delete the edge
    match controller.delete_edge(edge.from_id, edge.to_id) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "message": format!("Edge deleted from {} to {}", edge.from_id, edge.to_id)
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": format!("Failed to delete edge: {}", e)
            })),
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
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid JSON: {}", err)
                })),
            );
        }
    };

    let mut controller = state.simulation_controller.lock().unwrap();

    match node_type {
        NodeType::Drone(drone_type) => {
            match controller.add_drone(None, drone_type.clone(), Some(1.0)) {
                Ok(id) => (
                    StatusCode::CREATED,
                    Json(json!({
                        "message": format!("Drone of type '{:?}' created", drone_type),
                        "id": id,
                    })),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Failed to create drone: {}", e)
                    })),
                ),
            }
        }

        NodeType::Server(server_type) => match controller.add_server(None, server_type.clone()) {
            Ok(id) => (
                StatusCode::CREATED,
                Json(json!({
                    "message": format!("Server of type '{:?}' created", server_type),
                    "id": id,
                })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to create server: {}", e)
                })),
            ),
        },

        NodeType::Client(client_type) => match controller.add_client(None, client_type.clone()) {
            Ok(id) => (
                StatusCode::CREATED,
                Json(json!({
                    "message": format!("Client of type '{:?}' created", client_type),
                    "id": id,
                })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to create client: {}", e)
                })),
            ),
        },
    }
}

async fn get_configurations(State(state): State<AppState>) -> impl IntoResponse {
    let controller = state.simulation_controller.lock().unwrap();
    let configurations = controller.get_deserializable_configurations();

    (StatusCode::OK, Json(json!(configurations)))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let simulation_controller = match SimulationController::new(1) {
        Ok(controller) => {
            info!("Simulation controller initialized successfully");
            controller
        }
        Err(e) => {
            error!("Failed to initialize simulation controller: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to initialize simulation controller: {}",
                e
            ));
        }
    };

    // Create the simulation controller with all network state

    let app_state: AppState = AppState {
        simulation_controller: Arc::new(Mutex::new(simulation_controller)),
    };

    let app = Router::new()
        .route("/api/topology", get(get_topology))
        .route("/api/nodes", get(get_nodes).post(add_node))
        .route("/api/node/{id}", get(get_node).delete(delete_node))
        .route("/api/drone/{id}/pdr", patch(set_pdr))
        .route("/api/edges", post(add_edge).delete(delete_edge))
        .route("/api/messages", post(send_message))
        .route("/api/configurations", get(get_configurations))
        .with_state(app_state)
        .fallback_service(ServeDir::new("./dist").append_index_html_on_directories(true));

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    info!("Server running on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
