use axum::http::{HeaderValue, StatusCode};
use axum::{
    extract::{Path, State, rejection::JsonRejection},
    response::{IntoResponse, Json},
    routing::{Router, get, patch, post},
};

use log::{Level, debug, error, info, trace, warn};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs;
use std::sync::{Arc, Mutex};
use tower_http::cors::{Any, CorsLayer};
use tokio::net::TcpListener;
use unitn_advancedProgramming_WGL_2024_rust::network::message::{
    ChatId, ChatRequest, ContentRequest, CreateChatRequest, Request,
};
use unitn_advancedProgramming_WGL_2024_rust::simulation_controller::{
    SimulationController, network_object::NetworkObject,
};
use unitn_advancedProgramming_WGL_2024_rust::utils::*;
use unitn_advancedProgramming_WGL_2024_rust::utils::{
    ClientType, ServerType,
    logger::{self, InMemoryLogger},
};

// Shared state structure - wraps the simulation controller for thread-safe access
#[derive(Clone)]
struct AppState {
    simulation_controller: Arc<Mutex<SimulationController>>,
    logger: &'static InMemoryLogger,
}

async fn get_logs(
    State(state): State<AppState>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<Value> {
    let logs = state.logger.get_logs();
    let level_filter = query.get("level").map(|l| l.to_lowercase());

    let filtered_logs: Vec<_> = logs
        .into_iter()
        .filter(|log| {
            if let Some(ref level_str) = level_filter {
                let filter_level = match level_str.as_str() {
                    "trace" => Level::Trace,
                    "debug" => Level::Debug,
                    "info" => Level::Info,
                    "warn" => Level::Warn,
                    "error" => Level::Error,
                    _ => return false, // Should not happen with valid frontend input
                };
                log.level <= filter_level
            } else {
                true
            }
        })
        .map(|log| json!({"level": log.level.to_string(), "message": log.message}))
        .collect();

    Json(json!(filtered_logs))
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
                "type": "server",
                "subtype": match server.get_server_type() {
                    ServerType::Content => "content",
                    ServerType::Communication => "communication",
                }
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
                "type": "client",
                "subtype": match client.get_client_type() {
                    ClientType::Web => "web",
                    ClientType::Chat => "chat",
                }

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
                        }));
                        for (dep_name, _) in dependencies {
                            if dep_name.starts_with("wg_drone_") {
                                // Convert drone name to PascalCase display name
                                let display_name = format_drone_name(dep_name);
                                debug!("Found drone dependency: {}", display_name);
                                nodes.push(json!({
                                    "name": display_name,
                                    "type": "drone",
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
    }));

    nodes.push(json!({
        "name": "Web Client",
        "type": "client",
    }));

    // Add predefined servers
    nodes.push(json!({
        "name": "Content Server",
        "type": "server",
    }));

    nodes.push(json!({
        "name": "Communication Server",
        "type": "server",
    }));

    Json(json!({ "nodes": nodes }))
}

async fn get_node(State(state): State<AppState>, Path(id): Path<u8>) -> impl IntoResponse {
    info!("Fetching node with ID: {}", id);
    // Access the simulation controller
    let mut controller = state.simulation_controller.lock().unwrap();

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
                "subtype": node_data.subtype,
                "neighbours": neighbours,
            });
            match node_data.pdr {
                Some(pdr_value) => {
                    let node = node_json.as_object_mut().unwrap();
                    node.insert("packet_drop_rate".to_string(), pdr_value.into());
                    node.insert("pkg_sent".to_string(), node_data.stats.unwrap().0.into());
                    node.insert("pkg_drop".to_string(), node_data.stats.unwrap().1.into());
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

fn gimme_request(msg_type: &str, payload: &Value) -> Result<Request, String> {
    fn get_chat_id(payload: &Value) -> Result<ChatId, String> {
        payload
            .get("id")
            .and_then(|v| v.as_u64())
            .map(|id| id as ChatId)
            .ok_or_else(|| "Missing or invalid chat_id".to_string())
    }
    fn get_string(payload: &Value, key: &str) -> Result<String, String> {
        payload
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| format!("Missing or invalid {}", key))
    }
    fn get_option_string(payload: &Value, key: &str) -> Option<String> {
        payload
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| Some(s.to_string()))
            .unwrap_or(None)
    }

    match msg_type {
        "server-type" => Ok(Request::ServerType),
        "join" => {
            let chat_id = get_chat_id(payload)?;
            let password = get_option_string(payload, "password");
            Ok(Request::ChatRequest(ChatRequest::Join(chat_id, password)))
        }
        "leave" => {
            let chat_id = get_chat_id(payload)?;
            Ok(Request::ChatRequest(ChatRequest::Leave(chat_id)))
        }
        "send-message" => {
            let chat_id = get_chat_id(payload)?;
            let message = get_string(payload, "message")?;
            Ok(Request::ChatRequest(ChatRequest::SendMessage(
                chat_id, message,
            )))
        }
        "create" => {
            let name = get_string(payload, "name")?;
            let public: bool = !(payload
                .get("private")
                .and_then(|v| v.as_bool())
                .unwrap_or(false));
            let password = get_option_string(payload, "password");
            Ok(Request::ChatRequest(ChatRequest::Create(
                CreateChatRequest {
                    name,
                    public,
                    password,
                },
            )))
        }
        "delete" => {
            let chat_id = get_chat_id(payload)?;
            Ok(Request::ChatRequest(ChatRequest::Delete(chat_id)))
        }
        "get-chats" => Ok(Request::ChatRequest(ChatRequest::GetChats)),
        "get-messages" => {
            let chat_id = get_chat_id(payload)?;
            Ok(Request::ChatRequest(ChatRequest::GetMessages(chat_id)))
        }
        "list-public-files" => Ok(Request::ContentRequest(ContentRequest::ListPublicFiles)),
        "get-public-file" => {
            let file_name = get_string(payload, "file_name")?;
            Ok(Request::ContentRequest(ContentRequest::GetPublicFile(
                file_name,
            )))
        }
        "write-public-file" => {
            let file_name = get_string(payload, "file_name")?;
            let content = get_string(payload, "content")?;
            Ok(Request::ContentRequest(ContentRequest::WritePublicFile(
                file_name, content,
            )))
        }
        "list-private-files" => Ok(Request::ContentRequest(ContentRequest::ListPrivateFiles)),
        "get-private-file" => {
            let file_name = get_string(payload, "file_name")?;
            Ok(Request::ContentRequest(ContentRequest::GetPrivateFile(
                file_name,
            )))
        }
        "write-private-file" => {
            let file_name = get_string(payload, "file_name")?;
            let content = get_string(payload, "content")?;
            Ok(Request::ContentRequest(ContentRequest::WritePrivateFile(
                file_name, content,
            )))
        }
        _ => Err(format!("Unsupported message type: {}", msg_type)),
    }
}

async fn send_message(
    State(state): State<AppState>,
    Path(req): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    info!("Sending message of type: {}", req);
    // Access the simulation controller
    let controller = state.simulation_controller.lock().unwrap();

    let from_id: u64 = match payload.get("from_id").and_then(|v| v.as_u64()) {
        Some(id) => id,
        None => {
            warn!("Missing or invalid from_id in payload: {:?}", payload);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Missing or invalid from_id"
                })),
            );
        }
    };
    let to_id: u64 = match payload.get("to_id").and_then(|v| v.as_u64()) {
        Some(id) => id,
        None => {
            warn!("Missing or invalid to_id in payload: {:?}", payload);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Missing or invalid to_id"
                })),
            );
        }
    };

    if !(0..255).contains(&from_id) {
        warn!("Invalid from_id: {}", from_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Invalid from_id: {}", from_id)
            })),
        );
    }
    let from_id = from_id as u8;
    if !(0..255).contains(&to_id) {
        warn!("Invalid to_id: {}", to_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Invalid to_id: {}", to_id)
            })),
        );
    }
    let to_id = to_id as u8;

    let request = match gimme_request(&req, &payload) {
        Ok(req) => req,
        Err(e) => {
            warn!("Failed to parse request: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Invalid request: {}", e)
                })),
            );
        }
    };

    match controller.send_request(from_id, to_id, request) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "message": format!("Message sent from {} to {}", from_id, to_id)
            })),
        ),
        Err(e) => {
            error!("Failed to send message: {}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Failed to send message: {}", e)
                })),
            )
        }
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
            match controller.add_drone(None, drone_type.clone(), Some(0.0)) {
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

async fn change_configuration(
    State(state): State<AppState>,
    Json(config): Json<ConfigurationChange>,
) -> impl IntoResponse {
    debug!("Changing configuration with payload: {:?}", config);
    let mut controller = state.simulation_controller.lock().unwrap();

    match controller.spawn_network_from_config(config.id) {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({ "message": "Configuration changed successfully" })),
        ),
        Err(e) => {
            error!("Failed to change configuration: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to change configuration: {}", e) })),
            )
        }
    }
}

async fn get_messages(State(state): State<AppState>, Path(id): Path<u8>) -> impl IntoResponse {
    info!("Fetching messages from simulation controller");
    let mut controller = state.simulation_controller.lock().unwrap();

    match controller.get_messages(id) {
        Ok((v, messages)) => {
            let mut messages_json = Vec::new();
            for msg in messages {
                let message_json = json!({
                    "from": msg.server_id,
                    "message": msg.message.to_string(),
                    "timestamp": msg.timestamp,
                });
                messages_json.push(message_json);
            }
            debug!("Fetched {} messages for node {}", messages_json.len(), id);
            if messages_json.is_empty() {
                debug!("No messages found for node {}", id);
            }
            let json = json!({
                "messages": messages_json,
                "version": v,
            });
            (StatusCode::OK, Json(json!(json)))
        }
        Err(e) => {
            error!("Failed to fetch messages: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to fetch messages: {}", e) })),
            )
        }
    }
}

use std::sync::LazyLock;

static LOGGER: LazyLock<InMemoryLogger> = LazyLock::new(InMemoryLogger::new);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logger::init(&LOGGER).expect("Failed to initialize logger");

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
        logger: &LOGGER,
    };
    
    let bind_address = "0.0.0.0:3000".to_string();

    let cors = CorsLayer::new()
        .allow_origin(Any);

    let app = Router::new()
        .route("/api/topology", get(get_topology))
        .route("/api/nodes", get(get_nodes).post(add_node))
        .route("/api/node/{id}", get(get_node).delete(delete_node))
        .route("/api/node/{id}/messages", get(get_messages))
        .route("/api/drone/{id}/pdr", patch(set_pdr))
        .route("/api/edges", post(add_edge).delete(delete_edge))
        .route("/api/messages/{req}", post(send_message))
        .route(
            "/api/configurations",
            get(get_configurations).post(change_configuration),
        )
        .route("/api/logs", get(get_logs))
        .layer(cors)
        .with_state(app_state);

    let listener = TcpListener::bind(&bind_address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
