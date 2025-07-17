# Core of the Advanced Programming project @ University of Trento 2024/2025

This repository contains the core of the team project developed for the _Advanced Programming_ course at the **University of Trento** during the 2024/2025 academic year.

## 🎯 Project Overview

In simple terms, the project is a simulation of how the internet works, but instead of routers, we have drones that can crash or lose packets, just like real drones!
Clients can communicate with servers to write and save files, communicate with other clients by creating chats and sending messages.

## 💡 Some curiosities about the project
This project was developed as part of a unique W3C-like Working Group experience. Students were divided into teams, each electing a Working Group Leader (WGL) to represent them in weekly collaborative meetings, where shared decisions and protocol changes were discussed.
One student was elected as the Working Group Coordinator (WGC), responsible for facilitating meetings and drafting the official protocol specification (DCP).

In addition, each team participating in the course programmed their own drone with optional custom features. Towards the end of the course, there was a "fair" where each team had to both buy 10 drones from others and sell their own to as many teams as possible, showcasing well-tested code or distributing custom food/gadgets (that always works!).
The top 3 selling teams received bonus points for the final exam.

## 🛠️ Technologies Used

### Core Dependencies
- **[wg_2024](https://github.com/WGL-2024/WGL_repo_2024.git)**: Base protocol and packet definitions
- **[Axum](https://github.com/tokio-rs/axum)**: Modern async web framework for HTTP API
- **[Tokio](https://tokio.rs/)**: Asynchronous runtime for concurrent operations
- **[Crossbeam](https://github.com/crossbeam-rs/crossbeam)**: High-performance concurrent data structures
- **[Serde](https://serde.rs/)**: Serialization framework for messages and configuration

### Networking & Communication
- **[Bincode](https://github.com/bincode-org/bincode)**: Efficient binary serialization for packet transmission
- **[Tower-HTTP](https://github.com/tower-rs/tower-http)**: HTTP middleware and utilities

### Development & Utilities
- **[TOML](https://github.com/toml-rs/toml)**: Configuration file parsing
- **[Anyhow](https://github.com/dtolnay/anyhow)**: Ergonomic error handling
- **[Log](https://github.com/rust-lang/log)** + **[Env Logger](https://github.com/rust-cli/env_logger)**: Structured logging system
- **[Chrono](https://github.com/chronotope/chrono)**: Date and time handling
- **[Rand](https://github.com/rust-random/rand)**: Random number generation for simulation

### Multi-Group Drone Integrations
- **10 external drone implementations** from university teams

## 🕸 Network Node Types

There are three main categories of network nodes, each with specific roles and capabilities:

### 🚁 Drones
**11 different drone implementations** serve as the backbone routing infrastructure:
- **"Rust" drone**: Our team's custom implementation
- **10 external drones**: Implementations from other teams (Lockheed Rustin, Rusty Drones, Rustbusters, Skylink, Bagel Bomber, Bobry w Locie, D-R-O-N-E, Rust Roveri, Rust Do It, LeDron James)

Here're the drones' responsibilities:
- Act as **network routers** that forward packets between nodes
- Handle **packet dropping** based on configurable Packet Drop Rate (PDR)
- Maintain **network connectivity** and enable communication paths
- Support **source routing** without maintaining routing tables
- Provide **fault tolerance** through redundant paths

### 👥 Clients
Clients are end-user applications that consume network services. There are two types of clients in the project:

#### Chat Clients
- **Purpose**: Enable real-time messaging between users
- **Communication**: Exclusively interact with **Communication Servers**
- **Capabilities**:
  - Create new chat rooms with unique identifiers
  - Join existing chat rooms
  - Send and receive messages within chat rooms
  - Leave or delete chat rooms
- **Network Effect**: Allow **indirect client-to-client communication** through server mediation

#### Web Clients
- **Purpose**: Simulate web browsing and file management
- **Communication**: Exclusively interact with **Content Servers**
- **Capabilities**:
  - Upload files to the distributed file system
  - Download and retrieve stored files
  - Browse available content and directories
  - Manage file metadata and permissions

### 🖥️ Servers
Servers provide specialized services to clients through the drone network. There are two types of servers in the project:

#### Communication Servers
- **Role**: Central hubs for chat and messaging functionality
- **Client Support**: Handle requests from **Chat Clients** exclusively
- **Core Services**:
  - **Chat room management**: Create, delete, and manage chat rooms
  - **User session handling**: Track active users and their chat memberships
  - **Message routing**: Forward messages between chat participants
  - **Chat history**: Maintain persistent chat logs and participant lists
  - **Real-time updates**: Notify clients of new messages and room changes

#### Content Servers
- **Role**: Distributed file storage and content distribution
- **Client Support**: Handle requests from **Web Clients** exclusively  
- **Core Services**:
  - **File storage**: Securely store uploaded files with metadata
  - **Content retrieval**: Serve files to authorized clients
  - **Directory services**: Provide file listings and navigation
  - **Access control**: Manage file permissions and user access
  - **Content validation**: Ensure file integrity and format compliance

## 🏷️ Configuration

### Network Topology Definition
The simulation uses TOML configuration files to define network topology and node parameters:

#### Drone Configuration
```toml
[[drone]]
id = 0
connected_node_ids = [10, 20, 200, 101]
pdr = 0.43  # Packet Drop Rate (43%)
```

**Drone Properties:**
- **NodeId**: Unique identifier (0-255)
- **Connected Nodes**: List of directly connected neighbors
- **PDR (Packet Drop Rate)**: Probability of dropping received packets (0.0-1.0)
- **Bidirectional connections**: All connections are automatically bidirectional

#### Client Configuration
```toml
[[client]]
id = 100
client_type = "ChatClient"      # or "WebBrowser"
connected_drone_ids = [1]       # Max 2 drone connections
```

**Client Properties:**
- **Connection Limit**: 1-2 drone connections only
- **Types**: ChatClient (messaging), WebBrowser (content access)
- **Edge Placement**: Must be at network edges (not routing nodes)

#### Server Configuration
```toml
[[server]]
id = 10
connected_drone_ids = [0, 1]    # Min 2 drone connections
server_type = "Content"         # or "Communication"
base_path = "/tmp/rust"
```

**Server Properties:**
- **Connection Requirement**: Minimum 2 drone connections for redundancy
- **Types**: Content (file storage), Communication (chat/messaging)
- **Base Path**: File system location for content servers
- **High Availability**: Multiple connections ensure fault tolerance

## 🔌 Communication Protocol

### Source Routing Protocol

All data packets use **source routing**, where the complete path through the network is predetermined:

#### Key Benefits
- **No routing tables** required in drone nodes
- **Predictable paths** for debugging and analysis
- **Fault tolerance** through alternative pre-computed routes
- **Load balancing** by distributing traffic across multiple paths

#### Implementation Details
```rust
// Packets contain the complete route from source to destination
struct Packet {
    source: NodeId,
    destination: NodeId,
    route: Vec<NodeId>,     // Complete path through drone network
    current_hop: usize,     // Current position in route
    payload: Vec<u8>,       // Serialized message content
}
```

When a drone receives a packet:
1. **Validate route**: Ensure the drone is the next hop
2. **Check PDR**: Potentially drop packet based on configured drop rate
3. **Forward packet**: Send to next drone in route or deliver to destination
4. **Update statistics**: Track sent/dropped packet counts

## 🌐 Network Discovery Protocol

The network discovery mechanism enables nodes to automatically learn and map the complete network topology using a flood-based approach.

### Discovery Process

#### 1. Flood Initialization
```rust
pub struct NetDiscovery {
    last_id: u64,                    // Unique flood identifier
    ongoing: Arc<AtomicBool>,        // Thread-safe discovery status
    responses: Vec<FloodResponse>,   // Collected topology responses
    start_time: Instant,             // Discovery start timestamp
}
```

#### 2. Flood Propagation
- **Initiator** generates a unique flood ID and broadcasts flood requests
- **Each drone** receives the flood packet and:
  - Records its own NodeId in the path trace
  - Forwards the packet to all connected neighbors
  - Prevents loops by tracking seen flood IDs

#### 3. Response Collection
- **Destination nodes** (clients/servers) send FloodResponse packets back
- **Path trace** contains the complete route the flood packet traveled
- **Responses** are collected by the original flood initiator

#### 4. Topology Reconstruction
- **Path Analysis**: Each flood response contains a complete path trace from source to destination
- **Bidirectional Mapping**: Extract both forward and backward connections from each hop in the path
- **Adjacency List**: Build a HashMap where each NodeId maps to its directly connected neighbors
- **Duplicate Removal**: Ensure each connection is recorded only once per neighbor list
- **Memory Cleanup**: Clear processed responses to free memory

### Discovery Features
- **Automatic topology mapping** without manual configuration
- **Dynamic updates** when network topology changes
- **Bidirectional graph construction** from unidirectional path traces
- **Concurrent discovery** using thread-safe atomic operations
- **Timeout handling** for failed or incomplete discoveries

## 🪵 Custom Logging System
A custom in-memory logging implementation designed to send logs to the frontend for real-time display:

```rust
pub struct InMemoryLogger {
    logs: Arc<Mutex<Vec<LogEntry>>>,
}

pub struct LogEntry {
    pub level: Level,
    pub message: String,
}
```

### Custom Logging Features
- **Custom implementation**: Tailored for frontend integration and real-time log streaming
- **Multi-level logging**: Trace, Debug, Info, Warn, Error
- **In-memory storage**: Fast access for real-time monitoring and frontend display
- **Thread-safe**: Concurrent logging from all simulation components
- **API-ready**: Logs are formatted and filtered for frontend consumption
- **Real-time updates**: Frontend can query and display logs as they are generated

## 🚀 HTTP API Server

The backend exposes a RESTful API for the [frontend](https://github.com/F4bbi/a-rust-project-about-drones), enabling users to interact with the simulation. Using **Axum** and **Tokio**, HTTP requests are efficiently handled and forwarded to the simulation controller, which manages the entire simulation process.

## 📄 Full Project Specifications

For complete technical specifications, protocol details, and implementation requirements, please refer to the official project documentation: [Project Specifications](https://github.com/LuigiMiazzo17/unitn-advancedProgramming-WGL_2024-rust/tree/master/assets/spec)