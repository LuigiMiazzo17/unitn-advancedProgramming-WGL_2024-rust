use log::{debug, error, info, trace, warn};
use std::fs;
use std::path::PathBuf;

use wg_2024::network::NodeId;

use crate::network::message::{
    ContentRequest, ContentResponse, Message, Request, Response, ServerType,
};
use crate::network::{NodeTrait, SimControllerMessage};

#[derive(Debug)]
pub struct ContentServer {
    base_path: PathBuf,
}

impl NodeTrait for ContentServer {
    fn handle_message(&mut self, peer_id: NodeId, message: Message) -> Option<Response> {
        trace!("Handling message from peer {}: {:?}", peer_id, message);
        match message {
            Message::Request(request) => match request {
                Request::ServerType => Some(self.handle_server_type_request()),
                Request::ContentRequest(content_request) => {
                    debug!("Received ContentRequest from peer {}", peer_id);
                    Some(match content_request {
                        ContentRequest::ListPublicFiles => self.list_public_files(),
                        ContentRequest::ListPrivateFiles => self.list_private_files(peer_id),
                        ContentRequest::GetPublicFile(file_name) => {
                            self.get_public_file(&file_name)
                        }
                        ContentRequest::GetPrivateFile(file_name) => {
                            self.get_private_file(&file_name, peer_id)
                        }
                        ContentRequest::WritePublicFile(file_name, data) => {
                            self.write_public_file(&file_name, &data)
                        }
                        ContentRequest::WritePrivateFile(file_name, data) => {
                            self.write_private_file(&file_name, &data, peer_id)
                        }
                    })
                }
                _ => Some(Response::NotImplemented),
            },
            Message::Response(_) => {
                warn!("Server received response message from peer {}", peer_id);
                None
            }
        }
    }

    fn stop(&mut self) {}

    fn get_node_type(&self) -> wg_2024::packet::NodeType {
        wg_2024::packet::NodeType::Server
    }

    fn get_node_type_str(&self) -> &str {
        "ContentServer"
    }

    fn handle_control_message(
        &mut self,
        message: SimControllerMessage,
    ) -> Option<(NodeId, Option<u64>, Message)> {
        match message {
            SimControllerMessage::SendMessageToPeer(peer_id, message) => {
                Some((peer_id, None, message))
            }
        }
    }
}

impl ContentServer {
    pub fn new(node_id: NodeId, base_path: String) -> Self {
        let path = PathBuf::new()
            .join(base_path)
            .join(node_id.to_string())
            .join("content");

        if !path.exists() {
            if let Err(e) = fs::create_dir_all(&path) {
                error!(
                    "Failed to create content server directory {}: {}",
                    path.display(),
                    e
                );
            }
        }

        ContentServer { base_path: path }
    }

    fn handle_server_type_request(&self) -> Response {
        Response::ServerType(ServerType::Content)
    }

    fn get_public_dir(&self) -> PathBuf {
        self.base_path.join("public")
    }

    fn get_private_dir(&self, peer_id: NodeId) -> PathBuf {
        self.base_path.join(peer_id.to_string())
    }

    fn list_public_files(&self) -> Response {
        Self::list_files(&self.get_public_dir())
    }

    fn list_private_files(&self, peer_id: NodeId) -> Response {
        Self::list_files(&self.get_private_dir(peer_id))
    }

    fn list_files(path: &PathBuf) -> Response {
        match fs::read_dir(path) {
            Ok(files) => Response::ContentResponse(ContentResponse::Files(
                files
                    .into_iter()
                    .filter_map(|entry| {
                        entry
                            .ok()
                            .and_then(|e| e.file_name().into_string().ok().map(|s| s.to_string()))
                    })
                    .collect(),
            )),
            Err(_) => {
                if let Err(e) = fs::create_dir_all(path) {
                    error!("Failed to create directory {}: {}", path.display(), e);
                    Response::Error("Failed to list files".to_string())
                } else {
                    Response::ContentResponse(ContentResponse::Files(vec![]))
                }
            }
        }
    }

    fn get_public_file(&self, file_name: &str) -> Response {
        if file_name.contains("..") {
            return Response::Error("Invalid file name".to_string());
        }
        Self::get_file(&self.get_public_dir().join(file_name))
    }

    fn get_private_file(&self, file_name: &str, peer_id: NodeId) -> Response {
        if file_name.contains("..") {
            return Response::Error("Invalid file name".to_string());
        }
        Self::get_file(&self.get_private_dir(peer_id).join(file_name))
    }

    fn get_file(path: &PathBuf) -> Response {
        match fs::read_to_string(path) {
            Ok(data) => Response::ContentResponse(ContentResponse::File(data)),
            Err(_) => Response::Error("Failed to read file".to_string()),
        }
    }

    fn write_public_file(&self, file_name: &str, data: &str) -> Response {
        if file_name.contains("..") {
            warn!("Invalid file name: {}", file_name);
            Response::Error("Invalid file name".to_string())
        } else {
            debug!("Writing public file: {}", file_name);
            Self::write_file(&self.get_public_dir().join(file_name), data);
            Response::Success
        }
    }

    fn write_private_file(&self, file_name: &str, data: &str, peer_id: NodeId) -> Response {
        if file_name.contains("..") {
            warn!("Invalid file name: {}", file_name);
            Response::Error("Invalid file name".to_string())
        } else {
            debug!("Writing private file: {} for peer {}", file_name, peer_id);
            Self::write_file(&self.get_private_dir(peer_id).join(file_name), data);
            Response::Success
        }
    }

    fn write_file(path: &PathBuf, data: &str) {
        if let Err(e) = fs::write(path, data) {
            error!("Failed to write file {}: {}", path.display(), e);
        } else {
            info!("File written successfully: {}", path.display());
        }
    }
}
