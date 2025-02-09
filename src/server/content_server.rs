use std::fs;
use std::path::PathBuf;

use wg_2024::network::NodeId;

use crate::network::message::{Message, Request, Response, ServerType};
use crate::network::NodeTrait;

#[derive(Debug)]
pub struct ContentServer {
    base_path: PathBuf,
}

impl NodeTrait for ContentServer {
    fn handle_message(&mut self, _peer_id: NodeId, message: Message) -> Option<Message> {
        match message {
            Message::Request(request) => match request {
                Request::ServerType => Some(Message::Response(self.handle_server_type_request())),
                Request::ListPublicFiles => Some(Message::Response(self.list_public_files())),
                Request::ListPrivateFiles => {
                    Some(Message::Response(self.list_private_files(_peer_id)))
                }
                Request::GetPublicFile(file_name) => {
                    Some(Message::Response(self.get_public_file(&file_name)))
                }
                Request::GetPrivateFile(file_name) => Some(Message::Response(
                    self.get_private_file(&file_name, _peer_id),
                )),
                Request::WritePublicFile(file_name, data) => {
                    self.write_public_file(&file_name, &data);
                    None
                }
                Request::WritePrivateFile(file_name, data) => {
                    self.write_private_file(&file_name, &data, _peer_id);
                    None
                }
                _ => Some(Message::Response(Response::NotImplemented)),
            },
            Message::Response(response) => {
                // TODO: This is useless
                println!("Received response: {:?}", response);
                None
            }
        }
    }

    fn stop(&mut self) {}
}

impl ContentServer {
    pub fn new(node_id: NodeId, base_path: String) -> Self {
        let path = PathBuf::new()
            .join(base_path)
            .join(node_id.to_string())
            .join("content");

        if !path.exists() {
            fs::create_dir_all(&path).unwrap();
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
            Ok(files) => Response::Files(
                files
                    .into_iter()
                    .filter_map(|entry| {
                        entry
                            .ok()
                            .and_then(|e| e.file_name().into_string().ok().map(|s| s.to_string()))
                    })
                    .collect(),
            ),
            Err(_) => {
                fs::create_dir_all(path).expect("Failed to create directory");
                Response::Files(Vec::new())
            }
        }
    }

    fn get_public_file(&self, file_name: &str) -> Response {
        if file_name.contains("..") {
            return Response::NoSuchFile;
        }
        Self::get_file(&self.get_public_dir().join(file_name))
    }

    fn get_private_file(&self, file_name: &str, peer_id: NodeId) -> Response {
        if file_name.contains("..") {
            return Response::NoSuchFile;
        }
        Self::get_file(&self.get_private_dir(peer_id).join(file_name))
    }

    fn get_file(path: &PathBuf) -> Response {
        match fs::read_to_string(path) {
            Ok(data) => Response::File(data),
            Err(_) => Response::NoSuchFile,
        }
    }

    fn write_public_file(&self, file_name: &str, data: &str) {
        if file_name.contains("..") {
            return;
        }
        Self::write_file(&self.get_public_dir().join(file_name), data);
    }

    fn write_private_file(&self, file_name: &str, data: &str, peer_id: NodeId) {
        if file_name.contains("..") {
            return;
        }
        Self::write_file(&self.get_private_dir(peer_id).join(file_name), data);
    }

    fn write_file(path: &PathBuf, data: &str) {
        fs::write(path, data).expect("Failed to write file");
    }
}
