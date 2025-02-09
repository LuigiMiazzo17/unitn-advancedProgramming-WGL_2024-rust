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
                Request::ListFiles => Some(Message::Response(self.get_files())),
                Request::GetFile(file_name) => {
                    let data = self.get_file(&file_name);
                    Some(Message::Response(Response::File(data)))
                }
                Request::WriteFile(file_name, data) => {
                    self.write_file(&file_name, &data);
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
}

impl ContentServer {
    pub fn new(node_id: NodeId, base_path: String) -> Self {
        let path = PathBuf::new().join(base_path);
        if !path.is_dir() {
            panic!("Path is not a directory: {:?}", path);
        }

        let base_path = path.join(node_id.to_string());

        fs::create_dir_all(&base_path).expect("Failed to create directory");

        ContentServer { base_path }
    }

    fn handle_server_type_request(&self) -> Response {
        Response::ServerType(ServerType::Content)
    }

    fn get_files(&self) -> Response {
        let files = fs::read_dir(&self.base_path)
            .expect("Failed to read directory")
            .map(|entry| {
                entry
                    .expect("Failed to read entry")
                    .file_name()
                    .into_string()
                    .expect("Failed to convert OsString to String")
            })
            .collect();

        Response::Files(files)
    }

    fn get_file(&self, file_name: &str) -> String {
        if file_name.contains("..") {
            return String::new();
        }
        fs::read_to_string(self.base_path.join(file_name)).expect("Failed to read file")
    }

    fn write_file(&self, file_name: &str, data: &str) {
        if file_name.contains("..") {
            return;
        }
        fs::write(self.base_path.join(file_name), data).expect("Failed to write file");
    }
}
