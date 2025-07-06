use log::{Level, LevelFilter, Log, Metadata, Record};
use serde::{Serialize, Serializer};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Clone)]
pub struct LogEntry {
    #[serde(serialize_with = "serialize_level")]
    pub level: Level,
    pub message: String,
}

fn serialize_level<S>(level: &Level, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(level.as_str())
}

pub struct InMemoryLogger {
    logs: Arc<Mutex<Vec<LogEntry>>>,
}

impl Default for InMemoryLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryLogger {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_logs(&self) -> Vec<LogEntry> {
        self.logs.lock().unwrap().clone()
    }
}

impl Log for InMemoryLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Trace
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let mut logs = self.logs.lock().unwrap();
            logs.push(LogEntry {
                level: record.level(),
                message: format!("{}", record.args()),
            });
        }
    }

    fn flush(&self) {}
}

pub fn init(logger: &'static InMemoryLogger) -> Result<(), log::SetLoggerError> {
    log::set_logger(logger).map(|()| log::set_max_level(LevelFilter::Trace))
}
