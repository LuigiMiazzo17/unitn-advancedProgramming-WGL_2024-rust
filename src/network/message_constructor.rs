use log::{debug, error, info, trace};

use wg_2024::packet::Fragment;

pub struct MessageConstructor {
    log_target: String,
    fragments_len: u64,
    fragments: Vec<Option<Fragment>>,
    completed_up_to: u64,
}

impl MessageConstructor {
    pub fn new(log_target: String, fragments_len: u64) -> Self {
        trace!(target: &log_target, "Creating new MessageConstructor");
        MessageConstructor {
            log_target,
            fragments_len,
            fragments: vec![None; fragments_len as usize],
            completed_up_to: 0,
        }
    }

    pub fn add_packet(&mut self, fragment: Fragment) -> Result<Option<Vec<u8>>, ()> {
        debug!(target: &self.log_target, "Adding fragment {:?}", fragment);
        let fragment_index = fragment.fragment_index;
        if fragment_index >= self.fragments_len {
            error!(target: &self.log_target, "Fragment index out of bounds");
            return Err(());
        }

        self.fragments[fragment_index as usize] = Some(fragment);

        loop {
            if self.fragments[self.completed_up_to as usize].is_some() {
                self.completed_up_to += 1;
            } else {
                break;
            }

            if self.completed_up_to == self.fragments_len {
                let mut message = Vec::new();
                for fragment in self.fragments.iter() {
                    if let Some(fragment) = fragment {
                        debug!(target: &self.log_target, "Adding fragment to message {:?}", fragment);
                        message.extend_from_slice(&fragment.data);
                    } else {
                        error!(target: &self.log_target, "Fragment missing after completion");
                        unreachable!();
                    }
                }

                info!(target: &self.log_target, "Message completed");
                return Ok(Some(message));
            }
        }

        Ok(None)
    }
}
