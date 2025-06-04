use log::{debug, error, info, trace, warn};

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
        let frag_index = fragment.fragment_index;
        if frag_index >= self.fragments_len {
            error!(target: &self.log_target, "Fragment index out of bounds");
            return Err(());
        }

        let frag_slot = self.fragments.get_mut(frag_index as usize);
        match frag_slot {
            Some(buff_slot) => {
                if buff_slot.is_some() {
                    warn!(target: &self.log_target, "Fragment already exists for index: {}/{}", frag_index, self.fragments_len);
                }
                *buff_slot = Some(fragment);
                debug!(target: &self.log_target, "Fragment added at index: {}/{}", frag_index, self.fragments_len);
            }
            None => {
                error!(target: &self.log_target, "Fragment slot is None for index: {}/{}", frag_index, self.fragments_len);
                return Err(());
            }
        };

        while self.fragments.get(self.completed_up_to as usize).is_some() {
            self.completed_up_to += 1;
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

            info!(target: &self.log_target, "Message construction completed");
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
}
