use wg_2024::packet::Fragment;

pub struct MessageConstructor {
    fragments_len: u64,
    fragments: Vec<Option<Fragment>>,
    completed_up_to: u64,
}

impl MessageConstructor {
    pub fn new(fragments_len: u64) -> Self {
        MessageConstructor {
            fragments_len,
            fragments: vec![None; fragments_len as usize],
            completed_up_to: 0,
        }
    }

    pub fn add_packet(&mut self, fragment: Fragment) -> Result<Option<Vec<u8>>, ()> {
        let fragment_index = fragment.fragment_index;
        if fragment_index >= self.fragments_len {
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
                        message.extend_from_slice(&fragment.data);
                    } else {
                        unreachable!();
                    }
                }

                return Ok(Some(message));
            }
        }

        Ok(None)
    }
}
