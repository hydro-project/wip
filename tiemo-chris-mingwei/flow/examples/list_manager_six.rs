use std::collections::VecDeque;

use hydroflow::hydroflow_syntax;

/// Each lock has owners and waiters.

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum LockMode {
    NL,
    IS,
    IX,
    S,
    SIX,
    X,
}
impl LockMode {
    pub fn compatible(self, other: Self) -> bool {
        match (self, other) {
            (LockMode::NL, LockMode::NL) => true,
            (LockMode::NL, LockMode::IS) => true,
            (LockMode::NL, LockMode::IX) => true,
            (LockMode::NL, LockMode::S) => true,
            (LockMode::NL, LockMode::SIX) => true,
            (LockMode::NL, LockMode::X) => true,
            (LockMode::IS, LockMode::NL) => false,
            (LockMode::IS, LockMode::IS) => true,
            (LockMode::IS, LockMode::IX) => true,
            (LockMode::IS, LockMode::S) => true,
            (LockMode::IS, LockMode::SIX) => true,
            (LockMode::IS, LockMode::X) => false,
            (LockMode::IX, LockMode::NL) => true,
            (LockMode::IX, LockMode::IS) => true,
            (LockMode::IX, LockMode::IX) => true,
            (LockMode::IX, LockMode::S) => false,
            (LockMode::IX, LockMode::SIX) => false,
            (LockMode::IX, LockMode::X) => false,
            (LockMode::S, LockMode::NL) => true,
            (LockMode::S, LockMode::IS) => true,
            (LockMode::S, LockMode::IX) => false,
            (LockMode::S, LockMode::S) => true,
            (LockMode::S, LockMode::SIX) => false,
            (LockMode::S, LockMode::X) => false,
            (LockMode::SIX, LockMode::NL) => true,
            (LockMode::SIX, LockMode::IS) => true,
            (LockMode::SIX, LockMode::IX) => false,
            (LockMode::SIX, LockMode::S) => false,
            (LockMode::SIX, LockMode::SIX) => false,
            (LockMode::SIX, LockMode::X) => false,
            (LockMode::X, LockMode::NL) => true,
            (LockMode::X, LockMode::IS) => false,
            (LockMode::X, LockMode::IX) => false,
            (LockMode::X, LockMode::S) => false,
            (LockMode::X, LockMode::SIX) => false,
            (LockMode::X, LockMode::X) => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct LockRequest {
    client_id: &'static str,
    requested_state: LockMode,
}

pub fn main() {
    let (items_send, items_recv) =
        hydroflow::util::unbounded_channel::<(&'static str, LockRequest)>();

    let mut flow = hydroflow_syntax! {
        source_stream(items_recv)
            -> fold_keyed::<'static>(|| VecDeque::<LockRequest>::new(), |queue: &mut VecDeque<_>, req: LockRequest| {
                if LockMode::NL == req.requested_state {
                    queue.retain(|queue_req| queue_req.client_id != req.client_id);
                } else {
                    queue.push_back(req);
                }
            })
            -> flat_map(|(lock_id, queue)| {
                let mut output = Vec::new();
                let mut lock_state = LockMode::NL;
                for locked in queue {
                    if !lock_state.compatible(locked.requested_state) {
                        break;
                    }
                    lock_state = locked.requested_state;
                    output.push(locked);
                }
                output.into_iter().map(move |locked| (lock_id, locked))
            })
            -> for_each(|x| println!("{}: {:?}", context.current_tick(), x));
    };

    let reqs = [
        (
            "foo_lock",
            LockRequest {
                client_id: "joe",
                requested_state: LockMode::S,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "shadaj",
                requested_state: LockMode::S,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "mingwei",
                requested_state: LockMode::X,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "shadaj",
                requested_state: LockMode::NL,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "chris",
                requested_state: LockMode::S,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "tiemo",
                requested_state: LockMode::S,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "joe",
                requested_state: LockMode::NL,
            },
        ),
        (
            "foo_lock",
            LockRequest {
                client_id: "mingwei",
                requested_state: LockMode::NL,
            },
        ),
    ];

    for req in reqs {
        items_send.send(req).unwrap();

        flow.run_available();
    }
}

// Key: (TransactionId, MachineId)
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Key {
    transaction_id: usize,
    machine_id: usize,
}


// Client entry point: source of transaction commands (transaction id, command type)
/*
begin txn: (0, begin_txn)
acquire: (transaction id, acquire(entity id, mode))
release: (transaction id, release(entity id))
commit txn: (transaction id, commit)
abort txn: (transaction id, abort)
*/

// client to socket mapping ()