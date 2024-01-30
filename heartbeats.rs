use std::collections::HashMap;
use std::time::Duration;

use hydroflow_plus::*;
use stageleft::*;

use super::heartbeats_protocol::Message;

pub fn heartbeats<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) -> D::Cluster {
    let cluster = flow.cluster(cluster_spec);

    // members: a persistent hf+ collection of the cluster ids
    let members = cluster.source_iter(cluster.ids()).all_ticks();

    // generate a heartbeat every 100ms
    let hbs = cluster
        .source_interval(q!(Duration::from_millis(100)))
        .map(q!(|_| Message::Heartbeat))
        .tick_batch();

    // generate a heartbeat msg for each recipient
    let node_ack_pairs = hbs
        .cross_product(&members.cloned())
        .map(q!(|(msg, id)| (id, msg)));

    // scatter heartbeats, gather acks
    let acks = hbs
        .broadcast_bincode_tagged(&cluster) // broadcast to cluster, tagged with sender id
        // at each cluster member
        .inspect(q!(|n| println!("Received {:?}", n)))
        .map(q!(|(id, _m)| (id, Message::HeartbeatAck))) // generate an Ack
        .filter(q!(|_m| rand::random::<f32>() < 0.3)) // artifical drop for testing
        .demux_bincode_tagged(&cluster) // return Ack to sender
        // back at sender
        .inspect(q!(|n| println!("Got Ack {:?}", n)))
        .tick_batch();

    // track each nodes sent heartbeats and sets the unacked count to 0 when an ack comes in
    let unacked_hbs = node_ack_pairs.union(&acks).all_ticks().fold(
        q!(HashMap::<u32, usize>::new), // state is a hashmap from node_id => count
        q!(|accum, (id, msg)| {
            match msg {
                Message::Heartbeat => {
                    // increment for each Heartbeat
                    *accum.entry(id).or_insert(0) += 1
                }
                Message::HeartbeatAck => {
                    // reset to 0 on any HeartbeatAck
                    *accum.entry(id).or_insert(0) = 0
                }
            }
        }),
    );

    // every second, check which nodes have missed their last 3 heartbeats or more
    cluster
        .source_interval(q!(Duration::from_millis(1000)))
        .tick_batch()
        .cross_product(&unacked_hbs) // attach a handle to the unacked_hbs state
        .map(q!(|(_, unacked_hbs)| unacked_hbs))
        .flat_map(q!(|h| h.into_iter())) // go through the entries of unacked_hbs
        .filter(q!(|(_key, value)| *value > 3)) // a node has counted 3 unanswered acks in a row
        .for_each(q!(|n| println!("---------\ndead_list: {:?}\n---------", n)));

    cluster
}

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn heartbeats_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let _ = heartbeats(flow, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}

