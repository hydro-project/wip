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

    // generate heartbeats every 100ms
    let hbs = cluster
        .source_interval(q!(Duration::from_millis(100)))
        .map(q!(|_| Message::Heartbeat))
        .tick_batch();

    // helper to count how many heartbeats we have sent to each node
    // we generate a 1 for each heartbeat we send and sum these into the totals later
    let node_incr_pairs = hbs
        .map(q!(|_| 1i32))
        .cross_product(&cluster.source_iter(cluster.ids()).all_ticks().cloned())
        .map(q!(|(incr, id)| (id, incr)));

    // send/receive heartbeats
    let acks = hbs
        .broadcast_bincode_tagged(&cluster)
        .inspect(q!(|n| println!("Received {:?}", n)))
        .demux_bincode_tagged(&cluster)
        .inspect(q!(|n| println!("Acked {:?}", n)))
        //use a -1 to decrement the outsanding heartbeats for each node we hear an ack from
        .map(q!(|(id, _)| (id, -1i32)))
        .tick_batch();

    // track each nodes sent heartbeats and sets the unacked count to 0 when an ack comes in
    let unacked_hbs = node_incr_pairs.union(&acks).all_ticks().fold(
        q!(HashMap::<u32, i32>::new),
        q!(|accum, (id, m)| {
            if (m > 0) {
                *accum.entry(id).or_insert(0) += m
            } else {
                accum.entry(id).or_insert(0);
            }
        }),
    );

    // every second, check which nodes have missed their last 3 heartbeats or more
    cluster
        .source_interval(q!(Duration::from_millis(1000)))
        .tick_batch()
        .cross_product(&unacked_hbs)
        .map(q!(|(_, unacked_hbs)| unacked_hbs))
        .flat_map(q!(|h| h.into_iter()))
        .filter(q!(|(key, value)| *value > 3))
        .for_each(q!(|n| println!("dead_list: {:?}", n)));

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
