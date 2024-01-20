use hydroflow_plus::*;
use stageleft::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct Ballot { // Note: Important that num comes before id, since Ord is defined lexicographically
    num: u32,
    id: u32,
}

#[derive(Serialize, Deserialize, Clone)]
struct P1a {
    ballot: Ballot,
}

#[derive(Serialize, Deserialize, Clone)]
struct LogValue {
    ballot: Ballot,
    slot: u32,
    value: u32,
}

#[derive(Serialize, Deserialize, Clone)]
struct P1b {
    ballot: Ballot,
    max_ballot: Ballot,
    accepted: Vec<LogValue>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
struct P2a {
    ballot: Ballot,
    slot: u32,
    value: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
struct P2b {
    ballot: Ballot,
    max_ballot: Ballot,
    slot: u32,
    value: u32,
}

pub fn paxos<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>
) {
    let leader = flow.process(process_spec);
    let acceptors = flow.cluster(cluster_spec);

    /*
        Proposer state
     */

    //TODO: Find leader ID programmatically
    let p_id = leader
        .source_iter(q!([0]))
        .all_ticks();
    let p_ballot_nums = leader
        .source_iter(q!([1]))
        .all_ticks();
    let p_ballot = p_ballot_nums
        .reduce(q!(|a, b| if b > *a {
            *a = b
        }))
        .cross_product(&p_id)
        .map(q!(|(num, id)| Ballot {id, num}));

    /*
        Proposer client data input
     */

    let data_input: Stream<'_, P2a, stream::Windowed, <D as Deploy>::Process> = leader
        .source_iter(q!(0..10))
        .cross_product(&p_ballot)
        .map(q!(|(val, ballot)| P2a {
            ballot: ballot,
            slot: val,
            value: val,
        }));
    let a_p2a = data_input
        .broadcast_bincode(&acceptors);

    /*
        Proposer p1a
     */

    /*
        Acceptor state
     */

    let a_ballots = acceptors
        .source_iter(q!([Ballot{id: 0, num: 0}]))
        .all_ticks();
    let a_max_ballot = a_ballots
        .reduce(q!(|a, b| if b > *a {
            *a = b
        }));

    /*
        Acceptor p1b
     */

    /*
        Acceptor p2a
     */

    let a_p2a_and_max_ballot = a_p2a
        .tick_batch()
        .cross_product(&a_max_ballot);
    // TODO: This log includes overwritten values
    let a_log = a_p2a_and_max_ballot
        .filter_map(q!(|(p2a, max_ballot): (P2a, Ballot)| if p2a.ballot >= max_ballot {
            Some(LogValue {
                ballot: p2a.ballot,
                slot: p2a.slot,
                value: p2a.value,
            })
        } else {
            None
        }));
    let p_p2b = a_p2a_and_max_ballot
        .map(q!(|(p2a, max_ballot): (P2a, Ballot)| P2b {
            ballot: p2a.ballot,
            max_ballot: max_ballot,
            slot: p2a.slot,
            value: p2a.value,
        }))
        .send_bincode_tagged(&leader);

    /*
        Proposer p2b
     */

    // TODO: garbage collect p2bs where all replied & those whose ballots are no longer relevant
    let p_p2b_relevant = p_p2b
        .all_ticks()
        .cross_product(&p_ballot)
        .filter(q!(|((a_id, p2b), curr_ballot)| p2b.ballot == *curr_ballot));

    let p_p2b_count = p_p2b_relevant
        .map(q!(|((a_id, p2b), _curr_ballot)| ((p2b.slot, p2b.value), a_id))) // reformat so we can group by slot
        .fold_keyed(q!(|| 0), q!(|accum, a_id| *accum += 1)) // TODO: count num unique a_ids
        .filter_map(q!(|((slot, value), cnt)| if cnt >= 2 { // TODO: define f
            Some((slot, value))
        } else {
            None
        }))
        .for_each(q!(|(slot, value): (u32, u32)| println!("Committed {}: {}", slot, value)));
}

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn paxos_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let _ = paxos(flow, &cli, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}

#[stageleft::runtime]
#[cfg(test)]
mod tests {
    use hydro_deploy::{Deployment, HydroflowCrate};
    use hydroflow_plus::futures::StreamExt;
    use hydroflow_plus_cli_integration::{DeployCrateWrapper, DeployProcessSpec, DeployClusterSpec};
    use std::cell::RefCell;

    #[tokio::test]
    async fn paxos() {
        let bin = "paxos";
        let profile = "dev";
        let deployment = RefCell::new(Deployment::new());
    let localhost = deployment.borrow_mut().Localhost();

        let flow = hydroflow_plus::FlowBuilder::new();

        let second_process = super::paxos(
            &flow,
            &DeployProcessSpec::new(|| {
                let mut deployment = deployment.borrow_mut();
                deployment.add_service(
                    HydroflowCrate::new(".", localhost.clone())
                        .bin(bin)
                        .profile(profile)
                        .display_name("leader"),
                )
            }),
            &DeployClusterSpec::new(|| {
                (0..=2) // TODO: define f
                    .map(|idx| {
                        let mut deployment = deployment.borrow_mut();
                        deployment.add_service(
                            HydroflowCrate::new(".", localhost.clone())
                                .bin(bin)
                                .profile(profile)
                                .display_name(format!("acceptor/{}", idx)),
                        )
                    })
                    .collect()
            }),
        );

        let mut deployment = deployment.into_inner();

        deployment.deploy().await.unwrap();

        deployment.start().await.unwrap();

        tokio::signal::ctrl_c().await.unwrap()
    }
}
