use hydroflow_plus::*;
use stageleft::*;

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

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct EntityId(pub usize);

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct TransactionId(pub usize);

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct MachineId(pub usize);
// Key: (TransactionId, MachineId)
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Key {
    transaction_id: TransactionId,
    machine_id: MachineId,
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

pub enum ClientRequest {
    BeginTransaction,
    Acquire {
        entity_id: EntityId,
        mode: LockMode,
    },
    Release {
        entity_id: usize,
    },
    Commit,
    Abort,
}

fn process_client_requests<'a, D: Deploy<'a>>(
    begin_transaction_reqs: Stream<'a, MachineId, stream::Windowed, D::Process>,
    acquire_reqs: Stream<'a, (Key, EntityId, LockMode), stream::Windowed, D::Process>,
    release_reqs: Stream<'a, (Key, EntityId), stream::Windowed, D::Process>,
    commit_reqs: Stream<'a, Key, stream::Windowed, D::Process>,
    abort_reqs: Stream<'a, Key, stream::Windowed, D::Process>,
    // cluster: D::Cluster,
) -> Stream<'a, i32, stream::Async, D::Cluster> {

    acquire_reqs
}

pub fn first_ten_distributed<'a, D: Deploy<'a>>(
    flow: &'a FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    let process = flow.process(process_spec);
    // let second_process = flow.process(process_spec);
    let cluster = flow.cluster(cluster_spec);

    let numbers = process.source_iter(q!(0..10));
    numbers
        .broadcast_bincode(&cluster)
        .for_each(q!(|n| println!("{}", n)));

    // second_process
}

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};

#[stageleft::entry]
pub fn first_ten_distributed_runtime<'a>(
    flow: &'a FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    // let _ =
    first_ten_distributed(flow, &cli, &cli);
    flow.build(q!(cli.meta.subgraph_id))
}

#[stageleft::runtime]
#[cfg(test)]
mod tests {
    use hydro_deploy::{Deployment, HydroflowCrate};
    use hydroflow_plus::futures::StreamExt;
    use hydroflow_plus_cli_integration::{DeployCrateWrapper, DeployProcessSpec};

    // #[tokio::test]
    // async fn first_ten_distributed() {
    //     let mut deployment = Deployment::new();
    //     let localhost = deployment.Localhost();

    //     let flow = hydroflow_plus::FlowBuilder::new();
    //     let second_process = super::first_ten_distributed(
    //         &flow,
    //         &DeployProcessSpec::new(|| {
    //             deployment.add_service(
    //                 HydroflowCrate::new(".", localhost.clone())
    //                     .bin("first_ten_distributed")
    //                     .profile("dev"),
    //             )
    //         }),
    //     );

    //     deployment.deploy().await.unwrap();

    //     let second_process_stdout = second_process.stdout().await;

    //     deployment.start().await.unwrap();

    //     assert_eq!(
    //         second_process_stdout.take(10).collect::<Vec<_>>().await,
    //         vec!["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    //     );
    // }
}
