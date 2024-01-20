use std::{sync::Arc, cell::RefCell};

use hydro_deploy::{gcp::GCPNetwork, Deployment, HydroflowCrate};
use hydroflow_plus_cli_integration::{DeployClusterSpec, DeployProcessSpec};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    let gcp_project = std::env::args()
        .nth(1)
        .expect("Expected GCP project as first argument");

    let mut deployment = Deployment::new();
    let deployment = RefCell::new(deployment);
    let vpc = Arc::new(RwLock::new(GCPNetwork::new(&gcp_project, None)));

    let flow = hydroflow_plus::FlowBuilder::new();
    flow::first_ten_distributed::first_ten_distributed(
        &flow,
        &DeployProcessSpec::new(|| {
            let host = deployment.borrow_mut().GCPComputeEngineHost(
                gcp_project.clone(),
                "e2-micro",
                "debian-cloud/debian-11",
                "us-west1-a",
                vpc.clone(),
                None,
            );

            deployment.borrow_mut().add_service(HydroflowCrate::new(".", host).bin("first_ten_distributed"))
        }),
        &DeployClusterSpec::new(|| {
            let host = deployment.borrow_mut().GCPComputeEngineHost(
                gcp_project.clone(),
                "e2-micro",
                "debian-cloud/debian-11",
                "us-west1-a",
                vpc.clone(),
                None,
            );

            vec![
                deployment.borrow_mut().add_service(HydroflowCrate::new(".", host).bin("first_ten_distributed"))
            ]
        }),
    );

    let mut deployment = deployment.into_inner();

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap()
}
