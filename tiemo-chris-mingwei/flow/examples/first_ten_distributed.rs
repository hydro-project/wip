use std::cell::RefCell;

use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow_plus_cli_integration::{DeployClusterSpec, DeployProcessSpec};

#[tokio::main]
async fn main() {
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();
    let deployment = RefCell::new(deployment);

    let flow = hydroflow_plus::FlowBuilder::new();
    flow::first_ten_distributed::first_ten_distributed(
        &flow,
        &DeployProcessSpec::new(|| {
            deployment.borrow_mut().add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("first_ten_distributed")
                    .profile("dev"),
            )
        }),
        &DeployClusterSpec::new(|| {
            vec![
                deployment.borrow_mut().add_service(
                    HydroflowCrate::new(".", localhost.clone())
                        .bin("first_ten_distributed")
                        .profile("dev"),
                ),
                deployment.borrow_mut().add_service(
                    HydroflowCrate::new(".", localhost.clone())
                        .bin("first_ten_distributed")
                        .profile("dev"),
                ),
                deployment.borrow_mut().add_service(
                    HydroflowCrate::new(".", localhost.clone())
                        .bin("first_ten_distributed")
                        .profile("dev"),
                )
            ]
        }),
    );

    let mut deployment = deployment.into_inner();

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap()
}
