#[tokio::main]
async fn main() {
    hydroflow_plus::util::cli::launch(|ports| {
        let hf = flow::first_ten_distributed::first_ten_distributed_runtime!(&ports);
        hf.meta_graph().unwrap().open_mermaid(&Default::default()).unwrap();
        hf
    })
    .await;
}
