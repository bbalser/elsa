use futures::executor::block_on;
use rdkafka::admin::{AdminClient, AdminOptions, ConfigResource, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaError;
use rdkafka::ClientConfig;
use rustler::{Encoder, Env, Error, Term, types};

mod atoms {
    rustler::rustler_atoms! {
        atom ok;
        //atom error;
        //atom __true__ = "true";
        //atom __false__ = "false";
    }
}

rustler::rustler_export_nifs! {
    "Elixir.Elsa.Admin",
    [
        ("add", 2, add),
        ("describe_topic_nif", 2, describe_topic_nif)
    ],
    None
}

fn describe_topic_nif<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let brokers: &str = args[0].decode()?;
    let topic: &str = args[1].decode()?;

    let result = block_on(describe_topic_inner(brokers, topic));
    let config_hash = result[0]
        .as_ref()
        .ok()
        .unwrap();
    println!("{:?}", config_hash);
    let config_map = types::map::map_new(env);

    config_hash
        .entries
        .iter()
        .map(|entry|
             {
                 let entry_map = types::map::map_new(env);
                 entry_map.map_put("value".encode(env), entry.value.encode(env));
                 config_map.map_put(entry.name.encode(env), entry_map.encode(env))
             }
        ).collect();

    Ok((atoms::ok(), config_map).encode(env))
}

async fn describe_topic_inner(brokers: &str, topic: &str) -> Vec<Result<ConfigResource, RDKafkaError>> {
    let admin_client = create_client(brokers);
    let resource = [ResourceSpecifier::Topic(&topic)];
    let opts = AdminOptions::new();

    admin_client
        .describe_configs(
            &resource,
            &opts,
        )
        .await
        .expect("describe configs failed")
}

fn create_client(brokers: &str) -> AdminClient<DefaultClientContext> {
    let mut client = ClientConfig::new();
    client.set("bootstrap.servers", brokers)
        .create()
        .expect("admin client creation failed")
}

fn add<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let num1: i64 = args[0].decode()?;
    let num2: i64 = args[1].decode()?;

    Ok((atoms::ok(), num1 + num2).encode(env))
}
