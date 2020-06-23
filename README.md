# Elsa

## Description

Elsa is a full-featured Kafka library written in Elixir and extending the `:brod` library with additional support from the `:kafka_protocol` Erlang libraries to provide capabilities not available in `:brod`.

Elsa has the following goals:
* Run entirely as a library (only start processes explicitly listed in your supervision tree)
* Draw _none_ of its configuration from the calling application's app env
* Minimize and isolate failures in the broker/consumer interaction

As Elsa draws from the Brod library (named for Kafka friend and biographer Max Brod), it is named for Elsa Taussig, Brod's wife.

## Installation

The package can be installed by adding `elsa` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:elsa, "~> 1.0.0-rc.1"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/elsa](https://hexdocs.pm/elsa).

## Usage

To use Elsa in your application, configure the start of the top-level supervisor
to run and pass it the necessary arguments as a keyword list. Below is an example
of the arguments as they might be defined in the application environment `config.exs`
or `runtime.exs` files.

```elixir
config :my_app, :elsa,
  endpoints: [localhost: 9092],
  name: :myapp_elsa,
  producer: [
    topic: "outgoing-stream"
  ],
  group_consumer: [
    group: "data-stream-group",
    topics: ["incoming-streaming"],
    handler: MyApp.MessageHandler,
    handler_init_args: %{},
    config: [
      begin_offset: :earliest
    ]
  ],
  consumer: [
    topic: "incoming-stream",
    partition: 0,
    begin_offset: :earliest,
    handler: MyApp.MessageHandler
  ]
```

Note that each Elsa supervisor tree requires a single list of Kafka brokers
and a single name. Beyond that, defining consumer groups or producers is optional
(although defining neither will result in a relatively useless Elsa supervisor and
registry).

Producers may be a single producer or a list of producers, differentiated by their
topic, therefore Elsa allows a one-to-many association of supervision tree to producers.

Consumers and consumer groups, in contrast, have a one-to-one relationship to an Elsa supervision
tree, therefore you cannot define a nested list of `consumer` or `group_consumer` keyword arguments
within your Elsa configuration.

### Example

You can find an example of configuring and using Elsa [here](https://github.com/jdenen/let_it_go).

## Testing

Elsa uses the standard ExUnit testing library for unit testing. For integration testing interactions with Kafka, it uses the [`divo`](https://github.com/smartcitiesdata/divo) library. Run tests with the command `mix test.integration`.
