# Elsa

## Description

Elsa is a full-featured Kafka library written in Elixir and extending the `:brod` library with additional support from the `:kafka_protocol` Erlang libraries to provide capabilities not available in `:brod`.

Elsa has the following goals:
* Run entirely as a library (only start processes explicitly listed in your supervision tree)
* Draw _none_ of its configuration from the calling application's app env
* Minimize and isolate failures in the broker/consumer interaction

As Elsa draws from the Brod library (named for Kafka friend and biographer Max Brod), it is named for Elsa Taussig, Brod's wife.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `elsa` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:elsa, "~> 0.5.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/elsa](https://hexdocs.pm/elsa).

## Testing

Elsa uses the standard ExUnit testing library for unit testing. For integration testing interactions with Kafka, it uses the [`divo`](https://github.com/smartcitiesdata/divo) library. Run tests with the command `mix test.integration`.
