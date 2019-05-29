defmodule Elsa.Consumer.Worker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    client = Keyword.fetch!(opts, :client)
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    config = Keyword.get(opts, :config, [])

    state = %{
      client: client,
      topic: topic,
      partition: partition,
      config: config
    }

    {:ok, state, {:continue, :subscribe}}
  end

  def handle_continue(:subscribe, state) do
    :brod.subscribe(state.client, self(), state.topic, state.partition, state.config)
    {:noreply, state}
  end

  def handle_info(message, state) do
    IO.inspect(message, label: "message")
    {:noreply, state}
  end
end
