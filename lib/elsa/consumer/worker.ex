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
      config: config,
      subscriber_pid: nil,
      subscriber_ref: nil
    }

    {:ok, state, {:continue, :subscribe}}
  end

  def handle_continue(:subscribe, state) do
    new_state = subcribe_and_monitor(state)
    {:noreply, new_state}
  end

  def handle_info({:DOWN, ref, :process, _object, _reason}, %{subscriber_ref: subscriber_ref} = state) do
    new_state =
      case ref do
        ^subscriber_ref -> subcribe_and_monitor(state)
        _ -> state
      end

    {:noreply, new_state}
  end

  def handle_info(message, state) do
    IO.inspect(message, label: "message")
    {:noreply, state}
  end

  defp subscribe_and_monitor(state) do
    {:ok, subscriber_pid} = :brod.subscribe(state.client, self(), state.topic, state.partition, state.config)
    ref = Process.monitor(subscriber_pid)
    %{state | subscriber_pid: subscriber_pid, subscriber_ref: ref}
  end
end
