defmodule Elsa.Group.Worker do
  use GenServer, restart: :temporary
  require Logger

  import Elsa.Group.Supervisor, only: [registry: 1]
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")

  @subscribe_delay 200
  @subscribe_retries 20

  defmodule State do
    defstruct [
      :name,
      :group,
      :topic,
      :partition,
      :offset,
      :subscriber_pid,
      :handler,
      :handler_init_args,
      :handler_state
    ]
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init(init_args) do
    state = %State{
      name: Keyword.fetch!(init_args, :name),
      group: Keyword.fetch!(init_args, :group),
      topic: Keyword.fetch!(init_args, :topic),
      partition: Keyword.fetch!(init_args, :partition),
      offset: Keyword.fetch!(init_args, :begin_offset),
      handler: Keyword.fetch!(init_args, :handler),
      handler_init_args: Keyword.fetch!(init_args, :handler_init_args)
    }

    Registry.register(registry(state.name), :"worker_#{state.topic}_#{state.partition}", nil)

    {:ok, handler_state} = state.handler.init(state.handler_init_args)

    {:ok, %{state | handler_state: handler_state}, {:continue, :subscribe}}
  end

  def handle_continue(:subscribe, state) do
    case subscribe(state) do
      {:ok, pid} ->
        Process.link(pid)
        {:noreply, %{state | subscriber_pid: pid}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info({_consumer_pid, kafka_message_set(topic: topic, partition: partition, messages: messages)}, state) do
    IO.inspect(messages, label: "worker messages")

    {:ack, new_handler_state} = send_messages_to_handler(topic, partition, messages, state)
    offset = ack_messages(topic, partition, messages, state)

    {:noreply, %{state | offset: offset, handler_state: new_handler_state}}
  end

  defp send_messages_to_handler(topic, partition, messages, state) do
    messages
    |> Enum.map(&transform_message(topic, partition, &1))
    |> state.handler.handle_messages(state.handler_state)
  end

  defp ack_messages(topic, partition, messages, state) do
    offset = messages |> List.last() |> kafka_message(:offset)

    Elsa.Group.Manager.ack(state.name, topic, partition, offset)
    :ok = :brod.consume_ack(state.name, topic, partition, offset)

    offset
  end

  defp transform_message(topic, partition, kafka_message(offset: offset, key: key, value: value)) do
    %{
      topic: topic,
      partition: partition,
      offset: offset,
      key: key,
      value: value
    }
  end

  defp subscribe(state, retries \\ @subscribe_retries)

  defp subscribe(state, 0) do
    Logger.error("Unable to subscribe to topic #{state.topic} partition #{state.partition} offset #{state.offset}")
    {:error, :failed_subscription}
  end

  defp subscribe(state, retries) do
    opts = [begin_offset: offset(state.offset)]

    case :brod.subscribe(state.name, self(), state.topic, state.partition, opts) do
      {:error, reason} ->
        Logger.warn(
          "Retrying to subscribe to topic #{state.topic} parition #{state.partition} offset #{state.offset} reason #{
            inspect(reason)
          }"
        )

        Process.sleep(@subscribe_delay)
        subscribe(state, retries - 1)

      {:ok, subscriber_pid} ->
        Logger.info("Subscribing to topic #{state.topic} partition #{state.partition} offset #{state.offset}")
        {:ok, subscriber_pid}
    end
  end

  defp offset(:undefined), do: :earliest
  defp offset(offset), do: offset
end
