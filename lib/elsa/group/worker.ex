defmodule Elsa.Group.Worker do
  use GenServer
  require Logger

  import Elsa.Group.Supervisor, only: [registry: 1]
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")

  defmodule State do
    defstruct [
      :name,
      :group,
      :topic,
      :partition,
      :offset,
      :generation_id,
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
      generation_id: Keyword.fetch!(init_args, :generation_id),
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
    pid = subscribe(state)
    {:noreply, %{state | subscriber_pid: pid}}
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
    {:ok, group_coordinator_pid} = Registry.meta(registry(state.name), :group_coordinator)

    :ok = :brod_group_coordinator.ack(group_coordinator_pid, state.generation_id, topic, partition, offset)
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

  defp subscribe(state) do
    opts = [begin_offset: offset(state.offset)]
    {:ok, subscriber_pid} = :brod.subscribe(state.name, self(), state.topic, state.partition, opts)
    Logger.info("Subscribing to topic #{state.topic} partition #{state.partition} offset #{state.offset}")
    subscriber_pid
  end

  defp offset(:undefined), do: :earliest
  defp offset(offset), do: offset
end
