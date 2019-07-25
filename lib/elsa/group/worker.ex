defmodule Elsa.Group.Worker do
  @moduledoc """
  Defines the worker GenServer that is managed by the DynamicSupervisor.
  Workers are instantiated and assigned to a specific topic/partition
  and process messages according to the specified message handler module
  passed in from the manager before calling the manager's ack function to
  notify the cluster the messages have been successfully processed.
  """
  use GenServer, restart: :temporary
  require Logger

  import Elsa.Group.Supervisor, only: [registry: 1]
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")

  @subscribe_delay 200
  @subscribe_retries 20

  defmodule State do
    @moduledoc """
    The running state of the worker process.
    """
    defstruct [
      :name,
      :topic,
      :partition,
      :generation_id,
      :offset,
      :subscriber_pid,
      :handler,
      :handler_init_args,
      :handler_state,
      :config
    ]
  end

  @doc """
  Trigger the worker to gracefully disengage itself
  from the supervision tree, unsubscribe from the topic
  and partition and initiate its own stop sequence.
  """
  @spec unsubscribe(pid()) :: {:stop, :normal, term(), struct()}
  def unsubscribe(pid) do
    GenServer.call(pid, :unsubscribe)
  end

  @doc """
  Start the worker process and init the state with the given config.
  """
  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init(init_args) do
    state = %State{
      name: Keyword.fetch!(init_args, :name),
      topic: Keyword.fetch!(init_args, :topic),
      partition: Keyword.fetch!(init_args, :partition),
      generation_id: Keyword.fetch!(init_args, :generation_id),
      offset: Keyword.fetch!(init_args, :begin_offset),
      handler: Keyword.fetch!(init_args, :handler),
      handler_init_args: Keyword.fetch!(init_args, :handler_init_args),
      config: Keyword.fetch!(init_args, :config)
    }

    Process.put(:elsa_name, state.name)
    Process.put(:elsa_topic, state.topic)
    Process.put(:elsa_partition, state.partition)
    Process.put(:elsa_generation_id, state.generation_id)

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
    transformed_messages = transform_messages(topic, partition, messages, state)

    case send_messages_to_handler(transformed_messages, state) do
      {ack, new_handler_state} when ack in [:ack, :acknowledge] ->
        offset = transformed_messages |> List.last() |> Map.get(:offset)
        ack_messages(topic, partition, offset, state)
        {:noreply, %{state | offset: offset, handler_state: new_handler_state}}

      {ack, offset, new_handler_state} when ack in [:ack, :acknowledge] ->
        ack_messages(topic, partition, offset, state)
        {:noreply, %{state | offset: offset, handler_state: new_handler_state}}

      {no_ack, new_handler_state} when no_ack in [:no_ack, :noop] ->
        {:noreply, %{state | handler_state: new_handler_state}}

      {:continue, new_handler_state} ->
        offset = transformed_messages |> List.last() |> Map.get(:offset)
        :brod.consume_ack(state.name, topic, partition, offset)
        {:noreply, %{state | handler_state: new_handler_state}}
    end
  end

  def handle_call(:unsubscribe, _from, state) do
    Process.unlink(state.subscriber_pid)
    result = :brod.unsubscribe(state.name, state.topic, state.partition)
    {:stop, :normal, result, state}
  end

  defp transform_messages(topic, partition, messages, state) do
    Enum.map(messages, &Elsa.Message.new(&1, topic: topic, partition: partition, generation_id: state.generation_id))
  end

  defp send_messages_to_handler(messages, state) do
    state.handler.handle_messages(messages, state.handler_state)
  end

  defp ack_messages(topic, partition, offset, state) do
    Elsa.Group.Manager.ack(state.name, topic, partition, state.generation_id, offset)

    offset
  end

  defp subscribe(state, retries \\ @subscribe_retries)

  defp subscribe(state, 0) do
    Logger.error("Unable to subscribe to topic #{state.topic} partition #{state.partition} offset #{state.offset}")
    {:error, :failed_subscription}
  end

  defp subscribe(state, retries) do
    opts = determine_subscriber_opts(state)

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

  defp determine_subscriber_opts(state) do
    begin_offset =
      case state.offset do
        :undefined ->
          Keyword.get(state.config, :begin_offset, :latest)

        offset ->
          offset
      end

    Keyword.put(state.config, :begin_offset, begin_offset)
  end
end
