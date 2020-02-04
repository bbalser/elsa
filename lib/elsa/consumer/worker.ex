defmodule Elsa.Consumer.Worker do
  @moduledoc """
  Defines the worker GenServer that is managed by the DynamicSupervisor.
  Workers are instantiated and assigned to a specific topic/partition
  and process messages according to the specified message handler module
  passed in from the manager before calling the ack function to
  notify the cluster the messages have been successfully processed.
  """
  use GenServer, restart: :temporary
  require Logger

  import Elsa.Supervisor, only: [registry: 1]
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")

  @subscribe_delay 200
  @subscribe_retries 20
  @start_failure_delay 5_000

  defmodule State do
    @moduledoc """
    The running state of the worker process.
    """
    defstruct [
      :connection,
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

  @type init_opts :: [
    connection: Elsa.connection(),
    topic: Elsa.topic(),
    partition: Elsa.partition(),
    generation_id: non_neg_integer,
    begin_offset: non_neg_integer,
    handler: module,
    handler_init_args: list,
    config: :brod.consumer_options()
  ]

  @doc """
  Start the worker process and init the state with the given config.
  """
  @spec start_link(init_opts) :: GenServer.on_start()
  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init(init_args) do
    state = %State{
      connection: Keyword.fetch!(init_args, :connection),
      topic: Keyword.fetch!(init_args, :topic),
      partition: Keyword.fetch!(init_args, :partition),
      generation_id: Keyword.get(init_args, :generation_id),
      offset: Keyword.fetch!(init_args, :begin_offset),
      handler: Keyword.fetch!(init_args, :handler),
      handler_init_args: Keyword.get(init_args, :handler_init_args, []),
      config: Keyword.get(init_args, :config, [])
    }

    Process.put(:elsa_connection, state.connection)
    Process.put(:elsa_topic, state.topic)
    Process.put(:elsa_partition, state.partition)
    Process.put(:elsa_generation_id, state.generation_id)

    Elsa.Registry.register_name({registry(state.connection), :"worker_#{state.topic}_#{state.partition}"}, self())

    {:ok, handler_state} = state.handler.init(state.handler_init_args)

    {:ok, %{state | handler_state: handler_state}, {:continue, :subscribe}}
  end

  def handle_continue(:subscribe, state) do
    with {:ok, pid} <- subscribe(state) do
      Process.monitor(pid)
      {:noreply, %{state | subscriber_pid: pid}}
    else
      {:error, reason} ->
        Logger.warn(
          "Unable to subscribe to topic/partition/offset(#{state.topic}/#{state.partition}/#{state.offset}), reason #{
            inspect(reason)
          }"
        )

        Process.sleep(@start_failure_delay)
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
        :ok = Elsa.Consumer.ack(state.connection, topic, partition, offset)
        {:noreply, %{state | handler_state: new_handler_state}}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, _message}, state) do
    {:stop, :brod_consumer_stopped, state}
  end

  def handle_call(:unsubscribe, _from, state) do
    result = :brod.unsubscribe(state.connection, state.topic, state.partition)
    {:stop, :normal, result, state}
  end

  defp transform_messages(topic, partition, messages, state) do
    Enum.map(messages, &Elsa.Message.new(&1, topic: topic, partition: partition, generation_id: state.generation_id))
  end

  defp send_messages_to_handler(messages, state) do
    state.handler.handle_messages(messages, state.handler_state)
  end

  defp ack_messages(topic, partition, offset, %{generation_id: nil} = state) do
    Elsa.Consumer.ack(state.connection, topic, partition, offset)
  end

  defp ack_messages(topic, partition, offset, state) do
    Elsa.Group.Acknowledger.ack(state.connection, topic, partition, state.generation_id, offset)

    offset
  end

  defp subscribe(state, retries \\ @subscribe_retries)

  defp subscribe(_state, 0) do
    {:error, :failed_subscription}
  end

  defp subscribe(state, retries) do
    opts = determine_subscriber_opts(state)

    case Elsa.Consumer.subscribe(state.connection, state.topic, state.partition, opts) do
      {:error, reason} ->
        Logger.warn(
          "Retrying to subscribe to topic #{state.topic} parition #{state.partition} offset #{state.offset} reason #{
            inspect(reason)
          }"
        )

        Process.sleep(@subscribe_delay)
        subscribe(state, retries - 1)

      {:ok, consumer_pid} ->
        Logger.info("Subscribing to topic #{state.topic} partition #{state.partition} offset #{state.offset}")
        {:ok, consumer_pid}
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
