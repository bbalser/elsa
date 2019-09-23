defmodule Elsa.Producer do
  require Logger

  @moduledoc """
  Defines functions to write messages to topics based on either a list of endpoints or a named client.
  All produce functions support the following options:
    * An existing named client process to handle the request can be specified by the keyword option `name:`.
    * If no partition is supplied, the first (zero) partition is chosen.
    * Value may be a single message or a list of messages.
    * If a list of messages is supplied as the value, the key is defaulted to an empty string binary.
    * If message value is a list, it is expected to be a list of key/value tuples.
    * Partition can be specified by the keyword option `partition:` and an integer corresponding to a specific
      partition, or the keyword option `partitioner:` and the atoms `:md5` or `:random`. The atoms
      correspond to partitioner functions that will uniformely select a random partition
      from the total available topic partitions or assign an integer based on an md5 hash of the messages.
  """

  alias Elsa.Util

  @type hostname :: atom() | String.t()
  @type portnum :: pos_integer()
  @type endpoints :: [{hostname(), portnum()}]
  @type topic :: String.t()

  @doc """
  Write the supplied message(s) to the desired topic/partition via an endpoint list and optional named client
  as a one-off operation. Following the completion of the operation, the producer is stopped.
  If no client is supplied, the default named client is chosen.
  """
  @spec produce(endpoints(), topic(), {term(), term()} | term() | [{term(), term()}] | [term()], keyword()) :: :ok
  def produce(endpoints, topic, messages, opts \\ []) when is_list(endpoints) do
    name = Keyword.get_lazy(opts, :name, &Elsa.default_client/0)
    supervisor = Elsa.Supervisor.supervisor(name)
    case Process.whereis(supervisor) do
      nil ->
        {:ok, pid} = Elsa.Supervisor.start_link(endpoints: endpoints, name: name, producer: [topic: topic])
        produce_sync(topic, messages, opts)
        Process.unlink(pid)
        Supervisor.stop(pid)

      _pid ->
        produce_sync(topic, messages, opts)
    end

    :ok
  end

  @doc """
  Write the supplied messages to the desired topic/partition via a named client specified as a keyword
  option (via the `name:` key). Messages may be a single message or a list of messages.
  """
  @spec produce_sync(topic(), {term(), term()} | term() | [{term(), term()}] | [term()], keyword()) :: :ok
  def produce_sync(topic, messages, opts \\ [])

  def produce_sync(topic, messages, opts) when is_list(messages) do
    transformed_messages = Enum.map(messages, &transform_message/1)
    do_produce_sync(topic, transformed_messages, opts)
  end

  def produce_sync(topic, message, opts) do
    do_produce_sync(topic, [transform_message(message)], opts)
  end

  defp transform_message({key, value}), do: {key, value}
  defp transform_message(message), do: {"", message}

  defp do_produce_sync(topic, messages, opts) do
    name = Keyword.get_lazy(opts, :name, &Elsa.default_client/0)

    Elsa.Util.with_registry(name, fn registry ->
      with {:ok, partitioner} <- get_partitioner(registry, topic, opts),
           message_chunks <- create_message_chunks(partitioner, messages),
           {:ok, _} <- produce_sync_while_successful(registry, topic, message_chunks) do
          :ok
      else
        {:error, reason, messages_sent, failed_messages} -> failure_message(reason, messages_sent, failed_messages)
        error_result -> error_result
      end
    end)
  end

  defp produce_sync_while_successful(registry, topic, message_chunks) do
    Enum.reduce_while(message_chunks, {:ok, 0}, fn {partition, chunk}, {:ok, messages_sent} ->
      total_size = Enum.reduce(chunk, 0, fn {key, value}, acc -> acc + byte_size(key) + byte_size(value) end)

      Logger.debug(fn ->
        "#{__MODULE__} Sending #{length(chunk)} messages to #{topic}:#{partition} - Size : #{total_size}"
      end)

      case brod_produce(registry, topic, partition, chunk) do
        :ok ->
          {:cont, {:ok, messages_sent + length(chunk)}}
        {:error, reason} ->
          failed_messages =
             Enum.flat_map(message_chunks, fn {_partition, chunk} -> chunk end)
             |> Enum.drop(messages_sent)
          {:halt, {:error, reason, messages_sent, failed_messages}}
      end
    end)
  end

  defp create_message_chunks(partitioner, messages) do
    messages
    |> Enum.group_by(partitioner)
    |> Enum.map(fn {partition, messages} -> {partition, Util.chunk_by_byte_size(messages)} end)
    |> Enum.flat_map(fn {partition, chunks} -> Enum.map(chunks, fn chunk -> {partition, chunk} end) end)
  end

  defp failure_message(reason, messages_sent, failed_messages) do
    reason_string =
      "#{messages_sent} messages succeeded before elsa producer failed midway through due to #{inspect(reason)}"

    {:error, reason_string, failed_messages}
  end

  defp get_partitioner(registry, topic, opts) do
    Elsa.Util.with_client(registry, fn client ->
      case Keyword.get(opts, :partition) do
        nil ->
          {:ok, partition_num} = :brod_client.get_partitions_count(client, topic)
          partitioner = Keyword.get(opts, :partitioner, :default)
          {:ok, fn {key, _value} -> Elsa.Producer.Partitioner.partition(partitioner, partition_num, key) end}

        partition ->
          {:ok, fn _msg -> partition end}
      end
    end)
  end

  defp brod_produce(registry, topic, partition, messages) do
    producer = :"producer_#{topic}_#{partition}"
    case Elsa.Registry.whereis_name({registry, producer})  do
      :undefined -> {:error, "Elsa Producer for #{topic}:#{partition} not found"}
      pid -> call_brod_producer(pid, messages)
    end
  end

  defp call_brod_producer(pid, messages) do
    with {:ok, call_ref} <- :brod_producer.produce(pid, "", messages),
         {:ok, _partition} <- :brod_producer.sync_produce_request(call_ref, :infinity) do
      :ok
    end
  end
end
