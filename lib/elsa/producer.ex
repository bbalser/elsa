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
    name = get_client(opts)
    client_pid = Process.whereis(name)

    with {:ok, _client} <- Elsa.Util.start_client(endpoints, name),
         :ok <- :brod.start_producer(name, topic, opts),
         :ok <- produce_sync(topic, messages, Keyword.put(opts, :name, name)),
         :brod_client.stop_producer(name, topic) do
      Logger.info("Successfully produced to #{topic}")
      if client_pid == nil, do: :brod.stop_client(name)
      :ok
    else
      error -> {:error, "Failed to produce: #{inspect(error)}"}
    end
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
    do_with_valid_client(opts, fn client ->
      partitioner = get_partitioner(client, topic, opts)
      message_chunks = create_message_chunks(partitioner, messages)

      case produce_sync_while_successful(client, topic, message_chunks) do
        {:ok, _} -> :ok
        {:error, reason, chunks_sent} -> failure_message(message_chunks, reason, chunks_sent)
      end
    end)
  end

  defp do_with_valid_client(opts, function) when is_function(function, 1) do
    case get_valid_client(opts) do
      {:ok, client} -> function.(client)
      error -> error
    end
  end

  defp produce_sync_while_successful(client, topic, message_chunks) do
    Enum.reduce_while(message_chunks, {:ok, 0}, fn {partition, chunk}, {:ok, chunks_sent} ->
      total_size = Enum.reduce(chunk, 0, fn {key, value}, acc -> acc + byte_size(key) + byte_size(value) end)

      Logger.debug(fn ->
        "#{__MODULE__} Sending #{length(chunk)} messages to #{topic}:#{partition} - Size : #{total_size}"
      end)

      case :brod.produce_sync(client, topic, partition, "", chunk) do
        :ok -> {:cont, {:ok, chunks_sent + 1}}
        {:error, reason} -> {:halt, {:error, reason, chunks_sent}}
      end
    end)
  end

  defp create_message_chunks(partitioner, messages) do
    messages
    |> Enum.group_by(partitioner)
    |> Enum.map(fn {partition, messages} -> {partition, Util.chunk_by_byte_size(messages)} end)
    |> Enum.flat_map(fn {partition, chunks} -> Enum.map(chunks, fn chunk -> {partition, chunk} end) end)
  end

  defp failure_message(message_chunks, reason, chunks_sent) do
    messages_sent = Enum.take(message_chunks, chunks_sent) |> Enum.flat_map(fn {_partition, chunk} -> chunk end)

    reason_string =
      "#{length(messages_sent)} messages succeeded before elsa producer failed midway through due to #{inspect(reason)}"

    failed_messages = Enum.drop(message_chunks, chunks_sent) |> Enum.flat_map(fn {_partition, chunk} -> chunk end)
    {:error, reason_string, failed_messages}
  end

  defp get_client(opts) do
    Keyword.get_lazy(opts, :name, &Elsa.default_client/0)
  end

  defp get_valid_client(opts) do
    client = get_client(opts)

    case Util.client?(client) do
      true -> {:ok, client}
      false -> {:error, :client_down}
    end
  end

  defp get_partitioner(client, topic, opts) do
    case Keyword.get(opts, :partition) do
      nil ->
        {:ok, partition_num} = :brod.get_partitions_count(client, topic)
        partitioner = Keyword.get(opts, :partitioner, :default)
        fn {key, _value} -> Elsa.Producer.Partitioner.partition(partitioner, partition_num, key) end

      partition ->
        fn _msg -> partition end
    end
  end
end
