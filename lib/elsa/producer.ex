defmodule Elsa.Producer do
  require Logger

  @moduledoc """
  Defines functions to write messages to topics based on either a list of endpoints or a named client.
  All produce functions support the following options:
    * An existing named client process to handle the request can be specified by the keyword option `connection:`.
    * If no partition is supplied, the first (zero) partition is chosen.
    * Value may be a single message or a list of messages.
    * If a list of messages is supplied as the value, the key is defaulted to an empty string binary.
    * Partition can be specified by the keyword option `partition:` and an integer corresponding to a specific
      partition, or the keyword option `partitioner:` and the atoms `:md5` or `:random`. The atoms
      correspond to partitioner functions that will uniformely select a random partition
      from the total available topic partitions or assign an integer based on an md5 hash of the messages.
  """

  @typedoc """
  Elsa messages can take a number of different forms, including a single binary, a key/value tuple, a map
  including `:key` and `:value` keys, or a list of iolists. Because Elsa supports both single messages and
  lists of messages and because an iolist is indistinguishable from a list of other message types from the
  perspective of the compiler, even single-message iolists must be wrapped in an additional list in order to
  be produced. Internally, all messages are converted to a map before being encoded and produced.
  """
  @type message :: {iodata(), iodata()} | binary() | %{key: iodata(), value: iodata()}

  alias Elsa.Util

  @doc """
  Write the supplied message(s) to the desired topic/partition via an endpoint list and optional named client.
  If no client is supplied, the default named client is chosen.
  """
  @spec produce(
          Elsa.endpoints() | Elsa.connection(),
          Elsa.topic(),
          message() | [message()] | [iolist()],
          keyword()
        ) :: :ok | {:error, term} | {:error, String.t(), [Elsa.Message.elsa_message()]}
  def produce(endpoints_or_connection, topic, messages, opts \\ [])

  def produce(endpoints, topic, messages, opts) when is_list(endpoints) do
    connection = Keyword.get_lazy(opts, :connection, &Elsa.default_client/0)
    registry = Elsa.Supervisor.registry(connection)

    case Process.whereis(registry) do
      nil ->
        ad_hoc_produce(endpoints, connection, topic, messages, opts)

      _pid ->
        produce(connection, topic, messages, opts)
    end

    :ok
  end

  def produce(connection, topic, messages, opts) when is_atom(connection) and is_list(messages) do
    transformed_messages = Enum.map(messages, &transform_message/1)
    do_produce_sync(connection, topic, transformed_messages, opts)
  end

  def produce(connection, topic, message, opts) when is_atom(connection) do
    do_produce_sync(connection, topic, [transform_message(message)], opts)
  end

  def ready?(connection) do
    registry = Elsa.Supervisor.registry(connection)
    via = Elsa.Supervisor.via_name(registry, :producer_process_manager)
    Elsa.DynamicProcessManager.ready?(via)
  end

  defp ad_hoc_produce(endpoints, connection, topic, messages, opts) do
    with {:ok, pid} <-
           Elsa.Supervisor.start_link(endpoints: endpoints, connection: connection, producer: [topic: topic]) do
      ready?(connection)
      produce(connection, topic, messages, opts)
      Process.unlink(pid)
      Supervisor.stop(pid)
    end
  end

  defp transform_message(%{key: _key, value: _value} = msg) do
    msg
    |> Map.update!(:key, &IO.iodata_to_binary/1)
    |> Map.update!(:value, &IO.iodata_to_binary/1)
  end

  defp transform_message({key, value}), do: %{key: IO.iodata_to_binary(key), value: IO.iodata_to_binary(value)}
  defp transform_message(message), do: %{key: "", value: IO.iodata_to_binary(message)}

  defp do_produce_sync(connection, topic, messages, opts) do
    Elsa.Util.with_registry(connection, fn registry ->
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
      Logger.debug(fn ->
        "#{__MODULE__} Sending #{length(chunk)} messages to #{topic}:#{partition}"
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
          partitioner = Keyword.get(opts, :partitioner, Elsa.Partitioner.Default) |> remap_deprecated()
          {:ok, fn %{key: key} -> partitioner.partition(partition_num, key) end}

        partition ->
          {:ok, fn _msg -> partition end}
      end
    end)
  end

  @partitioners %{default: Elsa.Partitioner.Default, md5: Elsa.Partitioner.Md5, random: Elsa.Partitioner.Random}

  defp remap_deprecated(key) when key in [:default, :md5, :random] do
    mod = Map.get(@partitioners, key)
    Logger.warn(fn -> ":#{key} partitioner is deprecated. Use #{mod} instead." end)
    mod
  end

  defp remap_deprecated(key), do: key

  defp brod_produce(registry, topic, partition, messages) do
    producer = :"producer_#{topic}_#{partition}"

    case Elsa.Registry.whereis_name({registry, producer}) do
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
