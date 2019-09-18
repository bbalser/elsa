defmodule Elsa.Producer.Manager do
  @moduledoc """
  Define a to create and monitor producer processes
  for a given topic, one per topic partition. The
  manager will attempt to start a named client if one
  doesn't already exist. Once the client is started,
  The producer manager starts and monitors producers for
  the topic, ensuring they can be supervised in the event
  the client crashes and restarts.

  Because the brod client process takes ownership of
  producers, individual producer processes that exit
  abnormally are restarted by the client. However, the
  client doesn't keep track of its managed producers in the
  event of an exit. The manager takes this responsibility,
  restarting re-registering the producers to the desired
  client.
  """
  use GenServer
  require Logger

  @failure_delay 5_000
  @producer_retries 10
  @producer_retry_delay 200

  @spec start_link(name: atom(), endpoints: keyword(), topic: String.t(), config: keyword()) :: {:ok, pid()}
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    manager_name = manager_name(name)

    GenServer.start_link(__MODULE__, opts, name: manager_name)
  end

  @doc """
  Reads the configuration for the client and producers
  from the arguments and starts the requested producer processes.

  When starting the producers, the client is started and a
  the manager reduces over the list of topic partitions,
  starting and monitoring each one.
  """
  @impl GenServer
  def init(opts) do
    state = %{
      name: Keyword.fetch!(opts, :name),
      endpoints: Keyword.fetch!(opts, :endpoints),
      topic: Keyword.fetch!(opts, :topic),
      config: Keyword.get(opts, :config, [])
    }

    case start_producer(state) do
      :ok -> {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end

  defp start_producer(state) do
    with :ok <- Elsa.Util.start_client(state.endpoints, state.name, state.config),
         :ok <- :brod.start_producer(state.name, state.topic, state.config),
         {:ok, num_partitions} <- :brod.get_partitions_count(state.name, state.topic),
         :ok <- monitor_producers(state.name, state.topic, num_partitions) do
      :ok
    else
      {:error, reason} ->
        Logger.warn("Unable to start producers for topic(#{state.topic}, reason #{inspect(reason)})")
        Process.sleep(@failure_delay)
        {:error, reason}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, _, _, reason}, state) do
    {:stop, reason, state}
  end

  defp monitor_producers(name, topic, num_partitions) do
    Enum.reduce(0..(num_partitions - 1), :ok, fn partition, acc ->
      case acc do
        :ok -> monitor_producer(name, topic, partition)
        error -> error
      end
    end)
  end

  defp monitor_producer(name, topic, partition, retries \\ @producer_retries)

  defp monitor_producer(name, topic, partition, 1) do
    case :brod.get_producer(name, topic, partition) do
      {:ok, pid} ->
        Process.monitor(pid)
        :ok

      error ->
        error
    end
  end

  defp monitor_producer(name, topic, partition, retries) do
    case :brod.get_producer(name, topic, partition) do
      {:ok, pid} ->
        Process.monitor(pid)
        :ok

      _error ->
        Process.sleep(@producer_retry_delay)
        monitor_producer(name, topic, partition, retries - 1)
    end
  end

  defp manager_name(name), do: :"elsa_producer_manager_#{name}"
end
