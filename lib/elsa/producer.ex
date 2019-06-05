defmodule Elsa.Producer do
  use GenServer

  def produce_sync(endpoints, topic, partition, key, value) do
    client = Elsa.default_client()

    start_producer(client, endpoints, topic)
    :brod.produce_sync(client, topic, partition, key, value)
  end

  def produce_sync(name, key, value) do
    GenServer.call(name, {:produce_sync, key, value})
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def init(args) do
    brokers = Keyword.get(args, :brokers)
    topic = Keyword.get(args, :topic)
    partition = Keyword.get(args, :partition)
    client = Elsa.default_client()

    start_producer(client, brokers, topic)

    {:ok, _pid} = :brod.get_producer(client, topic, partition)
  end

  def handle_call({:produce_sync, key, value}, _from, pid) do
    pid
    |> :brod.produce_sync(key, value)
    |> reply(pid)
  end

  defp start_producer(client, brokers, topic) do
    brokers
    |> Elsa.Util.reformat_endpoints()
    |> :brod.start_client(client)

    :brod.start_producer(client, topic, [])
  end

  defp reply(:ok, pid), do: {:reply, :ok, pid}
  defp reply(error, pid), do: {:reply, error, pid}
end
