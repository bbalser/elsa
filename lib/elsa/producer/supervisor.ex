defmodule Elsa.Producer.Supervisor do
  @moduledoc """
  Defines functions for starting persistent kafka
  producer processes that can be managed by a consuming
  application supervisor.
  """

  def start_producer(endpoints, topic, config \\ []) when is_list(endpoints) do
    name = Keyword.get(config, :name, Elsa.default_client())

    case Elsa.Util.start_client(endpoints, name) do
      {:ok, client_pid} ->
        :brod.start_producer(name, topic, config)
        {:ok, client_pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def stop_producer(client \\ Elsa.default_client(), topic), do: :brod_client.stop_producer(client, topic)
end
