defmodule Elsa.Util do
  @moduledoc """
  Provides functions for simplifying first-class interactions (consuming and
  producing) such as connecting to a cluster and establishing a persistent
  client process for interacting with a cluster.
  """

  @doc """
  Wrap establishing a connection to a cluster for performing an operation.
  """
  @spec with_connection(keyword(), atom(), fun()) :: term()
  def with_connection(endpoints, type \\ :any, fun) when is_function(fun) do
    endpoints
    |> reformat_endpoints()
    |> connect(type)
    |> do_with_connection(fun)
  end

  @doc """
  Convert supplied cluster endpoints from common keyword list format to
  brod-compatible tuple.
  """
  @spec reformat_endpoints(keyword()) :: [{charlist(), integer()}]
  def reformat_endpoints(endpoints) do
    Enum.map(endpoints, fn {key, value} -> {to_charlist(key), value} end)
  end

  @doc """
  Retrieve the api version of the desired operation supported by the
  connected cluster.
  """
  @spec get_api_version(pid(), atom()) :: integer()
  def get_api_version(connection, api) do
    {:ok, api_versions} = :kpro.get_api_versions(connection)
    {_, version} = Map.get(api_versions, api)
    version
  end

  @doc """
  Create a named client connection process for managing interactions
  with the connected cluster.
  """
  @spec start_client(keyword(), atom()) :: {:ok, pid()} | {:error, term()}
  def start_client(endpoints, name) do
    endpoints
    |> reformat_endpoints()
    |> :brod.start_link_client(name)
    |> case do
      {:ok, client_pid} ->
        {:ok, client_pid}

      {:error, {:already_started, client_pid}} ->
        {:ok, client_pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp connect(endpoints, :controller), do: :kpro.connect_controller(endpoints, [])
  defp connect(endpoints, _type), do: :kpro.connect_any(endpoints, [])

  defp do_with_connection({:ok, connection}, fun) do
    fun.(connection)
  after
    :kpro.close_connection(connection)
  end

  defp do_with_connection({:error, reason}, _fun) do
    raise Elsa.ConnectError, message: format_reason(reason)
  end

  defp format_reason(reason) do
    cond do
      is_binary(reason) -> reason
      Exception.exception?(reason) -> Exception.format(:error, reason)
      true -> inspect(reason)
    end
  end
end
