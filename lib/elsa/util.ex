defmodule Elsa.Util do
  @moduledoc false

  def with_connection(endpoints, type \\ :any, fun) when is_function(fun) do
    endpoints
    |> reformat_endpoints()
    |> connect(type)
    |> do_with_connection(fun)
  end

  def reformat_endpoints(endpoints) do
    Enum.map(endpoints, fn {key, value} -> {to_charlist(key), value} end)
  end

  def get_api_version(connection, api) do
    {:ok, api_versions} = :kpro.get_api_versions(connection)
    {_, version} = Map.get(api_versions, api)
    version
  end

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
