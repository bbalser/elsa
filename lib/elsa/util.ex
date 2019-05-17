defmodule Elsa.Util do
  @moduledoc false

  def with_connection(endpoints, fun) when is_function(fun) do
    endpoints
    |> reformat_endpoints()
    |> :kpro.connect_any([])
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
