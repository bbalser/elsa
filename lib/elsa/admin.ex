defmodule Elsa.Admin do
  use Rustler, otp_app: :elsa, crate: "elsa_admin"

  def describe_topic(brokers, topic) do
    brokers
    |> Enum.map(fn {host, port} -> "#{to_string(host)}:#{to_string(port)}" end)
    |> hd()
    |> describe_topic_nif(topic)
  end

  defp describe_topic_nif(_brokers, _topic), do: :erlang.nif_error(:nif_not_loaded)

  def add(_a, _b), do: :erlang.nif_error(:nif_not_loaded)
end
