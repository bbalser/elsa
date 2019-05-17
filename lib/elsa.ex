defmodule Elsa do
  @moduledoc """
  Documentation for Elsa.
  """

  defdelegate list_topics(endpoints), to: Elsa.Topic, as: :list

  defdelegate create_topic(endpoints, topic, opts \\ []), to: Elsa.Topic, as: :create

  def delete_topic() do
  end

  def fetch() do
  end

  defmodule ConnectError do
    defexception [:message]
  end
end
