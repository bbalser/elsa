defmodule Elsa.Consumer.MessageHandler do
  @moduledoc """
  Define the behaviour and default implementations of functions
  for creating message handlers that will be called by Elsa worker
  processes.
  """
  @callback init(term()) :: {:ok, term()}
  @callback handle_messages(term(), term()) :: {:ack, term()} | {:ack, term(), term()} | {:no_ack, term()}
  @callback handle_messages(term()) :: :ack | {:ack, term()} | :no_ack

  @doc """
  Defines the macro for implementing the message handler behaviour
  in an application. Default implementations allow injecting of
  configuration into the worker process, persisting state between runs
  of the message handler function, or alternatively, basic processing
  and acknowlegement of messages.
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour Elsa.Consumer.MessageHandler

      def init(args) do
        {:ok, args}
      end

      def handle_messages(messages, state) do
        case handle_messages(messages) do
          :ack -> {:ack, state}
          {:ack, offset} -> {:ack, offset, state}
          :no_ack -> {:no_ack, state}
        end
      end

      def handle_messages(messages) do
        :ack
      end

      def topic() do
        Process.get(:elsa_topic)
      end

      def partition() do
        Process.get(:elsa_partition)
      end

      def generation_id() do
        Process.get(:elsa_generation_id)
      end

      def name() do
        Process.get(:elsa_name)
      end

      defoverridable Elsa.Consumer.MessageHandler
    end
  end
end
