defmodule Elsa.Consumer.MessageHandler do
  @callback init(term()) :: {:ok, term()}
  @callback handle_messages(term(), term()) :: {:ack, term()} | {:no_ack, term()}
  @callback handle_messages(term()) :: :ack | :no_ack

  defmacro __using__(_opts) do
    quote do
      @behaviour Elsa.Consumer.MessageHandler

      def init(args) do
        {:ok, args}
      end

      def handle_messages(messages, state) do
        handle_messages(messages)
        {:ack, state}
      end

      def handle_messages(messages) do
        :ack
      end

      defoverridable Elsa.Consumer.MessageHandler
    end
  end
end
