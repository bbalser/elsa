defmodule Elsa.TopicTest do
  use ExUnit.Case
  use Placebo

  require Elsa.Topic

  describe "create_topic/3" do
    test "returns error tuple when topic fails to get created" do
      allow Elsa.Util.with_connection(any(), any()), return: :not_sure_yet
      allow Elsa.Util.get_api_version(any(), :create_topics), return: :version
      allow :kpro_req_lib.create_topics(any(), any(), any()), return: :topic_request
      allow :kpro.request_sync(:connection, any(), any()), return: {:error, "some failure"}

      Elsa.create_topic(:endpoints, "topic-to-create")

      function = capture(Elsa.Util.with_connection(:endpoints, any()), 2)

      internal_result = function.(:connection)

      assert {:error, "some failure"} == internal_result
    end

    test "return error tuple when topic response contains an error" do
      allow Elsa.Util.with_connection(any(), any()), return: :not_sure_yet
      allow Elsa.Util.get_api_version(any(), :create_topics), return: :version
      allow :kpro_req_lib.create_topics(any(), any(), any()), return: :topic_request

      message = %{
        topic_errors: [
          %{
            error_code: :topic_already_exists,
            error_message: "Topic 'elsa-topic' already exists.",
            topic: "elsa-topic"
          }
        ]
      }

      kpro_rsp = Elsa.Topic.kpro_rsp(api: :create_topics, vsn: 2, msg: message)
      allow :kpro.request_sync(:connection, any(), any()), return: {:ok, kpro_rsp}

      Elsa.create_topic(:endpoints, "elsa-topic")
      function = capture(Elsa.Util.with_connection(:endpoints, any()), 2)

      internal_result = function.(:connection)

      assert {:error, "Topic 'elsa-topic' already exists."} == internal_result
    end
  end
end
