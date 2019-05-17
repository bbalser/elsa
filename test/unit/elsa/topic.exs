defmodule Elsa.TopicTest do
  use ExUnit.Case
  use Placebo

  describe "create_topic/3" do
    test "when topic failes to get created" do
      allow Elsa.Util.with_connection(any(), any()), return: :not_sure_yet
      allow Elsa.Util.get_api_version(any(), :create_topics), return: :version
      allow :kpro_req_lib.create_topics(any(), any(), any()), return: :topic_request
      allow :kpro.request_sync(:connection, any(), any()), return: {:error,  "some failure"}

      Elsa.create_topic(:endpoints, "topic-to-create")

      function = capture(Elsa.Util.with_connection(:endpoints, any()), 2)

      internal_result = function.(:connection)

      assert {:error, "some failure"} == internal_result
    end
  end
end
