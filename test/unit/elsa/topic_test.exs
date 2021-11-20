defmodule Elsa.TopicTest do
  use ExUnit.Case
  use Placebo

  require Elsa.Topic

  describe "create_topic/3" do
    test "returns error tuple when topic fails to get created" do
      allow Elsa.Util.with_connection(any(), :controller, any()), return: :not_sure_yet
      allow Elsa.Util.get_api_version(any(), :create_topics), return: :version
      allow :kpro_req_lib.create_topics(any(), any(), any()), return: :topic_request
      allow :kpro.request_sync(:connection, any(), any()), return: {:error, "some failure"}

      Elsa.create_topic(:endpoints, "topic-to-create")

      function = capture(Elsa.Util.with_connection(:endpoints, :controller, any()), 3)

      internal_result = function.(:connection)

      assert {:error, "some failure"} == internal_result
    end

    test "return error tuple when topic response contains an error" do
      allow Elsa.Util.with_connection(any(), :controller, any()), return: :not_sure_yet
      allow Elsa.Util.get_api_version(any(), :create_topics), return: :version
      allow :kpro_req_lib.create_topics(any(), any(), any()), return: :topic_request

      message = %{
        topics: [
          %{
            error_code: :topic_already_exists,
            error_message: "Topic 'elsa-topic' already exists.",
            name: "elsa-topic"
          }
        ]
      }

      kpro_rsp = Elsa.Topic.kpro_rsp(api: :create_topics, vsn: 2, msg: message)
      allow :kpro.request_sync(:connection, any(), any()), return: {:ok, kpro_rsp}

      Elsa.create_topic(:endpoints, "elsa-topic")
      function = capture(Elsa.Util.with_connection(:endpoints, :controller, any()), 3)

      internal_result = function.(:connection)

      assert {:error, {:topic_already_exists, "Topic 'elsa-topic' already exists."}} == internal_result
    end
  end

  describe "delete_topic/2" do
    test "return error tuple when topic response contains an error" do
      allow Elsa.Util.with_connection(any(), :controller, any()), return: :not_sure_yet
      allow Elsa.Util.get_api_version(any(), :delete_topics), return: :version
      allow :kpro_req_lib.delete_topics(any(), any(), any()), return: :topic_request

      message = %{
        responses: [
          %{
            error_code: :topic_doesnt_exist,
            name: "elsa-topic"
          }
        ]
      }

      kpro_rsp = Elsa.Topic.kpro_rsp(api: :delete_topics, vsn: 2, msg: message)
      allow :kpro.request_sync(:connection, any(), any()), return: {:ok, kpro_rsp}

      Elsa.delete_topic(:endpoints, "elsa-topic")
      function = capture(Elsa.Util.with_connection(:endpoints, :controller, any()), 3)

      internal_result = function.(:connection)

      assert {:error, {:topic_doesnt_exist, :delete_topic_error}} == internal_result
    end
  end

  describe "list_topics/1" do
    test "extracts topics and partitions as a list of tuples" do
      metadata = %{
        topics: [
          %{
            partitions: [%{partition: 0}],
            name: "elsa-other-topic"
          },
          %{
            partitions: [%{partition: 0}, %{partition: 1}],
            name: "elsa-topic"
          }
        ]
      }

      allow :brod.get_metadata(any(), :all), return: {:ok, metadata}

      assert Elsa.list_topics(localhost: 9092) == {:ok, [{"elsa-other-topic", 1}, {"elsa-topic", 2}]}
    end

    test "returns error tuple if error is thrown from brod" do
      assert {:error, _} = Elsa.list_topics(localhost: 9092)
    end
  end

  describe "exists?/2" do
    test "returns a boolean identifying the presence of a given topic" do
      metadata = %{
        topics: [
          %{
            partitions: [%{partition: 0}],
            name: "elsa-other-topic"
          },
          %{
            partitions: [%{partition: 0}, %{partition: 1}],
            name: "elsa-topic"
          }
        ]
      }

      allow :brod.get_metadata(any(), :all), return: {:ok, metadata}

      assert Elsa.Topic.exists?([localhost: 9092], "elsa-other-topic") == true
      assert Elsa.Topic.exists?([localhost: 9092], "missing-topic") == false
    end
  end
end
