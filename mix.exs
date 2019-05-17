defmodule Elsa.MixProject do
  use Mix.Project

  def project do
    [
      app: :elsa,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:brod, "~> 3.7"},
      {:divo, "~> 1.1", only: [:dev, :test, :integration], override: true},
      {:divo_kafka, "~> 0.1.0", organization: "smartcolumbus_os", only: [:dev, :test, :integration]},
      {:placebo, "~> 1.2", only: [:dev, :test]},
      {:checkov, "~> 0.4.0", only: [:test]}
    ]
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]
end
