use Mix.Config

config :elsa,
  divo: [
    {DivoKafka, [create_topics: "elsa-topic:1:1", outside_host: "localhost"]}
  ],
  divo_wait: [dwell: 700, max_tries: 50]
