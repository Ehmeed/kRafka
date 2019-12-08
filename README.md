# kRafka

*Wrapper for apache kafka consumer in R*

To install, run (requires rJava):
```R
install_github("Ehmeed/kRafka/R")
```

To read topic messages, run:
```R
df = kRafka.read("localhost:9093", "topic_name", type = "datetime", from = "1970-01-01T00:00:00.00Z", to = "2037-01-01T00:00:00.00Z")
```

To list topics, run:
```R
kRafka.listTopics("localhost:9093")
```

Also supports custom deserializers and other consumer configs. See the [source](R/R/kRafka.R).
