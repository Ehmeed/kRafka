\name{kRafka}
\title{kRafka}
\usage{
df = kRafka.read("localhost:9093", "topic_name")
}
\description{
Reads Kafka messages from given topic.
Optional specify desired time range, timeout, maximum messages fetched, groupId, deserializers and other consumer configs.
}
\examples{
df = kRafka.read("localhost:9093", "topic_name")
df = kRafka.read("localhost:9093", "topic_name", type = "datetime", from = "1970-01-01T00:00:00.00Z", to = "2037-01-01T00:00:00.00Z")
}
