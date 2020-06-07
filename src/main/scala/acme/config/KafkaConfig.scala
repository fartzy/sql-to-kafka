package acme.config

import java.util
import com.fasterxml.jackson.annotation.JsonProperty


abstract class KafkaConfig {

  @JsonProperty("groupBySteps")
  val groupBySteps: GroupByConfig

}
