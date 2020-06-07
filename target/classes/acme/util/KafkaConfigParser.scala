package acme.util

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException
import java.nio.file.Paths

import acme.config._


object BusinessTierConfigParser {
  private val MAPPER = new ObjectMapper()

  def parse[C <: KafkaConfig](path: String, clzz: Class[C]): KafkaConfig = {
    try {
      MAPPER.readValue(Paths.get(path).toFile, clzz)
    } catch {
      case e: Exception => throw new Exception(e)
    }
  }
}

