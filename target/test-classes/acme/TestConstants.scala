package acme

object TestConstants {

  val KAFKA_BROKERS = "localhost:9092"
  val CLIENT_ID = "kafka-test"
  val INGEST_TOPIC = "mytest2"
  val MESSAGE_COUNT = 4
  val TEST_RECORDS =  Array("{\"topic\":\"sl_ben_svc_ctgry_xrcf_json\",\"partition\":4,\"offset\":29,\"key\":\"1\",\"payload\":\"{\"parentUUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbd\"," +
    "\"UUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbe\",\"type\":\"action\",\"createdAt\":\"2018-09-12T18:19:54.562Z\",\"payload\":\"{\"BEN_SVC_CTGRY_XREF_ID\":\"1\"," +
    "\"BEN_CTGRY_CD\":\"AD\",\"BEN_SVC_SRC_PRODT_ID\":\"29\",\"BEN_SVC_CTGRY_RANK\":\"3\"\"createdAt\":\"1992-09-12 00:00:00\"}}\"}",
    "{\"topic\":\"sl_ben_svc_ctgry_xrcf_json\",\"partition\":4,\"offset\":29,\"key\":\"1\",\"payload\":\"{\"parentUUID\":\"\"," +
      "\"UUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbd\",\"type\":\"action\",\"createdAt\":\"2018-09-12T18:19:54.562Z\",\"payload\":\"{\"BEN_SVC_CTGRY_XREF_ID\":\"1\"," +
      "\"BEN_CTGRY_CD\":\"AD\",\"BEN_SVC_SRC_PRODT_ID\":\"11\",\"BEN_SVC_CTGRY_RANK\":\"4\"\"createdAt\":\"2018-08-12\"}}\"}",
    "{\"topic\":\"sl_ben_svc_ctgry_xrcf_json\",\"partition\":4,\"offset\":29,\"key\":\"2\",\"payload\":\"{\"parentUUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbf\"," +
      "\"UUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbd\",\"type\":\"action\",\"createdAt\":\"2018-09-12T18:19:54.562Z\",\"payload\":\"{\"BEN_SVC_CTGRY_XREF_ID\":\"2\"," +
      "\"BEN_CTGRY_CD\":\"AD\",\"BEN_SVC_SRC_PRODT_ID\":\"31\",\"BEN_SVC_CTGRY_RANK\":\"1\"\"createdAt\":\"2018-09-10 12:00:00\"}}\"}",
    "{\"topic\":\"sl_ben_svc_ctgry_xrcf_json\",\"partition\":4,\"offset\":29,\"key\":\"2\",\"payload\":\"{\"parentUUID\":\"\"," +
      "\"UUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbd\",\"type\":\"action\",\"createdAt\":\"2018-09-12T18:19:54.562Z\",\"payload\":\"{\"BEN_SVC_CTGRY_XREF_ID\":\"2\"," +
      "\"BEN_CTGRY_CD\":\"AD\",\"BEN_SVC_SRC_PRODT_ID\":\"29\",\"BEN_SVC_CTGRY_RANK\":\"4\"\"createdAt\":\"2018-09-09 09:00:00\"}}\"}",
  "{\"topic\":\"sl_ben_svc_ctgry_xrcf_json\",\"partition\":4,\"offset\":29,\"key\":\"3\",\"payload\":\"{\"parentUUID\":\"\"," +
    "\"UUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbd\",\"type\":\"action\",\"createdAt\":\"2018-09-12T18:19:54.562Z\",\"payload\":\"{\"BEN_SVC_CTGRY_XREF_ID\":\"3\"," +
    "\"BEN_CTGRY_CD\":\"AD\",\"BEN_SVC_SRC_PRODT_ID\":\"91\",\"BEN_SVC_CTGRY_RANK\":\"4\"\"createdAt\":\"2018-09-12 11:00:01\"}}\"}",
  "{\"topic\":\"sl_ben_svc_ctgry_xrcf_json\",\"partition\":4,\"offset\":29,\"key\":\"3\",\"payload\":\"{\"parentUUID\":\"\"," +
    "\"UUID\":\"ae0a03df-5ca2-445b-b684-5c1f230b0bbd\",\"type\":\"action\",\"createdAt\":\"2018-09-12T18:19:54.562Z\",\"payload\":\"{\"BEN_SVC_CTGRY_XREF_ID\":\"3\"," +
    "\"BEN_CTGRY_CD\":\"AD\",\"BEN_SVC_SRC_PRODT_ID\":\"33\",\"BEN_SVC_CTGRY_RANK\":\"5\"\"createdAt\":\"2018-09-12 12:00:00\"}}\"}")

}
