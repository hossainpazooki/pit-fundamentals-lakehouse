package pit.observability

final case class RunMetrics(
    batchId: String,
    rowsIn: Long,
    rowsOut: Long,
    rejected: Long,
    quarantined: Long,
    gateOutcome: String,
    deltaVersion: Long
) {
  // ASCII only (Windows cp1252 console safe), keyed by batch id.
  def asLogLine: String =
    s"run batch_id=$batchId rows_in=$rowsIn rows_out=$rowsOut " +
      s"rejected=$rejected quarantined=$quarantined gate=$gateOutcome delta_version=$deltaVersion"
}
