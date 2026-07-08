package pit.observability

final case class RunMetrics(
    batchId: String,
    rowsIn: Long,
    rowsOut: Long,
    scopedOut: Long,
    rejected: Long,
    quarantined: Long,
    gateOutcome: String,
    deltaVersion: Long,
    wallClockMs: Long = 0L
) {
  // ASCII only (Windows cp1252 console safe), keyed by batch id.
  def asLogLine: String =
    s"run batch_id=$batchId rows_in=$rowsIn rows_out=$rowsOut scoped_out=$scopedOut " +
      s"rejected=$rejected quarantined=$quarantined gate=$gateOutcome delta_version=$deltaVersion " +
      s"wall_clock_ms=$wallClockMs"
}
