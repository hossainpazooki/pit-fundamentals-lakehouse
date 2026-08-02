# Airflow orchestration image for VANTAGE WS1 (gap-closure sprint, 2026-07-22).
# Base: apache/airflow:3.0.2 (pinned per the WS1 contract / spike ground truth).
#
# Layer additions and WHY:
#   - duckdb                          gate_b_audit.py imports duckdb directly
#                                      (its own top-of-file requirement); the
#                                      `delta` duckdb extension auto-installs
#                                      on first use per that script's own
#                                      docstring, so nothing else is needed
#                                      for the audit task to run in-process
#                                      inside this image.
#   - apache-airflow-providers-docker DockerOperator, used by every silver_*,
#                                      gold_rebuild task to run the Spark
#                                      sibling container against the HOST
#                                      docker daemon (via the docker.sock
#                                      mount in docker-compose.yaml -- see
#                                      that file's comments; sock-mount route
#                                      was confirmed viable in the WS1 spike,
#                                      so no fat Spark-in-Airflow image is
#                                      built here).
#   - apache-airflow-providers-fab    Airflow 3.0's default auth manager
#                                      changed away from FAB; docker-compose
#                                      .yaml pins AIRFLOW__CORE__AUTH_MANAGER
#                                      back to FabAuthManager (matches the
#                                      official 3.0.2 reference compose) so
#                                      _AIRFLOW_WWW_USER_USERNAME/PASSWORD and
#                                      `airflow users create` keep working for
#                                      this dev-only stack. This provider
#                                      supplies that auth manager.
#
#   - pyarrow                        the DAG's `negative_control` task runs
#                                      scripts/gate_b_mutate_twin.py IN this
#                                      container (added 2026-07-22 after the
#                                      verify phase refuted the first
#                                      negative-control design: a pre-run
#                                      mutation is healed by gold's pure
#                                      rebuild, so the mutation must happen
#                                      inside the run, between gold_rebuild
#                                      and audit -- see the DAG + evidence
#                                      doc Gate 3). The mutator imports only
#                                      pyarrow.
#
# Deliberately NOT added (per WS1 contract section 3 -- "document the choice,
# do not add speculatively"):
#   - deltalake: nothing in-container imports it; idempotency_diff.py stays
#     on the host route (see README.md).
#   - docker CLI binary: DockerOperator talks to the Engine API directly over
#     the mounted socket, no shell-out needed. Only add it (e.g.
#     `apt-get install -y docker.io` as root, then drop back to the airflow
#     user) if a task ends up shelling out to `docker run` instead of using
#     DockerOperator -- not the case in this build.
FROM apache/airflow:3.0.2

RUN pip install --no-cache-dir \
    duckdb \
    pyarrow \
    apache-airflow-providers-docker \
    apache-airflow-providers-fab
