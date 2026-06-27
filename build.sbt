ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "pit"
ThisBuild / version      := "0.1.0"

val sparkV = "3.5.1"
val deltaV = "3.2.0"
val deequV = "2.0.7-spark-3.5"

// Scalafix OrganizeImports (removeUnused = true by default) needs the compiler
// to flag unused imports; -Ywarn-unused-import is the 2.12 switch for that.
ThisBuild / scalacOptions += "-Ywarn-unused-import"

lazy val root = (project in file("."))
  .settings(
    name := "vantage",
    libraryDependencies ++= Seq(
      "org.apache.spark"      %% "spark-core"          % sparkV % Provided,
      "org.apache.spark"      %% "spark-sql"           % sparkV % Provided,
      "io.delta"              %% "delta-spark"         % deltaV,
      "com.amazon.deequ"       % "deequ"               % deequV,
      "com.github.pureconfig" %% "pureconfig"          % "0.17.6",
      "org.scalatest"         %% "scalatest"           % "3.2.18" % Test,
      // Spark needed at test runtime since main deps are Provided:
      "org.apache.spark"      %% "spark-core"          % sparkV % Test,
      "org.apache.spark"      %% "spark-sql"           % sparkV % Test
    ),
    // Deequ pulls a Spark transitively; keep ours authoritative.
    dependencyOverrides ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql"  % sparkV
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    ),
    // Merge strategy verified by the toolchain spike (2026-06-26). The naive
    // "discard all META-INF + default deduplicate" FAILS assembly on
    // commons-logging vs jcl-over-slf4j and Arrow's arrow-git.properties, and
    // would also drop Delta's DataSourceRegister (breaking format("delta")).
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case "module-info.class"                      => MergeStrategy.discard
      case PathList("org", "apache", "commons", "logging", _ @ _*) => MergeStrategy.first
      case "arrow-git.properties"                   => MergeStrategy.discard
      case x =>
        val old = (assembly / assemblyMergeStrategy).value
        old(x)
    },
    // Spark provided at runtime on the cluster; exclude from the fat jar.
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter(_.data.getName.startsWith("spark-"))
    }
  )

semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"
