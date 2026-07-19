ThisBuild / organization := "pit"
ThisBuild / version := "0.1.0"

val scala212 = "2.12.18"
val scala213 = "2.13.16"
val sparkV = "3.5.1"
val deltaV = "3.2.0"

// Two profiles, one source tree (serverless port, 2026-07-18):
//   2.12 = classic Spark, local dev + the full test suite (Spark local mode).
//   2.13 = Databricks Connect 17.3 client, the ONLY profile serverless JAR tasks accept
//          (Scala 2.13 / JDK 17 / serverless environment version 4). Spark and Delta APIs
//          come from the connect client; nothing Spark-shaped is bundled in the jar.
// The suite cannot run on the connect profile (no local Spark there) -- 2.12 green is the
// parity gate for code shared by both.
ThisBuild / scalaVersion := scala212

lazy val root = (project in file("."))
  .settings(
    name := "vantage",
    crossScalaVersions := Seq(scala212, scala213),
    // Scalafix OrganizeImports needs unused-import warnings; the flag is per-Scala-version.
    scalacOptions += (if (scalaVersion.value.startsWith("2.13")) "-Wunused:imports"
                      else "-Ywarn-unused-import"),
    libraryDependencies ++= {
      if (scalaVersion.value.startsWith("2.13"))
        Seq(
          "com.databricks" %% "databricks-connect" % "17.3.+" % Provided,
          "com.github.pureconfig" %% "pureconfig" % "0.17.6"
        )
      else
        Seq(
          "org.apache.spark" %% "spark-core" % sparkV % Provided,
          "org.apache.spark" %% "spark-sql" % sparkV % Provided,
          "io.delta" %% "delta-spark" % deltaV,
          "com.github.pureconfig" %% "pureconfig" % "0.17.6",
          "org.scalatest" %% "scalatest" % "3.2.18" % Test,
          // Spark needed at test runtime since main deps are Provided:
          "org.apache.spark" %% "spark-core" % sparkV % Test,
          "org.apache.spark" %% "spark-sql" % sparkV % Test
        )
    },
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
      case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case PathList("org", "apache", "commons", "logging", _ @_*) => MergeStrategy.first
      case "arrow-git.properties" => MergeStrategy.discard
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

// Scalafix/semanticdb rides the 2.12 dev profile only; the pinned semanticdb compiler
// plugin has no artifact for the 2.13 patch version the connect profile uses.
semanticdbEnabled := !scalaVersion.value.startsWith("2.13")
semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"
