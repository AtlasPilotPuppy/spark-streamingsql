/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._

import com.typesafe.sbt.SbtScalariform._
import org.scalastyle.sbt.ScalastylePlugin
import scalariform.formatter.preferences._

object Properties {
  val SPARK_VERSION = "1.3.0-SNAPSHOT"
}

object StreamSQLBuild extends Build {

  import Dependencies._

  lazy val sparkExtension = Project(id = "spark-extension", base = file("spark-extension"),
    settings = commonSettings ++ Seq(
      description := "Spark extensions to support StreamSQL",
      libraryDependencies ++= sparkDeps ++ testDeps
    )
  )

  lazy val core = Project(id = "core", base = file("core"),
    settings = commonSettings ++ Seq(
        description := "Spark StreamSQL core module",
        libraryDependencies ++= sparkDeps ++ testDeps
      )
  ) dependsOn(sparkExtension)

  lazy val example = Project(id = "example", base = file("example"),
    settings  = commonSettings ++ Seq(
      description := "Spark StreamSQL example module",
      libraryDependencies ++= sparkDeps
    )
  ) dependsOn(core)

  lazy val tool = Project(id = "tool", base = file("tool"),
    settings = commonSettings ++ Seq(
      description := "Spark StreamSQL tools module",
      libraryDependencies ++= sparkDeps
    )
  ) dependsOn(core)

  lazy val root = Project(id = "spark-streamsql", base = file("."),
    settings = commonSettings ++ Seq(
      parallelExecution in Test := false)
  ) aggregate (core, sparkExtension, example, tool)

  lazy val runScalaStyle = taskKey[Unit]("testScalaStyle")

  // rat task need to be added later.
  lazy val runRat = taskKey[Unit]("run-rat-task")
  lazy val runRatTask = runRat:= {
    "bin/run-rat.sh" !
  }

  lazy val commonSettings = Defaults.defaultSettings ++ Seq(
    organization := "spark.streamsql",
    version      := "0.1.0-SNAPSHOT",
    crossPaths   := false,
    scalaVersion := "2.10.4",
    scalaBinaryVersion := "2.10",
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",

    runScalaStyle := {
      org.scalastyle.sbt.PluginKeys.scalastyle.toTask("").value
    },

    (compile in Compile) <<= (compile in Compile) dependsOn runScalaStyle,

    scalacOptions := Seq("-deprecation", "-feature",
                         "-language:implicitConversions", "-language:postfixOps"),
    resolvers ++= Dependencies.repos,
    parallelExecution in Test := false
  ) ++ scalariformPrefs ++ ScalastylePlugin.Settings

  lazy val scalariformPrefs = defaultScalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(PreserveDanglingCloseParenthesis, false)
  )
}
