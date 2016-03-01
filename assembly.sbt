import sbtassembly.Plugin.AssemblyKeys._

// put this at the top of the file

assemblySettings

jarName in assembly := "log-island-assembly.jar"

test in assembly := {}

mergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
