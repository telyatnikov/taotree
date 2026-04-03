plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

description = "TaoTree JMH benchmarks"

dependencies {
    jmh(project(":core"))
}

jmh {
    warmupIterations = 3
    iterations = 5
    fork = 1
    jvmArgs = listOf("-Xms512m", "-Xmx512m")
    resultFormat = "JSON"
    resultsFile = rootProject.layout.projectDirectory.file("performance/last-run.json")
}
