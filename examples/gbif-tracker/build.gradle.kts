import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

plugins {
    application
}

description = "GBIF Species Observation Tracker — TaoTree example"

dependencies {
    implementation(project(":core"))
    implementation("dev.hardwood:hardwood-core:1.0.0.Beta1")
    implementation("org.xerial.snappy:snappy-java:1.1.10.7")
}

application {
    mainClass = "org.taotree.examples.gbif.GbifTracker"
    applicationDefaultJvmArgs = listOf("-Xmx2g")
}

val gbifBenchmarkMainClass = "org.taotree.examples.gbif.GbifBenchmark"
val gbifBenchmarkJvmArgs = listOf("-Xmx4g", "--enable-native-access=ALL-UNNAMED")
val gbifProfilingDir = layout.buildDirectory.dir("reports/profiling")
val gbifJfrLockThreshold = providers.gradleProperty("jfrLockThreshold").orElse("1 ms")
val gbifJfrAllocationProfiling = providers.gradleProperty("jfrAllocationProfiling").orElse("high")

fun JavaExec.configureGbifBenchmark() {
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = gbifBenchmarkMainClass
    jvmArgs = gbifBenchmarkJvmArgs
    workingDir = rootProject.projectDir
}

tasks.register<JavaExec>("bench") {
    description = "Run GBIF ingestion benchmark (multi-thread comparison)"
    configureGbifBenchmark()
}

tasks.register<JavaExec>("benchJfr") {
    description = "Run GBIF ingestion benchmark with Java Flight Recorder"
    configureGbifBenchmark()

    doFirst {
        val profilingDir = gbifProfilingDir.get().asFile
        profilingDir.mkdirs()

        val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now())
        val settingsTemplate = file("${System.getProperty("java.home")}/lib/jfr/profile.jfc")
        check(settingsTemplate.isFile) { "JFR profile settings not found: ${settingsTemplate.absolutePath}" }

        val settingsFile = profilingDir.resolve("gbif-bench-$timestamp.jfc")
        val recordingFile = profilingDir.resolve("gbif-bench-$timestamp.jfr")
        val settingsText = settingsTemplate.readText()
            .replace(
                Regex("""(<selection name="allocation-profiling" default=")[^"]+(" label="Allocation Profiling">)"""),
                "$1${gbifJfrAllocationProfiling.get()}$2"
            )
            .replace(
                Regex("""(<text name="locking-threshold" label="Locking Threshold" contentType="timespan" minimum="0 s">)[^<]+(</text>)"""),
                "$1${gbifJfrLockThreshold.get()}$2"
            )
        settingsFile.writeText(settingsText)

        jvmArgs(
            "-XX:StartFlightRecording=" +
                "name=gbif-bench," +
                "settings=${settingsFile.absolutePath}," +
                "dumponexit=true," +
                "filename=${recordingFile.absolutePath}"
        )

        println("JFR settings: ${settingsFile.relativeTo(rootProject.projectDir)}")
        println("JFR recording: ${recordingFile.relativeTo(rootProject.projectDir)}")
    }
}
