import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

plugins {
    application
}

description = "GBIF Species Observation Tracker — TaoTree temporal example"

dependencies {
    implementation(project(":core"))
    implementation("dev.hardwood:hardwood-core:1.0.0.Beta1")
    implementation("org.xerial.snappy:snappy-java:1.1.10.7")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "org.taotree.examples.gbif.GbifTracker"
    applicationDefaultJvmArgs = listOf("-Xmx2g", "--enable-native-access=ALL-UNNAMED")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

tasks.register<JavaExec>("ingest") {
    description = "Ingest GBIF Parquet files into a TaoTree (Approach B)"
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.taotree.examples.gbif.GbifTracker"
    jvmArgs = listOf("-Xmx4g", "--enable-native-access=ALL-UNNAMED")
    workingDir = rootProject.projectDir
    args("ingest", "data/gbif", "build/gbif/gbif.taotree")
}

tasks.register<JavaExec>("verify") {
    description = "Round-trip verify an ingested GBIF TaoTree against source Parquet"
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.taotree.examples.gbif.GbifTracker"
    jvmArgs = listOf("-Xmx4g", "--enable-native-access=ALL-UNNAMED")
    workingDir = rootProject.projectDir
    args("verify", "data/gbif", "build/gbif/gbif.taotree")
}

val gbifProfilingDir = layout.buildDirectory.dir("reports/profiling")
val gbifJfrLockThreshold = providers.gradleProperty("jfrLockThreshold").orElse("1 ms")
val gbifJfrAllocationProfiling = providers.gradleProperty("jfrAllocationProfiling").orElse("high")

tasks.register<JavaExec>("ingestJfr") {
    description = "Ingest with Java Flight Recorder enabled"
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.taotree.examples.gbif.GbifTracker"
    jvmArgs = listOf("-Xmx4g", "--enable-native-access=ALL-UNNAMED")
    workingDir = rootProject.projectDir
    args("ingest", "data/gbif", "build/gbif/gbif.taotree")

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
