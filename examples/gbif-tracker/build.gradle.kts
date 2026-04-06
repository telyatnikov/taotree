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

tasks.register<JavaExec>("bench") {
    description = "Run GBIF ingestion benchmark (multi-thread comparison)"
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.taotree.examples.gbif.GbifBenchmark"
    jvmArgs = listOf("-Xmx4g", "--enable-native-access=ALL-UNNAMED")
    workingDir = rootProject.projectDir
}
