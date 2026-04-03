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
