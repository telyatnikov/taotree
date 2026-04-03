plugins {
    `java-library`
    jacoco
    id("info.solidsoft.pitest") version "1.19.0"
}

description = "TaoTree — Adaptive Radix Tree on Java FFM"

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.jetbrains.lincheck:lincheck:3.5")
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        html.required = true
        xml.required = true
    }
}

pitest {
    junit5PluginVersion = "1.2.1"
    targetClasses = setOf("org.taotree.*", "org.taotree.internal.*")
    targetTests = setOf("org.taotree.*", "org.taotree.internal.*")
    threads = Runtime.getRuntime().availableProcessors()
    outputFormats = setOf("HTML", "XML")
    timestampedReports = false
    mutators = setOf("STRONGER")
    mutationThreshold = 75  // fail build if mutation score drops below 75%

    // Exclude Preallocator (platform-specific native calls, not meaningfully mutable)
    excludedClasses = setOf("org.taotree.internal.Preallocator")

    // Exclude Lincheck stress tests — they test concurrency, not code paths,
    // and their Arena-leaking lifecycle crashes the PIT minion.
    excludedTestClasses = setOf("org.taotree.Lincheck*")

    // Pass native access flag to forked test JVMs
    jvmArgs = listOf(
        "--enable-native-access=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED"
    )
}
