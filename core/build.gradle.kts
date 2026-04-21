plugins {
    `java-library`
    jacoco
    id("info.solidsoft.pitest") version "1.19.0"
    id("org.pastalab.fray.gradle") version "0.8.3"
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
    targetClasses = setOf("org.taotree.*")
    targetTests = setOf("org.taotree.*")
    threads = Runtime.getRuntime().availableProcessors()
    outputFormats = setOf("HTML", "XML")
    timestampedReports = false
    mutators = setOf("STRONGER")

    // Mutation threshold is 70, not 75. Headroom above 70% is concentrated in
    // concurrency paths that are only reachable from Lincheck and Fray tests,
    // which must be excluded from PIT because their lifecycle crashes the
    // minion. Raising the threshold would require either reflection-based
    // unit tests on private concurrency helpers or crafted binary-corruption
    // fixtures for file-recovery paths — both add brittleness without
    // proportional signal. See plan.md Phase 7 coverage notes.
    mutationThreshold = 70  // fail build if mutation score drops below 70%

    // Exclude Preallocator (platform-specific native calls, not meaningfully mutable)
    excludedClasses = setOf(
        "org.taotree.internal.alloc.Preallocator"
    )

    // Exclude Lincheck and Fray stress tests — they test concurrency, not code paths,
    // and their lifecycle crashes the PIT minion.
    excludedTestClasses = setOf("org.taotree.Lincheck*", "org.taotree.fray.*")

    // Pass native access flag to forked test JVMs
    jvmArgs = listOf(
        "--enable-native-access=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED"
    )
}
