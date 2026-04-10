plugins {
    java
}

allprojects {
    group = "org.taotree"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion = JavaLanguageVersion.of(26)
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        jvmArgs(
            "--enable-native-access=ALL-UNNAMED",
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens", "java.base/java.lang=ALL-UNNAMED"
        )

        // Fork a fresh JVM per test class so that Lincheck stress tests
        // (which leak Arena instances) don't accumulate across classes.
        forkEvery = 1

        // Use RAM-backed filesystem for file-backed tests when available.
        // macOS: create with `hdiutil attach -nomount ram://524288 | xargs diskutil erasevolume APFS RAMDisk`
        // Linux: /dev/shm is tmpfs by default
        // Override: gradle test -PtestTmpDir=/path/to/ramdisk
        val tmpDir = findProperty("testTmpDir") as String?
            ?: if (file("/Volumes/RAMDisk").isDirectory) "/Volumes/RAMDisk"
            else if (file("/dev/shm").isDirectory) "/dev/shm"
            else null
        if (tmpDir != null) {
            systemProperty("java.io.tmpdir", tmpDir)
        }
    }
}
