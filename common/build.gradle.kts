/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("buildlogic.java-library-conventions")
    id("io.freefair.lombok") version "8.14"
    `maven-publish`
}

dependencies {
    implementation(libs.slf4j.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.inline)
    testImplementation(libs.mockito.junit.jupiter)

    testRuntimeOnly(libs.junit.jupiter.launcher)
}

tasks.withType<JavaCompile>().configureEach {
}
