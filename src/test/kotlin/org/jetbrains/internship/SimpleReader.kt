package org.jetbrains.internship

import java.nio.file.Files
import java.nio.file.Path

internal class SimpleReader(bedPath: Path) {
    private val lines = Files.readAllLines(bedPath)

    fun find(chromosome: String, start: Int, end: Int): List<BedEntry> {
        return lines.asSequence()
            .dropWhile {
                it.startsWith("browser ") || it.startsWith("track ")
            }.filter {
                it.startsWith(chromosome)
            }.map {
                BedReaderImpl.parseEntry(it)
            }.filter {
                it.chromosome == chromosome && it.start >= start && it.end <= end
            }.toList()
    }
}