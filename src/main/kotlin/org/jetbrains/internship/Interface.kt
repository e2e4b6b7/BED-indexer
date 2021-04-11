package org.jetbrains.internship

import java.nio.file.Path

data class BedEntry(val chromosome: String, val start: Int, val end: Int, val other: List<Any>)

interface BedIndex

interface BedReader {

    /**
     * Creates index for [bedPath] and saves it to [indexPath]
     */
    fun createIndex(bedPath: Path, indexPath: Path)

    /**
     * Loads [BedIndex] instance from file [indexPath]
     */
    fun loadIndex(indexPath: Path): BedIndex

    /**
     * Loads list of [BedEntry] from file [bedPath] using [index].
     * All the loaded entries should be located on the given [chromosome],
     * and be inside the range from [start] inclusive to [end] exclusive.
     * E.g. entry [1, 2) is inside [0, 2), but not inside [0, 1).
     */
    fun findWithIndex(
        index: BedIndex, bedPath: Path,
        chromosome: String, start: Int, end: Int
    ): List<BedEntry>

}