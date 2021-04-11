package org.jetbrains.internship

import java.io.*
import java.nio.file.Path
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern


object BedReaderImpl : BedReader {
    private const val maxBuffer: Long = 4 * 1024 * 1024
    private val rowsDivider: Pattern = Pattern.compile("( +)|\t")

    internal fun parseEntry(line: String): BedEntry {
        val items = line.split(rowsDivider)
        return BedEntry(items[0], items[1].toInt(), items[2].toInt(), items.subList(3, items.size))
    }

    /**
     * Parse file to sequence of lines with their ranges
     */
    private fun parseFile(channel: FileChannel): Sequence<FileLine> {
        val fileSize = channel.size()
        var bufferStart: Long = 0
        var bufferLength: Long = minOf(maxBuffer, fileSize - bufferStart)
        var currentLineStart = 0
        var bufferWindow = channel.map(FileChannel.MapMode.READ_ONLY, bufferStart, bufferLength)
        return generateSequence {
            var currentLineEnd = currentLineStart + 1
            while (currentLineEnd < bufferLength && bufferWindow[currentLineEnd] != '\n'.toByte()) {
                currentLineEnd++
            }

            if (currentLineEnd >= bufferLength.toInt()) {
                bufferStart += currentLineStart
                bufferLength = minOf(maxBuffer, fileSize - bufferStart)
                currentLineEnd -= currentLineStart
                currentLineStart = 0
                bufferWindow = channel.map(FileChannel.MapMode.READ_ONLY, bufferStart, bufferLength)

                while (currentLineEnd < bufferLength && bufferWindow[currentLineEnd] != '\n'.toByte()) {
                    currentLineEnd++
                }
                if (currentLineEnd >= bufferLength.toInt()) {
                    return@generateSequence null
                }
            }

            val next = FileLine(
                StandardCharsets.UTF_8.decode(
                    bufferWindow.slice(currentLineStart, currentLineEnd - currentLineStart)
                ).toString(),
                FileRange(
                    bufferStart + currentLineStart,
                    currentLineEnd - currentLineStart
                )
            )
            currentLineStart = currentLineEnd + 1
            return@generateSequence next
        }
    }

    /**
     * @param channel FileChannel to read from
     * @param ranges Sorted sequence of file ranges
     * @return Sequence of strings from given ranges
     */
    private fun getRanges(channel: FileChannel, ranges: Sequence<FileRange>): Sequence<String> {
        val fileSize = channel.size()
        var bufferStart: Long = 0
        var bufferWindow =
            channel.map(FileChannel.MapMode.READ_ONLY, bufferStart, minOf(maxBuffer, fileSize))

        return ranges
            .map { filePosition ->
                if (filePosition.offset + filePosition.length > bufferStart + maxBuffer) {
                    bufferStart = filePosition.offset
                    bufferWindow = channel.map(
                        FileChannel.MapMode.READ_ONLY,
                        bufferStart,
                        minOf(maxBuffer, fileSize - bufferStart)
                    )
                }

                bufferWindow.slice(
                    (filePosition.offset - bufferStart).toInt(),
                    filePosition.length
                )
            }.map {
                StandardCharsets.UTF_8.decode(it).toString()
            }
    }

    /**
     * Parse BED file.
     *
     * @return Lists of lines sorted by position in the file and grouped by chromosome
     */
    private fun parseBed(bedPath: Path): Map<String, List<ChromosomeRange>> {
        return FileInputStream(bedPath.toFile()).channel.use { channel ->
            parseFile(channel)
                .dropWhile {
                    it.line.startsWith("browser ") || it.line.startsWith("track ")
                }.map {
                    val data = it.line.split(rowsDivider, limit = 4)
                    Pair(data[0], ChromosomeRange(data[1].toInt(), data[2].toInt(), it.range))
                }.groupBy({ it.first }, { it.second })
        }
    }

    /**
     * Creates index for [bedPath] and saves it to [indexPath]
     */
    override fun createIndex(bedPath: Path, indexPath: Path) {
        val managers = parseBed(bedPath)
        DataOutputStream(indexPath.toFile().outputStream().buffered()).use { writer ->
            managers.entries.forEach { manager ->
                val chromosomeName = manager.key.encodeToByteArray()
                writer.writeInt(chromosomeName.size)
                writer.write(chromosomeName)
                PositionManager.writeIndex(manager.value, writer)
            }
        }
    }

    /**
     * Loads [BedIndex] instance from file [indexPath]
     */
    override fun loadIndex(indexPath: Path): BedIndex {
        val managers: HashMap<String, PositionManager> = HashMap()
        DataInputStream(indexPath.toFile().readBytes().inputStream()).use { reader ->
            while (reader.available() > 0) {
                val length = reader.readInt()
                val chromosome = reader.readNBytes(length).decodeToString()
                managers[chromosome] = PositionManager.parseIndex(reader)
            }

        }
        return BedIndexImpl(managers)
    }

    /**
     * Loads list of [BedEntry] from file [bedPath] using [index].
     * All the loaded entries located on the given [chromosome],
     * and inside the range from [start] inclusive to [end] exclusive.
     */
    override fun findWithIndex(
        index: BedIndex, bedPath: Path,
        chromosome: String, start: Int, end: Int,
    ): List<BedEntry> {
        if (index is BedIndexImpl) {
            val manager = index.managers[chromosome]
            return if (manager != null) {
                FileInputStream(bedPath.toFile()).channel.use { channel ->
                    getRanges(channel, manager.getPointers(start, end))
                        .map {
                            parseEntry(it)
                        }.toList()
                }
            } else {
                emptyList()
            }
        } else {
            throw IllegalArgumentException("Bad index")
        }
    }


    private data class BedIndexImpl(val managers: Map<String, PositionManager>) : BedIndex

    private data class FileRange(val offset: Long, val length: Int)

    private data class FileLine(val line: String, val range: FileRange)

    private data class ChromosomeRange(val chromosomeBegin: Int, val chromosomeEnd: Int, val fileRange: FileRange)

    private sealed class PositionManager {
        companion object {
            /**
             * Select position manager for chromosome ranges.
             */
            fun writeIndex(elements: List<ChromosomeRange>, writer: DataOutput) {
                // Here should be selection between array manager for small lists and tree manager for big
                // But in my implementation bottleneck is not here yet
                writer.writeByte(1)
                ArrayManager.writeIndex(elements, writer)
            }

            /**
             * Delegate parsing to exact position manager.
             */
            fun parseIndex(reader: DataInput): PositionManager {
                val id = reader.readByte()
                when (id.toInt()) {
                    1 -> return ArrayManager.parseIndex(reader)
                    else -> throw IllegalArgumentException("Unknown manager")
                }
            }
        }

        /**
         * @return Sorted sequence of file ranges with chromosome ranges in given range.
         */
        abstract fun getPointers(start: Int, end: Int): Sequence<FileRange>

        class ArrayManager(val array: Array<ChromosomeRange>) : PositionManager() {
            companion object {
                /**
                 * Write ArrayManager data in writer.
                 */
                fun writeIndex(elements: List<ChromosomeRange>, writer: DataOutput) {
                    writer.writeInt(elements.size)
                    elements.forEach { line ->
                        writer.writeInt(line.chromosomeBegin)
                        writer.writeInt(line.chromosomeEnd)
                        writer.writeLong(line.fileRange.offset)
                        writer.writeInt(line.fileRange.length)
                    }
                }

                /**
                 * Read ArrayManager from reader.
                 */
                fun parseIndex(reader: DataInput): PositionManager {
                    val length = reader.readInt()
                    return ArrayManager(Array(length) {
                        ChromosomeRange(
                            reader.readInt(),
                            reader.readInt(),
                            FileRange(
                                reader.readLong(),
                                reader.readInt()
                            )
                        )
                    })
                }
            }

            override fun getPointers(start: Int, end: Int): Sequence<FileRange> {
                return array.asSequence()
                    .filter { it.chromosomeBegin >= start && it.chromosomeEnd <= end }
                    .map { it.fileRange }
            }
        }
    }
}