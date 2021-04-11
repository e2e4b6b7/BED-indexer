package org.jetbrains.internship

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.lang.System.currentTimeMillis
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createFile
import kotlin.math.pow
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.math.*

internal class BedReaderImplTest {
    companion object {
        const val chromosomesSize: Int = 10000

        private fun generateDirectories() {
            val path = Path.of("BEDs")
            if (!Files.exists(path)) {
                Files.createDirectory(path)
            }
            val indexPath = Path.of("BED indices")
            if (!Files.exists(indexPath)) {
                Files.createDirectory(indexPath)
            }

        }

        fun generateFiles() {
            generateDirectories()

            (1..3).map { 3.0.pow(it.toDouble()).roundToInt() }.forEach { count ->
                (1..8).map { 5.0.pow(it.toDouble()).roundToInt() }.forEach { size ->
                    val path = Path.of("BEDs", "generated.$size.$count")
                    if (!Files.exists(path)) {
                        Files.createFile(path)
                        generateFile(path, size, count)
                    }
                }
            }
        }

        private fun generateFile(path: Path, size: Int, count: Int) {
            path.toFile().printWriter().use { writer ->
                val chromosomesList = (0 until count).map { "chromosome$it" }.toList()
                repeat(size) {
                    val chromosome = chromosomesList.random()

                    val l = Random.Default.nextInt(0, chromosomesSize)
                    val tmpR = Random.Default.nextInt(0, chromosomesSize - 1)
                    val r = if (tmpR >= l) {
                        tmpR + 1
                    } else {
                        tmpR
                    }

                    writer.print(chromosome)
                    writer.print('\t')
                    writer.print(min(l, r))
                    writer.print('\t')
                    writer.print(max(l, r))
                    writer.print('\t')
                    writer.print("someAttribute")
                    writer.println()
                }
            }
        }
    }

    @Test
    fun testSimple0() {
        testSimple("sample0")
    }

    @Test
    fun testSimple1() {
        testSimple("sample1")
    }

    private fun testSimple(filename: String) {
        val bedFile = Path.of("testBEDs", "$filename.txt")
        val bedIndex = Path.of("testBEDs", "$filename.index")
        BedReaderImpl.createIndex(bedFile, bedIndex)
        val index = BedReaderImpl.loadIndex(bedIndex)
        val correctReader = SimpleReader(bedFile)
        assertEquals(
            correctReader.find("chr7", 127471196, 127472363),
            BedReaderImpl.findWithIndex(index, bedFile, "chr7", 127471196, 127472363)
        )
        assertEquals(
            correctReader.find("chr7", 127480532, 127481699),
            BedReaderImpl.findWithIndex(index, bedFile, "chr7", 127480532, 127481699)
        )
        assertEquals(
            correctReader.find("chr7", 127471196, 127481699),
            BedReaderImpl.findWithIndex(index, bedFile, "chr7", 127471196, 127481699)
        )
    }

    @Test
    fun stressSmallTest() {
        generateFiles()

        val dir = Path.of("BEDs")
        dir.toFile().listFiles()?.forEach {
            val data = it.name.split('.')
            if (data[1].toInt() < 30000) {
                testFile(it.name, data[2].toInt())
            }
        }
    }

    @Test
    fun stressBigTest() {
        generateFiles()

        val dir = Path.of("BEDs")
        dir.toFile().listFiles()?.forEach {
            val data = it.name.split('.')
            if (data[1].toInt() > 10000) {
                testFile(it.name, data[2].toInt())
            }
        }
    }

    private fun testFile(filename: String, count: Int) {
        val bedPath = Path.of("BEDs", filename)
        val indexPath = Path.of("BED indices", filename)

        BedReaderImpl.createIndex(bedPath, indexPath)
        val index = BedReaderImpl.loadIndex(indexPath)

        val chromosomesList = (0 until count).map { "chromosome$it" }.toList()

        val correctReader = SimpleReader(bedPath)

        repeat(20) {
            val chromosome = chromosomesList.random()

            val l = Random.Default.nextInt(0, chromosomesSize)
            val tmpR = Random.Default.nextInt(0, chromosomesSize - 1)
            val r = if (tmpR >= l) {
                tmpR + 1
            } else {
                tmpR
            }

            val actual = BedReaderImpl.findWithIndex(index, bedPath, chromosome, l, r)
            val correct = correctReader.find(chromosome, l, r)
            assertEquals(correct, actual)
        }
    }
}