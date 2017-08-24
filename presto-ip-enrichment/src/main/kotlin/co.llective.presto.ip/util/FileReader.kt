/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.llective.presto.ip.util

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader

class FileReader(private val filePath: String, private val splitSign: String) {

    fun processFile(rowConsumer: (Array<String>) -> Unit) {
        try {
            getBufferedReader(filePath).use { reader ->
                var line = reader.readLine()
                while (line != null) {
                    val values = line.split(splitSign.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                    rowConsumer(values)
                    line = reader.readLine()
                }
            }
        } catch (exc: IOException) {
            throw RuntimeException("Problem with reading $filePath file", exc)
        }
    }

    private fun getBufferedReader(filePath: String): BufferedReader {
        val inputStream = javaClass.classLoader.getResourceAsStream(filePath)
        return BufferedReader(
                InputStreamReader(inputStream)
        )
    }
}
