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
package cs.drill.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;

public class FileReader {
  private String filePath;
  private String splitSign;

  public FileReader(String filePath, String splitSign) {
    this.filePath = filePath;
    this.splitSign = splitSign;
  }

  public void processFile(Consumer<String[]> rowConsumer) {
    try (BufferedReader reader = getBufferedReader(filePath)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split(splitSign);
        rowConsumer.accept(values);
      }
    } catch (IOException exc) {
      throw new RuntimeException("Problem with reading " + filePath + " file", exc);
    }
  }

  private BufferedReader getBufferedReader(String filePath) {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
    return new BufferedReader(
      new InputStreamReader(inputStream)
    );
  }
}
