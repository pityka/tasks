/*
 * The MIT License
 *
 * Copyright (c) 2018 Istvan Bartha
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package tasks

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}
import org.scalatest.matchers.should.Matchers
import org.ekrich.config.{Config, ConfigFactory}
import tasks.util.config.ConfigValuesForHostConfiguration

class HostGpuConfigTest extends FunSuite with Matchers {

  def configValues(s: String): ConfigValuesForHostConfiguration =
    new ConfigValuesForHostConfiguration {
      val raw: Config = ConfigFactory
        .parseString(s)
        .withFallback(ConfigFactory.load("reference.conf"))
    }

  test("hostGPU returns sorted, distinct ids when sources overlap") {
    val v = configValues(
      """hosts.gpus = [0, 1]
        |hosts.gpusAsCommaString = "0,1"
        |""".stripMargin
    )
    v.hostGPU shouldBe List(0, 1)
  }

  test("hostGPU sorts unsorted gpu lists") {
    val v = configValues("hosts.gpus = [3, 1, 2, 0]")
    v.hostGPU shouldBe List(0, 1, 2, 3)
  }

  test("hostGPU is empty by default") {
    val v = configValues("")
    v.hostGPU shouldBe Nil
  }

}
