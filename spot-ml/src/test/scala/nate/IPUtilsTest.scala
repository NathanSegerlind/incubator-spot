package nate

import org.apache.spot.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import nate.Schema._

class IPUtilsTest extends TestingSparkContextFlatSpec with Matchers {

  "isInternal" should "correctly classify an internal IP" in {
      IPUtils.isInternal(IP("198.175.100.255")) shouldBe true
}

  "isInternal" should "correctly classify an external IP" in {
    IPUtils.isInternal(IP("192.169.11.0")) shouldBe false
  }
}
