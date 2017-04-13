package nate


class IP(s: String) extends Serializable {

  require(IP.isValidIP(s))


  val oct0 = s.split('.')(0).toInt
  val oct1 = s.split('.')(1).toInt
  val oct2 = s.split('.')(2).toInt
  val oct3 = s.split('.')(3).toInt

  def <=(that: IP): Boolean = {
    oct0 < that.oct0 ||
      (oct0 == that.oct0 && oct1 < that.oct1) ||
      (oct0 == that.oct0 && oct1 == that.oct1 && oct2 < that.oct2) ||
      (oct0 == that.oct0 && oct1 == that.oct1 && oct2 == that.oct2 && oct3 <= that.oct3)
  }

}

object IP extends Serializable {
  def apply(s: String) = new IP(s)

  def isValidIP(s: String): Boolean = {
    if (s == null) {
      false
    } else {
      val octets = s.split('.')
      (octets.length == 4) && octets.forall(_.forall(_.isDigit)) && octets.forall(x => (0 <= x.toInt && x.toInt <= 255))
    }
  }
}

case class IPRange(lower: IP, upper: IP) extends Serializable {

  require(lower <= upper)

  def contains(ip: IP) = lower <= ip && ip <= upper


}

object IPRange extends Serializable {
  def apply(l: String, u: String): IPRange = IPRange(IP(l), IP(u))

  def lt(a: IPRange, b: IPRange): Boolean = a.upper <= b.lower

}

object IPUtils extends Serializable {


  val internalIPRanges: Vector[IPRange] = Vector(
    IPRange("10.0.0.0", "10.255.255.255"),
    IPRange("172.16.0.0", "172.16.31.255"),
    IPRange("192.168.0.0", "192.168.255.255"),
    IPRange("198.93.84.0", "198.93.87.255"),
    IPRange("192.169.1.0", "192.169.3.255"),
    IPRange("178.178.176.0", "178.178.178.255"),
    IPRange("143.185.222.0", "143.185.222.255"),
    IPRange("143.181.0.0","143.185.255.255"),

  IPRange("192.52.51.0", "192.52.60.255"),
    IPRange("192.55.32.0", "192.55.81.255"),
    IPRange("192.55.233.0", "192.55.233.255"),
    IPRange("192.102.182.0", "192.102.211.255"),
    IPRange("192.198.128.0", "192.198.177.255"),
    IPRange("198.175.64.0", "198.175.123.255"),
    IPRange("204.128.183.0", "204.128.183.255"),
    IPRange("134.191.200.0", "134.191.255.255"),
    IPRange("128.215.0.0", "128.215.255.255"),
    IPRange("132.233.0.0", "132.233.255.255"),
    IPRange("134.134.0.0", "134.134.255.255"),
    IPRange("137.46.0.0", "137.46.255.255"),
    IPRange("146.152.0.0", "146.152.255.255"),
    IPRange("147.208.0.0", "147.208.255.255"),
    IPRange("134.191.0.0", "134.191.255.255"),
    IPRange("137.102.0.0", "137.102.255.255"),
    IPRange("146.152.0.0", "146.152.255.255"),
    IPRange("161.69.0.0", "161.69.255.255"),
    IPRange("163.33.0.0", "163.33.255.255")
  )

  def isInternal(ip: IP): Boolean = {
    internalIPRanges.exists(_.contains(ip))
  }

  def isExternal(ip: IP): Boolean = {
    !internalIPRanges.exists(_.contains(ip))
  }
  def isInternalIP(s: String): Boolean = {
      IP.isValidIP(s) && isInternal(IP(s))
  }

  def isExternalIP(s: String): Boolean = {
    IP.isValidIP(s) && isExternal(IP(s))
  }

}
