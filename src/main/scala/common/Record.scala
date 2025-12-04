package common

import java.util.Arrays

case class Record(data: Array[Byte]) extends Ordered[Record] {
  require(data.length == Record.SIZE, s"Record size must be ${Record.SIZE} bytes")

  // 정렬 기준: 앞 10바이트 (Key)
  def key: Array[Byte] = data.slice(0, 10)

  // 비교: Java 8 호환 Unsigned Byte Lexicographical Comparator
  override def compare(that: Record): Int = {
    val thisKey = this.key
    val thatKey = that.key
    val len = Math.min(thisKey.length, thatKey.length)
    
    var i = 0
    while (i < len) {
      val a = thisKey(i) & 0xFF
      val b = thatKey(i) & 0xFF
      if (a != b) {
        return a - b
      }
      i += 1
    }
    thisKey.length - thatKey.length
  }

  def toBytes: Array[Byte] = data
}

object Record {
  val SIZE = 100
  def fromBytes(bytes: Array[Byte]): Record = new Record(bytes)
}