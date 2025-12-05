package common

import java.util.Arrays
import scala.math.Ordering

case class Record(data: Array[Byte]) extends Ordered[Record] {
  require(data.length == Record.SIZE, s"Record size must be ${Record.SIZE} bytes")

  def key: Array[Byte] = data.slice(0, 10)

  override def compare(that: Record): Int = {
    Record.KeyOrdering.compare(this.key, that.key)
  }

  def toBytes: Array[Byte] = data
}

object Record {
  val SIZE = 100
  def fromBytes(bytes: Array[Byte]): Record = new Record(bytes)

  // [추가] Unsigned Byte 비교를 위한 명시적 Ordering 정의 (DataSorter 컴파일 에러 해결)
  implicit val KeyOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    override def compare(a: Array[Byte], b: Array[Byte]): Int = {
      val len = Math.min(a.length, b.length)
      var i = 0
      while (i < len) {
        val v1 = a(i) & 0xFF
        val v2 = b(i) & 0xFF
        if (v1 != v2) return v1 - v2
        i += 1
      }
      a.length - b.length
    }
  }
}
