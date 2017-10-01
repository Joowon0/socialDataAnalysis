import scala.collection.mutable.MutableList
import scala.collection.parallel.mutable.ParHashSet
import scala.collection.parallel.mutable.ParSet

val a : MutableList[Int] = MutableList(3)
a += 4
5 +=: a

a

println(a)
println(a map (x => x+10) mkString(" "))


val x : ParHashSet[Int] = ParHashSet(1)
x + 3
x + 2
x + 2
x + 2

x


val a1 : ParSet[Int] = ParSet(4)
a1 + 5
a1

