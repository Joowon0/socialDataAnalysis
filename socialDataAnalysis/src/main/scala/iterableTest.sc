
val a  : IndexedSeq[Int] = IndexedSeq(2,3,1)
val b  : IndexedSeq[Int] = IndexedSeq(6,5,4)

a(0)
a(1)

a.drop(0)
a.drop(2)


a
a ++ b

val x : Iterable[Int] = Iterable(1,2,3)
val s : Set[Int] = x.toSet

s - 3

//val