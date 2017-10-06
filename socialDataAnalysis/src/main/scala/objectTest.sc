import libFromCoursera.Var

class Score(i : Int) {
  val sc = Var(i)

  def decrease() = {
    val temp = sc() - 1
    sc() = temp
  }
}

class Obj(val score: Score) {}
val score10 = new Score(10)
println(score10.sc())

val x = score10.decrease()
println(score10.sc())


val o : Obj = new Obj(score10)
val o2 : Obj = new Obj(score10)
println(score10.sc())
println(o.score.sc())
println(o2.score.sc())

score10.decrease()
println(score10.sc())
println(o.score.sc())
println(o2.score.sc())