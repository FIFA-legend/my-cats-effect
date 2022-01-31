package com.rockthejvm.part2effects

import cats.effect.IO

import scala.io.StdIn

object IOIntroduction {

  // IO
  val ourFirstIO: IO[Int] = IO.pure(42) // arg that should not have side effects
  val aDelayedIO: IO[Int] = IO.delay {
    println("I'm producing an integer")
    54
  }

  val shouldNotDoThis: IO[Int] = IO.pure {
    println("I'm producing an integer")
    54
  }

  val aDelayedIO_v2: IO[Int] = IO { // apply == delay
    println("I'm producing an integer")
    54
  }

  // map, flatMap
  val improvedMeaningOfLife = ourFirstIO.map(_ * 2)
  val printedMeaningOfLife = ourFirstIO.flatMap(mol => IO.delay(println(mol)))

  def smallProgram(): IO[Unit] = for {
    line1 <- IO(StdIn.readLine())
    line2 <- IO(StdIn.readLine())
    _ <- IO.delay(println(line1 + line2))
  } yield ()

  // mapN - combine IO effects as tuples
  import cats.syntax.apply._
  val combinedMeaningOfLife: IO[Int] = (ourFirstIO, improvedMeaningOfLife).mapN(_ + _)

  def smallProgram_v2(): IO[Unit] =
    (IO(StdIn.readLine()), IO(StdIn.readLine())).mapN(_ + _).map(println)

  /**
   * Exercises
   */

  // 1 - sequence two IOs and take the result of the LAST one
  // hint: use flatMap
  def sequenceTakeLast[A, B](ioa: IO[A], iob: IO[B]): IO[B] = for {
    _ <- ioa
    result <- iob
  } yield result

  def sequenceTakeLastRTJ[A, B](ioa: IO[A], iob: IO[B]): IO[B] =
    ioa.flatMap(_ => iob)

  def sequenceTakeLast_v2[A, B](ioa: IO[A], iob: IO[B]): IO[B] = ioa *> iob // "andThen"

  def sequenceTakeLast_v3[A, B](ioa: IO[A], iob: IO[B]): IO[B] = ioa >> iob // "andThen" with by-name call

  // 2 - sequence two IOs and take the result of the FIRST one
  // hint: use flatMap
  def sequenceTakeFirst[A, B](ioa: IO[A], iob: IO[B]): IO[A] = for {
    result <- ioa
    _ <- iob
  } yield result

  def sequenceTakeFirstRTJ[A, B](ioa: IO[A], iob: IO[B]): IO[A] =
    ioa.flatMap(a => iob.map(_ => a))

  def sequenceTakeFirst_v2[A, B](ioa: IO[A], iob: IO[B]): IO[A] = ioa <* iob

  // 3 - repeat an IO effect forever
  // hint: use flatMap + recursion
  def forever[A](io: IO[A]): IO[A] = for {
    result <- io
    _ <- forever(io)
  } yield result

  def foreverRTJ[A](io: IO[A]): IO[A] =
    io.flatMap(_ => foreverRTJ(io))

  def forever_v2[A](io: IO[A]): IO[A] =
    io >> forever_v2(io) // same

  def forever_v3[A](io: IO[A]): IO[A] =
    io *> forever_v3(io) // same

  def forever_v4[A](io: IO[A]): IO[A] = io.foreverM // with tail recursion

  // 4 - convert an IO to a different type
  // hint: use map
  def convert[A, B](ioa: IO[A], value: B): IO[B] = ioa.map(_ => value)

  def convert_v2[A, B](ioa: IO[A], value: B): IO[B] = ioa.as(value) // same

  // 5 - discard value inside an IO, just return Unit
  def asUnit[A](ioa: IO[A]): IO[Unit] = convert(ioa, ())

  def asUnitRTJ[A](ioa: IO[A]): IO[Unit] = ioa.map(_ => ())

  def asUnit_v2[A](ioa: IO[A]): IO[Unit] = ioa.as(()) // discourage - don't use this

  def asUnit_v3[A](ioa: IO[A]): IO[Unit] = ioa.void // same - encouraged

  // 6 - fix stack recursion
  def sum(n: Int): Int =
    if (n <= 0) 0
    else n + sum(n - 1)

  def sumIO(n: Int): IO[Int] = {
    def sumIORec(ion: IO[Int]): IO[Int] = for {
      number <- ion
      result <- if (number <= 0) IO(0) else sumIORec(IO(number - 1))
    } yield result + number

    sumIORec(IO(n))
  }

  def sumIO_v2(n: Int): IO[Int] =
    if (n <= 0) IO(0)
    else for {
      lastNumber <- IO(n)
      prevSum <- sumIO_v2(n - 1)
    } yield prevSum + lastNumber

  // 7 (hard) - write a fibonacci function that does not crash on recursion
  // hints: use recursion, ignore exponential complexity, use flatMap heavily
  def fibonacci(n: Int): IO[BigInt] =
    if (n < 2) IO(1)
    else for {
      last <- IO.defer(fibonacci(n - 1)) // same as delay(...).flatten
      prev <- IO(fibonacci(n - 2)).flatten
    } yield last + prev

  def main(args: Array[String]): Unit = {
    import cats.effect.unsafe.implicits.global // "platform"
    // "end of the world"
    // println(aDelayedIO.unsafeRunSync())
    // println(smallProgram().unsafeRunSync())
    // println(smallProgram_v2().unsafeRunSync())
    println(sumIO_v2(20000).unsafeRunSync())
    println(fibonacci(100).unsafeRunSync())
  }

}
