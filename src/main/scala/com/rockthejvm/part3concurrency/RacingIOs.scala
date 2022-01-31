package com.rockthejvm.part3concurrency

import cats.effect.kernel.Outcome
import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import cats.effect.{Fiber, IO, IOApp, Outcome}

import scala.concurrent.duration.*

object RacingIOs extends IOApp.Simple {

  import com.rockthejvm.utils._

  def runWithSleep[A](value: A, duration: FiniteDuration): IO[A] =
    (
    IO(s"Starting computation: $value").debug >>
      IO.sleep(duration) >>
      IO(s"Computation for $value: done") >>
      IO(value)
    ).onCancel(IO(s"Computation CANCELLED for $value").debug.void)

  def testRace() = {
    val meaningOfLife = runWithSleep(42, 1.second)
    val favLang = runWithSleep("Scala", 2.seconds)
    val first: IO[Either[Int, String]] = IO.race(meaningOfLife, favLang)
    /*
      both IOs run on separate fibers
      - the first one to finish will complete the result
      - the loser will be cancelled
    */

    first.flatMap {
      case Left(mol) => IO(s"Meaning of life won: $mol")
      case Right(lang) => IO(s"Fav language won: $lang")
    }
  }

  def testRacePair() = {
    val meaningOfLife = runWithSleep(42, 1.second)
    val favLang = runWithSleep("Scala", 2.seconds)
    val raceResult: IO[Either[
      (Outcome[IO, Throwable, Int], Fiber[IO, Throwable, String]), // (winner result, loser fiber)
      (Fiber[IO, Throwable, Int], Outcome[IO, Throwable, String]) // (loser fiber, winner result)
    ]] = IO.racePair(meaningOfLife, favLang)

    raceResult.flatMap {
      case Left((outMol, fibLang)) => fibLang.cancel >> IO("Mol won").debug >> IO(outMol).debug
      case Right((fibMol, outLang)) => fibMol.cancel >> IO("Language won").debug >> IO(outLang).debug
    }
  }

  /**
   * Exercises:
   * 1 - implement a timeout pattern with race
   * 2 - a method to return a LOSING effect from a race (hint: use racePair)
   * 3 - implement race in terms of racePair
   */

  // 1
  def timeout[A](io: IO[A], duration: FiniteDuration): IO[A] = {
    val timeoutIO = IO.sleep(duration)
    val result = IO.race(io, timeoutIO)
    result.flatMap {
      case Left(a) => IO(a)
      case Right(_) => IO.raiseError(new RuntimeException("Time has passed"))
    }
  }

  val importantTask = IO.sleep(2.seconds) >> IO(42).debug
  val testTimeout = timeout(importantTask, 1.second)
  val testTimeout_v2 = importantTask.timeout(1.second)

  // 2
  def outcomeToResult[R](outcome: Outcome[IO, Throwable, R]): IO[R] = {
    outcome match {
      case Succeeded(fa) => fa
      case Errored(ex) => IO.raiseError(ex)
      case Canceled() => IO.raiseError(new RuntimeException("Computation is cancelled"))
    }
  }

  def unrace[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] = {
    val result = IO.racePair(ioa, iob)
    result.flatMap {
      case Left((_, fiberB)) => fiberB.join.flatMap(v => outcomeToResult(v).map(Right(_)))
      case Right((fiberA, _)) => fiberA.join.flatMap(v => outcomeToResult(v).map(Left(_)))
    }
  }

  def unraceRTJ[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] =
    IO.racePair(ioa, iob).flatMap {
      case Left((_, fibB)) => fibB.join.flatMap {
        case Succeeded(resultEffect) => resultEffect.map(result => Right(result))
        case Errored(e) => IO.raiseError(e)
        case Canceled() => IO.raiseError(new RuntimeException("Loser cancelled."))
      }
      case Right((fibA, _)) => fibA.join.flatMap {
        case Succeeded(resultEffect) => resultEffect.map(result => Left(result))
        case Errored(e) => IO.raiseError(e)
        case Canceled() => IO.raiseError(new RuntimeException("Loser cancelled."))
      }
    }

  // 3
  // my solution doesn't care the case when winner computation is cancelled, so the other computation is the real winner
  def simpleRace[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] = {
    val race = IO.racePair(ioa, iob)
    race.flatMap {
      case Left((a, fiber)) => fiber.cancel >> outcomeToResult(a).map(Left(_))
      case Right((fiber, b)) => fiber.cancel >> outcomeToResult(b).map(Right(_))
    }
  }

  def simpleRaceRTJ[A, B](ioa: IO[A], iob: IO[B]): IO[Either[A, B]] =
    IO.racePair(ioa, iob).flatMap {
      case Left((outA, fibB)) => outA match {
        case Succeeded(effectA) => fibB.cancel >> effectA.map(a => Left(a))
        case Errored(e) => fibB.cancel >> IO.raiseError(e)
        case Canceled() => fibB.join.flatMap {
          case Succeeded(effectB) => effectB.map(b => Right(b))
          case Errored(e) => IO.raiseError(e)
          case Canceled() => IO.raiseError(new RuntimeException("Both computations cancelled."))
        }
      }
      case Right((fibA, outB)) => outB match {
        case Succeeded(effectB) => fibA.cancel >> effectB.map(b => Right(b))
        case Errored(e) => fibA.cancel >> IO.raiseError(e)
        case Canceled() => fibA.join.flatMap {
          case Succeeded(effectA) => effectA.map(a => Left(a))
          case Errored(e) => IO.raiseError(e)
          case Canceled() => IO.raiseError(new RuntimeException("Both computations cancelled."))
        }
      }
    }

  override def run: IO[Unit] = testRace().debug.void

}
