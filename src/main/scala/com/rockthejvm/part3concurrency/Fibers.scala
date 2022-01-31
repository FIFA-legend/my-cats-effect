package com.rockthejvm.part3concurrency

import cats.effect.kernel.Outcome
import cats.effect.kernel.Outcome.{Errored, Succeeded, Canceled}
import cats.effect.{Fiber, IO, IOApp, Outcome}

import scala.concurrent.duration._

object Fibers extends IOApp.Simple {

  val meaningOfLife = IO.pure(42)
  val favLang = IO.pure("Scala")

  import com.rockthejvm.utils._

  def sameThreadIOs() = for {
    _ <- meaningOfLife.debug
    _ <- favLang.debug
  } yield ()

  // introduce the Fiber
  def createFiber: Fiber[IO, Throwable, String] = ??? // effect type, error type, value type
  // almost impossible to create fibers manually

  // the fiber is not actually started, but the fiber allocation is wrapped in another effect
  val aFiber: IO[Fiber[IO, Throwable, Int]] = meaningOfLife.debug.start

  def differentThreadIOs() = for {
    _ <- aFiber
    _ <- favLang.debug
  } yield ()

  // joining a fiber
  def runOnSomeOtherThread[A](io: IO[A]): IO[Outcome[IO, Throwable, A]] = for {
    fib <- io.start
    result <- fib.join // an effect which waits for the fiber to terminate
  } yield result
  /*
    IO[ResultType of fib.join]
    fib.join = Outcome[IO, Throwable, A]

    possible outcomes:
    - success with an IO
    - failure with an exception
    - cancelled
  */

  val someIOOnAnotherThread = runOnSomeOtherThread(meaningOfLife)
  val someResultFromAnotherThread = someIOOnAnotherThread.flatMap {
    case Succeeded(effect) => effect
    case Errored(e) => IO(0)
    case Canceled() => IO(0)
  }

  def throwOnAnotherThread() = for {
    fib <- IO.raiseError[Int](new RuntimeException("No number for you")).start
    result <- fib.join
  } yield result

  def testCancel() = {
    val task = IO("Starting").debug >> IO.sleep(1.second) >> IO("Done").debug
    // onCancel is a "finalizer", allowing you to free up resources in case you get cancelled
    val taskWithCancellationHandler = task.onCancel(IO("I'm being cancelled!").debug.void)


    for {
      fib <- taskWithCancellationHandler.start // on a separate thread
      _ <- IO.sleep(500.millis) >> IO("Cancelling").debug // running on the calling thread
      _ <- fib.cancel
      result <- fib.join
    } yield result
  }

  /**
   * Exercises:
   * 1. Write a function that runs an IO on another thread, and, depending on the result of the fiber
   *   - return the result in an IO
   *   - if errored or cancelled, return a failed IO
   *
   * 2. Write a function that takes two IOs, runs them on different fibers and return an IO with a tuple containing both result
   *   - if both IOs complete successfully, tuple their results
   *   - if the first IO returns an error, raise that error (ignoring the second IO's result/error)
   *   - if the first IO doesn't error but second IO returns an error, raise that error
   *   - of one (or both) cancelled, raise a RuntimeException.
   *
   * 3. Write a function that adds a timeout to an IO:
   *   - IO runs on a fiber
   *   - if the timeout duration passes, then the fiber is cancelled
   *   - the method returns an IO[A] which contains
   *     - the original value if the computation is successful before timeout signal
   *     - the exception if the computation is failed before timeout signal
   *     - a RuntimeException if it times out (i.e. cancelled by the timeout)
   */

  // 1
  def processResultsFromFiber[A](io: IO[A]): IO[A] = for {
    fiber <- io.debug.start
    fiberResult <- fiber.join
    computationResult <- fiberResult match {
      case Succeeded(effect) => effect
      case Errored(ex) => IO.raiseError(ex)
      case Canceled() => IO.raiseError(new RuntimeException("Fiber cancelled"))
    }
  } yield computationResult

  def processResultsFromFiberRTJ[A](io: IO[A]): IO[A] = {
    val ioResult = for {
      fib <- io.debug.start
      result <- fib.join
    } yield result

    ioResult.flatMap {
      case Succeeded(fa) => fa
      case Errored(e) => IO.raiseError(e)
      case Canceled() => IO.raiseError(new RuntimeException("Computation cancelled."))
    }
  }

  def testEx1() = {
    val aComputation = IO("Starting").debug >> IO.sleep(1.second) >> IO("Done").debug >> IO(42)
    processResultsFromFiberRTJ(aComputation).void
  }

  // 2
  def tupleIOs[A, B](ioa: IO[A], iob: IO[B]): IO[(A, B)] = for {
    fiberA <- ioa.start
    fiberB <- iob.start
    resultA <- fiberA.join
    resultB <- fiberB.join
    result <- (resultA, resultB) match {
      case (Canceled(), _) => IO.raiseError(new RuntimeException("Fiber cancelled"))
      case (_, Canceled()) => IO.raiseError(new RuntimeException("Fiber cancelled"))
      case (Errored(ex), _) => IO.raiseError(ex)
      case (_, Errored(ex)) => IO.raiseError(ex)
      case (Succeeded(a): Succeeded[IO, Throwable, A], Succeeded(b): Succeeded[IO, Throwable, B]) => a.flatMap(a => b.map(b => (a, b)))
    }
  } yield result

  def tupleIOsRTJ[A, B](ioa: IO[A], iob: IO[B]): IO[(A, B)] = {
    val result = for {
      fibA <- ioa.start
      fibB <- iob.start
      resultA <- fibA.join
      resultB <- fibB.join
    } yield (resultA, resultB)

    result.flatMap {
      case (Succeeded(fa), Succeeded(fb)) => for {
        a <- fa
        b <- fb
      } yield (a, b)
      case (Errored(e), _) => IO.raiseError(e)
      case (_, Errored(e)) => IO.raiseError(e)
      case _ => IO.raiseError(new RuntimeException("Some computation cancelled."))
    }
  }

  def testEx2() = {
    val firstIO = IO.sleep(2.seconds) >> IO(1).debug
    val secondIO = IO.sleep(3.seconds) >> IO(2).debug
    tupleIOsRTJ(firstIO, secondIO).debug.void
  }

  // 3
  def timeout[A](io: IO[A], duration: FiniteDuration): IO[A] = {
    val computation = for {
      fib <- io.start
      _ <- (IO.sleep(duration) >> fib.cancel).start // careful - fibers can leak
      result <- fib.join
    } yield result

    computation.flatMap {
      case Succeeded(fa) => fa
      case Errored(e) => IO.raiseError(e)
      case Canceled() => IO.raiseError(new RuntimeException("Computation cancelled."))
    }
  }

  def testEx3() = {
    val computation = IO("Starting").debug >> IO.sleep(1.second) >> IO("Done").debug >> IO(42)
    timeout(computation, 500.millis).debug.void
  }

  override def run: IO[Unit] = testEx3()

}
