package com.rockthejvm.part3concurrency

import cats.effect.{IO, IOApp}
import scala.concurrent.duration._

object CancellingIOs extends IOApp.Simple {

  import com.rockthejvm.utils._

  /*
    Cancelling IOs
    - fib.cancel
    - IO.race & other APIs
    - manual cancellation
  */
  val chainOfIOs: IO[Int] = IO("Waiting") >> IO.canceled >> IO(42).debug

  // uncancellable
  // example: online store, payment processor
  // payment process must NOT be cancelled
  val specialPaymentSystem = (
    IO("Payment running, don't cancel me...").debug >>
    IO.sleep(1.second) >>
    IO("Payment completed.").debug
  ).onCancel(IO("MEGA CANCEL OF DOOM!").debug.void)

  val cancellationOfDoom = for {
    fib <- specialPaymentSystem.start
    _ <- IO.sleep(500.millis) >> fib.cancel
    _ <- fib.join
  } yield ()

  val atomicPayment = IO.uncancelable(_ => specialPaymentSystem) // "masking"
  val atomicPayment_v2 = specialPaymentSystem.uncancelable // same

  val noCancellationOfDoom = for {
    fib <- atomicPayment.start
    _ <- IO.sleep(500.millis) >> IO("Attempting cancellation...").debug >> fib.cancel
    _ <- fib.join
  } yield ()

  /*
    The uncancellable API is more complex and more general.
    It takes a function from Poll[IO] to IO. In the example above, we aren't using that Poll instance.
    The Poll object can be used to mark sections within the returned effect which CAN BE CANCELLED.
  */

  /*
    Example: authentication service. Has two parts:
    - input password, can be cancelled, because otherwise we might block indefinitely on user input
    - verify password, CANNOT be cancelled once it's started
  */
  val inputPassword = IO("Input password").debug >> IO("(typing password)").debug >> IO.sleep(2.seconds) >> IO("RockTheJVM!")
  val verifyPassword = (pw: String) => IO("Verifying...").debug >> IO.sleep(2.seconds) >> IO(pw == "RockTheJVM!")

  val authFlow: IO[Unit] = IO.uncancelable { poll =>
    for {
      pw <- poll(inputPassword).onCancel(IO("Authentication timed out. Try again later.").debug.void) // this is cancellable
      verified <- verifyPassword(pw) // this is NOT cancellable
      _ <- if (verified) IO("Authentication successful.").debug else IO("Authentication failed.").debug // this is NOT cancellable
    } yield ()
  }

  val authProgram = for {
    authFib <- authFlow.start
    _ <- IO.sleep(3.seconds) >> IO("Authentication timeout, attempting cancel...").debug >> authFib.cancel
    _ <- authFib.join
  } yield ()

  /*
    Uncancellable calls are MARKS which suppress cancellation.
    Poll calls are "gaps opened" in the uncancellable region.
  */

  /**
   * Exercises
   */
  // 1
  val cancelBeforeMol = IO.canceled >> IO(42).debug
  val uncancelableMol = IO.uncancelable(_ => IO.canceled >> IO(42).debug)
  // uncancelable will eliminate ALL cancel point

  // 2 - IO.uncancelable makes gapes inside its call also uncancelable
  val invincibleAuthProgram = for {
    authFib <- IO.uncancelable(_ => authFlow).start
    _ <- IO.sleep(1.seconds) >> IO("Authentication timeout, attempting cancel...").debug >> authFib.cancel
    _ <- authFib.join
  } yield ()

  // 3
  def threeStepProgram(): IO[Unit] = {
    val sequence = IO.uncancelable { poll =>
      poll(IO("Cancellable").debug >> IO.sleep(1.second) >> IO("Cancellable end").debug) >>
        IO("Uncancelable").debug >> IO.sleep(1.second) >> IO("Uncancellable end").debug >>
        poll(IO("Second cancellable").debug >> IO.sleep(1.second) >> IO("Second cancellable end").debug)
    }

    for {
      fib <- sequence.start
      _ <- IO.sleep(1500.millis) >> IO("CANCELLING").debug >> fib.cancel
      _ <- fib.join
    } yield ()
  }

  override def run: IO[Unit] = threeStepProgram().void

}
