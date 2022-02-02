package com.rockthejvm.part5polymorphic

import cats.effect.kernel.Outcome
import cats.{Applicative, Monad}
import cats.effect.{IO, IOApp, MonadCancel, Poll}

import scala.concurrent.duration._

object PolymorphicCancellation extends IOApp.Simple {

  trait MyApplicativeError[F[_], E] extends Applicative[F] {
    def raiseError[A](error: E): F[A]
    def handleErrorWith[A](fa: F[A])(f: E => F[A]): F[A]
  }

  trait MyMonadError[F[_], E] extends MyApplicativeError[F, E] with Monad[F]

  // MonadCancel

  trait MyPoll[F[_]] {
    def apply[A](fa: F[A]): F[A]
  }

  trait MyMonadCancel[F[_], E] extends MyMonadError[F, E] {
    def canceled: F[Unit]
    def uncancelable[A](poll: Poll[F] => F[A]): F[A]
  }

  // monadCancel for IO
  val monadCancelIO: MonadCancel[IO, Throwable] = MonadCancel[IO]

  // we can create values
  val molIO: IO[Int] = monadCancelIO.pure(42)
  val ambitiousMolIO: IO[Int] = monadCancelIO.map(molIO)(_ * 10)

  val mustCompute = monadCancelIO.uncancelable { _ =>
    for {
      _ <- monadCancelIO.pure("Once started, I can't go back...")
      res <- monadCancelIO.pure(56)
    } yield res
  }

  import cats.syntax.flatMap._ // flatMap
  import cats.syntax.functor._ // map
  def mustComputeGeneral[F[_], E](using mc: MonadCancel[F, E]): F[Int] = mc.uncancelable { _ =>
    for {
      _ <- mc.pure("Once started, I can't go back...")
      res <- mc.pure(56)
    } yield res
  }

  // can generalize code
  val mustCompute_v2 = mustComputeGeneral[IO, Throwable]

  // allow cancellation listeners
  val mustComputeWithListener = mustCompute.onCancel(IO("I'm being cancelled!").void)
  val mustComputeWithListener_v2 = monadCancelIO.onCancel(mustCompute, IO("I'm being cancelled!").void) // same
  // .onCancel as extension method
  import cats.effect.syntax.monadCancel._ // .onCancel

  // allow finalizers: guarantee, guaranteeCase
  val aComputationWithFinalizers = monadCancelIO.guaranteeCase(IO(42)) {
    case Outcome.Succeeded(fa) => fa.flatMap(a => IO(s"Successful: $a").void)
    case Outcome.Errored(e) => IO(s"Failed: $e").void
    case Outcome.Canceled() => IO("Cancelled").void
  }

  // bracket pattern is specific to MonadCancel
  val aComputationWithUsage = monadCancelIO.bracket(IO(42)) { value =>
    IO(s"Using the meaning of life: $value")
  } { value =>
    IO("Releasing the meaning of life...").void
  }

  /**
   * Exercise - generalize a piece of code
   */
  import com.rockthejvm.utils.general._

  // hint: use this instead of IO.sleep
  def unsafeSleep[F[_], E](duration: FiniteDuration)(using mc: MonadCancel[F, E]): F[Unit] =
    mc.pure(Thread.sleep(duration.toMillis)) // not semantic blocking

  val inputPassword1 = IO("Input password").debug >> IO("(typing password)").debug >> IO.sleep(2.seconds) >> IO("RockTheJVM!")
  val verifyPassword1 = (pw: String) => IO("Verifying...").debug >> IO.sleep(2.seconds) >> IO(pw == "RockTheJVM!")

  val authFlow1: IO[Unit] = IO.uncancelable { poll =>
    for {
      pw <- poll(inputPassword1).onCancel(IO("Authentication timed out. Try again later.").debug.void) // this is cancellable
      verified <- verifyPassword1(pw) // this is NOT cancellable
      _ <- if (verified) IO("Authentication successful.").debug else IO("Authentication failed.").debug // this is NOT cancellable
    } yield ()
  }

  val authProgram1 = for {
    authFib <- authFlow1.start
    _ <- IO.sleep(3.seconds) >> IO("Authentication timeout, attempting cancel...").debug >> authFib.cancel
    _ <- authFib.join
  } yield ()

  def inputPassword[F[_], E](using mc: MonadCancel[F, E]): F[String] =
    mc.pure("Input password").debug >> mc.pure("(typing password)").debug >> unsafeSleep(2.seconds) >> mc.pure("RockTheJVM!")

  def inputPasswordRTJ[F[_], E](using mc: MonadCancel[F, E]): F[String] = for {
    _ <- mc.pure("Input password").debug
    _ <- mc.pure("(typing password)").debug
    _ <- unsafeSleep[F, E](5.seconds)
    pw <- mc.pure("RockTheJVM1!")
  } yield pw

  def verifyPassword[F[_], E](using mc: MonadCancel[F, E]): String => F[Boolean] =
    (pw: String) => mc.pure("Verifying...").debug >> unsafeSleep(2.seconds) >> mc.pure(pw == "RockTheJVM!")

  def verifyPasswordRTJ[F[_], E](pw: String)(using mc: MonadCancel[F, E]): F[Boolean] = for {
    _ <- mc.pure("Verifying...").debug
    _ <- unsafeSleep[F, E](2.seconds)
    check <- mc.pure(pw == "RockTheJVM!")
  } yield check

  def authFlow[F[_], E](using mc: MonadCancel[F, E]): F[Unit] = mc.uncancelable { poll =>
    for {
      pw <- mc.onCancel(poll(inputPassword), mc.pure("Authentication timed out. Try again later.").debug.void) // this is cancellable
      verified <- verifyPassword(pw) // this is NOT cancellable
      _ <- if (verified) mc.pure("Authentication successful.").debug else mc.pure("Authentication failed.").debug // this is NOT cancellable
    } yield ()
  }

  def authFlowRTJ[F[_], E](using mc: MonadCancel[F, E]): F[Unit] = mc.uncancelable { poll =>
    for {
      pw <- poll(inputPasswordRTJ).onCancel(mc.pure("Authentication timed out. Try again later.").debug.void) // this is cancellable
      verified <- verifyPasswordRTJ(pw) // this is NOT cancellable
      _ <- if (verified) mc.pure("Authentication successful.").debug else mc.pure("Authentication failed.").debug // this is NOT cancellable
    } yield ()
  }

  val authProgram: IO[Unit] = for {
    authFib <- authFlow[IO, Throwable].start
    _ <- IO.sleep(3.seconds) >> IO("Authentication timeout, attempting cancel...").debug >> authFib.cancel
    _ <- authFib.join
  } yield ()

  override def run: IO[Unit] = authProgram

}