package com.rockthejvm.part5polymorphic

import cats.effect.kernel.Concurrent
import cats.effect.{IO, IOApp, Temporal}

import scala.concurrent.duration._
import com.rockthejvm.utils.general._

object PolymorphicTemporalSuspension extends IOApp.Simple {

  // Temporal - time-blocking effects
  trait MyTemporal[F[_]] extends Concurrent[F] {
    def sleep(time: FiniteDuration): F[Unit] // semantically blocks this fiber for a specified time
  }

  // abilities: pure, map/flatMap, raiseError, uncancelable, start, ref/deferred, + sleep
  val temporalIO = Temporal[IO] // given Temporal[IO] in scope
  val chainOfEffects = IO("Loading...").debug *> IO.sleep(1.second) *> IO("Game ready!").debug
  val chainOfEffects_v2 = temporalIO.pure("Loading...").debug *> temporalIO.sleep(1.second) *> temporalIO.pure("Game ready!").debug

  /**
   * Exercise: generalize the following piece
   */
  import cats.syntax.flatMap._

  def timeout[F[_], A](fa: F[A], duration: FiniteDuration)(using temporal: Temporal[F]): F[A] = {
    val timeoutIO = temporal.sleep(duration)
    val result = temporal.race(fa, timeoutIO)
    result.flatMap {
      case Left(a) => temporal.pure(a)
      case Right(_) => temporal.raiseError(new RuntimeException("Time has passed"))
    }
  }

  override def run: IO[Unit] = ???

}
