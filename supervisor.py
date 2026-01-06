"""
Global Supervisor/Watchdog Module.
Monitors the main trading loop and handles crash recovery.
"""

import logging
import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable, Any, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class SupervisorStats:
    """Track supervisor statistics."""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    restarts: int = 0
    last_restart: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    consecutive_failures: int = 0
    total_runtime_seconds: float = 0.0


class Supervisor:
    """
    Global supervisor for crash recovery and health monitoring.

    Features:
    - Automatic restart on crash with exponential backoff
    - Heartbeat monitoring to detect stuck processes
    - Graceful shutdown handling
    - Max restart limit to prevent restart loops
    """

    def __init__(
        self,
        max_restarts: int = 5,  # Per Grok Round 18: Hard cap at 5 without manual intervention
        restart_delay_base: float = 5.0,
        restart_delay_max: float = 300.0,
        heartbeat_timeout: float = 10.0,  # Per Grok Round 18: 10s for HF (rn1 cycles <1s)
        cooldown_after_success: float = 300.0,
    ):
        """
        Initialize the supervisor.

        Args:
            max_restarts: Maximum restarts before giving up.
                         Per Grok Round 18: Reduced to 5 - restart loop on persistent
                         failure (bad creds, geoblock) drains gas/fees.
            restart_delay_base: Base delay between restarts (exponential backoff).
            restart_delay_max: Maximum delay between restarts.
            heartbeat_timeout: Seconds without heartbeat before force restart.
                              Per Grok Round 18: Reduced to 10s for HF trading.
                              rn1-style bots cycle <1s, so 10s catches stuck processes
                              much faster. 30s was still too slow for live sports.
            cooldown_after_success: Seconds of successful running before resetting restart count.
        """
        self.max_restarts = max_restarts
        self.restart_delay_base = restart_delay_base
        self.restart_delay_max = restart_delay_max
        self.heartbeat_timeout = heartbeat_timeout
        self.cooldown_after_success = cooldown_after_success

        self._stats = SupervisorStats()
        self._running = False
        self._shutdown_requested = False
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat = time.time()
        self._restarts_this_hour: List[float] = []  # Track restart times for alerting

    def heartbeat(self):
        """
        Record a heartbeat from the main loop.

        The main loop should call this regularly (e.g., every iteration).
        If no heartbeat is received within heartbeat_timeout, the supervisor
        will force a restart.
        """
        self._last_heartbeat = time.time()
        self._stats.last_heartbeat = datetime.now(timezone.utc)

    async def _heartbeat_monitor(self):
        """Monitor heartbeats and force restart if stuck."""
        while self._running and not self._shutdown_requested:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds

                if self._last_heartbeat:
                    time_since_heartbeat = time.time() - self._last_heartbeat

                    if time_since_heartbeat > self.heartbeat_timeout:
                        logger.error(
                            f"WATCHDOG: No heartbeat for {time_since_heartbeat:.0f}s "
                            f"(timeout: {self.heartbeat_timeout}s). Force restarting..."
                        )
                        # Cancel the main task to trigger restart
                        if self._main_task and not self._main_task.done():
                            self._main_task.cancel()
                        break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat monitor error: {e}")

    async def run(
        self,
        main_coro_factory: Callable[[], Awaitable[Any]],
        cleanup_coro: Optional[Callable[[], Awaitable[Any]]] = None,
    ):
        """
        Run the main coroutine with supervision.

        Args:
            main_coro_factory: Factory function that creates the main coroutine.
            cleanup_coro: Optional cleanup coroutine to run between restarts.
        """
        self._running = True
        self._setup_signal_handlers()

        logger.info(
            f"Supervisor started: max_restarts={self.max_restarts}, "
            f"heartbeat_timeout={self.heartbeat_timeout}s"
        )

        while self._running and not self._shutdown_requested:
            if self._stats.restarts >= self.max_restarts:
                logger.error(
                    f"Max restarts ({self.max_restarts}) reached. "
                    f"Manual intervention required."
                )
                break

            try:
                # Reset heartbeat
                self._last_heartbeat = time.time()
                run_start = time.time()

                # Start heartbeat monitor
                self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

                # Run main coroutine
                logger.info(f"Supervisor: Starting main loop (restart #{self._stats.restarts})")
                self._main_task = asyncio.create_task(main_coro_factory())

                try:
                    await self._main_task
                except asyncio.CancelledError:
                    logger.info("Main task cancelled")

                # If we get here cleanly, the main loop exited normally
                run_duration = time.time() - run_start
                self._stats.total_runtime_seconds += run_duration

                # If ran successfully for cooldown period, reset restart count
                if run_duration >= self.cooldown_after_success:
                    logger.info(
                        f"Ran successfully for {run_duration:.0f}s, "
                        f"resetting restart counter"
                    )
                    self._stats.consecutive_failures = 0

                logger.info("Main loop exited normally")
                break  # Normal exit

            except Exception as e:
                run_duration = time.time() - run_start
                self._stats.total_runtime_seconds += run_duration
                self._stats.consecutive_failures += 1

                logger.error(f"SUPERVISOR: Main loop crashed: {e}")

                # Run cleanup if provided
                if cleanup_coro:
                    try:
                        logger.info("Running cleanup...")
                        await cleanup_coro()
                    except Exception as cleanup_error:
                        logger.error(f"Cleanup failed: {cleanup_error}")

                # Exponential backoff for restarts
                delay = min(
                    self.restart_delay_base * (2 ** self._stats.consecutive_failures),
                    self.restart_delay_max
                )

                self._stats.restarts += 1
                self._stats.last_restart = datetime.now(timezone.utc)

                # Per Grok Round 18: Track restarts per hour for alerting
                now = time.time()
                self._restarts_this_hour = [t for t in self._restarts_this_hour if now - t < 3600]
                self._restarts_this_hour.append(now)

                if len(self._restarts_this_hour) > 3:
                    logger.critical(
                        f"ALERT: {len(self._restarts_this_hour)} restarts in last hour! "
                        f"Possible persistent failure (bad creds, geoblock, API down). "
                        f"Manual intervention recommended."
                    )

                if self._shutdown_requested:
                    logger.info("Shutdown requested, not restarting")
                    break

                logger.warning(
                    f"Restarting in {delay:.1f}s "
                    f"(restart {self._stats.restarts}/{self.max_restarts}, "
                    f"{len(self._restarts_this_hour)} this hour)..."
                )
                await asyncio.sleep(delay)

            finally:
                # Cancel heartbeat monitor
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                    try:
                        await self._heartbeat_task
                    except asyncio.CancelledError:
                        pass

        self._running = False
        logger.info(
            f"Supervisor stopped. "
            f"Total restarts: {self._stats.restarts}, "
            f"Total runtime: {self._stats.total_runtime_seconds:.0f}s"
        )

    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating graceful shutdown...")
            self._shutdown_requested = True
            self._running = False

            # Cancel main task
            if self._main_task and not self._main_task.done():
                self._main_task.cancel()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def request_shutdown(self):
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._shutdown_requested = True
        self._running = False

        if self._main_task and not self._main_task.done():
            self._main_task.cancel()

    def get_stats(self) -> dict:
        """Get supervisor statistics."""
        return {
            "start_time": self._stats.start_time.isoformat(),
            "restarts": self._stats.restarts,
            "consecutive_failures": self._stats.consecutive_failures,
            "last_restart": self._stats.last_restart.isoformat() if self._stats.last_restart else None,
            "last_heartbeat": self._stats.last_heartbeat.isoformat() if self._stats.last_heartbeat else None,
            "total_runtime_seconds": self._stats.total_runtime_seconds,
            "is_running": self._running,
        }


# Global supervisor instance
_supervisor: Optional[Supervisor] = None


def get_supervisor() -> Supervisor:
    """Get or create the global supervisor instance."""
    global _supervisor
    if _supervisor is None:
        _supervisor = Supervisor()
    return _supervisor


def heartbeat():
    """
    Record a heartbeat from the main loop.

    Call this regularly from your main loop to indicate the process is healthy.
    """
    if _supervisor:
        _supervisor.heartbeat()
