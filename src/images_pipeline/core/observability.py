"""Observability utilities for logging, metrics, and tracing."""

import logging
import time
import uuid
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
from functools import wraps
from enum import Enum


class LogLevel(Enum):
    """Log levels for structured logging."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogContext:
    """Context information for structured logging."""

    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation: str = ""
    component: str = ""
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def with_operation(self, operation: str) -> "LogContext":
        """Create new context with operation set."""
        return LogContext(
            correlation_id=self.correlation_id,
            operation=operation,
            component=self.component,
            user_id=self.user_id,
            metadata=self.metadata.copy(),
        )

    def with_metadata(self, **kwargs) -> "LogContext":
        """Create new context with additional metadata."""
        new_metadata = self.metadata.copy()
        new_metadata.update(kwargs)
        return LogContext(
            correlation_id=self.correlation_id,
            operation=self.operation,
            component=self.component,
            user_id=self.user_id,
            metadata=new_metadata,
        )


class StructuredLogger:
    """Structured logger with context support."""

    def __init__(self, name: str, level: int = logging.INFO):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _log(
        self,
        level: LogLevel,
        message: str,
        context: Optional[LogContext] = None,
        **kwargs,
    ):
        """Internal logging method with context support."""
        if context:
            log_data = {
                "message": message,
                "correlation_id": context.correlation_id,
                "operation": context.operation,
                "component": context.component,
                **context.metadata,
                **kwargs,
            }
            if context.user_id:
                log_data["user_id"] = context.user_id

            formatted_message = f"[{context.correlation_id}] {message}"
            if context.operation:
                formatted_message = f"[{context.operation}] {formatted_message}"

            # Add metadata to message if present
            if context.metadata or kwargs:
                metadata_str = ", ".join(
                    f"{k}={v}" for k, v in {**context.metadata, **kwargs}.items()
                )
                formatted_message = f"{formatted_message} ({metadata_str})"
        else:
            formatted_message = message

        getattr(self._logger, level.value.lower())(formatted_message)

    def debug(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, context, **kwargs)

    def info(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log info message."""
        self._log(LogLevel.INFO, message, context, **kwargs)

    def warning(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log warning message."""
        self._log(LogLevel.WARNING, message, context, **kwargs)

    def error(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log error message."""
        self._log(LogLevel.ERROR, message, context, **kwargs)

    def critical(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log critical message."""
        self._log(LogLevel.CRITICAL, message, context, **kwargs)


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations."""

    operation: str
    start_time: float
    end_time: float
    success: bool
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        """Calculate operation duration in seconds."""
        return self.end_time - self.start_time

    @property
    def duration_ms(self) -> float:
        """Calculate operation duration in milliseconds."""
        return self.duration * 1000


class MetricsCollector:
    """Collector for performance metrics."""

    def __init__(self):
        self._metrics: list[PerformanceMetrics] = []

    def record_metric(self, metric: PerformanceMetrics):
        """Record a performance metric."""
        self._metrics.append(metric)

    def get_metrics(self, operation: Optional[str] = None) -> list[PerformanceMetrics]:
        """Get recorded metrics, optionally filtered by operation."""
        if operation:
            return [m for m in self._metrics if m.operation == operation]
        return self._metrics.copy()

    def get_summary(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get summary statistics for metrics."""
        metrics = self.get_metrics(operation)

        if not metrics:
            return {}

        durations = [m.duration for m in metrics]
        successful = [m for m in metrics if m.success]
        failed = [m for m in metrics if not m.success]

        return {
            "total_operations": len(metrics),
            "successful_operations": len(successful),
            "failed_operations": len(failed),
            "success_rate": len(successful) / len(metrics) if metrics else 0,
            "avg_duration": sum(durations) / len(durations) if durations else 0,
            "min_duration": min(durations) if durations else 0,
            "max_duration": max(durations) if durations else 0,
            "total_duration": sum(durations),
        }

    def clear_metrics(self):
        """Clear all recorded metrics."""
        self._metrics.clear()


def timed_operation(
    operation_name: str,
    logger: Optional[StructuredLogger] = None,
    metrics_collector: Optional[MetricsCollector] = None,
    context: Optional[LogContext] = None,
):
    """Decorator for timing and logging operations."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            operation_context = (context or LogContext()).with_operation(operation_name)

            if logger:
                logger.info(f"Starting {operation_name}", operation_context)

            success = False
            error_message = None

            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                error_message = str(e)
                if logger:
                    logger.error(f"Failed {operation_name}: {e}", operation_context)
                raise
            finally:
                end_time = time.time()
                duration = end_time - start_time

                if logger:
                    if success:
                        logger.info(
                            f"Completed {operation_name}",
                            operation_context,
                            duration_ms=duration * 1000,
                        )
                    else:
                        logger.error(
                            f"Failed {operation_name}",
                            operation_context,
                            duration_ms=duration * 1000,
                        )

                if metrics_collector:
                    metric = PerformanceMetrics(
                        operation=operation_name,
                        start_time=start_time,
                        end_time=end_time,
                        success=success,
                        error_message=error_message,
                    )
                    metrics_collector.record_metric(metric)

        return wrapper

    return decorator


def log_operation_start(
    operation: str,
    logger: StructuredLogger,
    context: Optional[LogContext] = None,
    **metadata,
):
    """Log the start of an operation."""
    operation_context = (context or LogContext()).with_operation(operation)
    if metadata:
        operation_context = operation_context.with_metadata(**metadata)

    logger.info(f"Starting {operation}", operation_context)
    return operation_context


def log_operation_end(
    operation: str,
    logger: StructuredLogger,
    context: LogContext,
    success: bool = True,
    error_message: Optional[str] = None,
    **metadata,
):
    """Log the end of an operation."""
    if metadata:
        context = context.with_metadata(**metadata)

    if success:
        logger.info(f"Completed {operation}", context)
    else:
        logger.error(f"Failed {operation}: {error_message}", context)


class ObservabilityConfig:
    """Configuration for observability features."""

    def __init__(
        self,
        log_level: LogLevel = LogLevel.INFO,
        enable_metrics: bool = True,
        enable_correlation_ids: bool = True,
        component_name: str = "images_pipeline",
    ):
        self.log_level = log_level
        self.enable_metrics = enable_metrics
        self.enable_correlation_ids = enable_correlation_ids
        self.component_name = component_name


def create_logger(name: str, config: ObservabilityConfig) -> StructuredLogger:
    """Create a structured logger with the given configuration."""
    level_map = {
        LogLevel.DEBUG: logging.DEBUG,
        LogLevel.INFO: logging.INFO,
        LogLevel.WARNING: logging.WARNING,
        LogLevel.ERROR: logging.ERROR,
        LogLevel.CRITICAL: logging.CRITICAL,
    }

    return StructuredLogger(name, level_map[config.log_level])


def create_metrics_collector(config: ObservabilityConfig) -> Optional[MetricsCollector]:
    """Create a metrics collector if enabled in config."""
    if config.enable_metrics:
        return MetricsCollector()
    return None
