"""Structured logging utilities for the trading system."""

import logging
import structlog
from datetime import datetime

class StructuredLogger:
    """Structured logger for consistent logging across the system."""
    
    def __init__(self, name: str):
        self.logger = structlog.get_logger(name)
    
    def info(self, message: str, **kwargs):
        """Log info message with structured data."""
        log_kwargs = {k: v for k, v in kwargs.items() if k != 'timestamp'}
        self.logger.info(message, log_timestamp=datetime.now().isoformat(), **log_kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with structured data."""
        log_kwargs = {k: v for k, v in kwargs.items() if k != 'timestamp'}
        self.logger.error(message, log_timestamp=datetime.now().isoformat(), **log_kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with structured data."""
        log_kwargs = {k: v for k, v in kwargs.items() if k != 'timestamp'}
        self.logger.warning(message, log_timestamp=datetime.now().isoformat(), **log_kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with structured data."""
        log_kwargs = {k: v for k, v in kwargs.items() if k != 'timestamp'}
        self.logger.debug(message, log_timestamp=datetime.now().isoformat(), **log_kwargs)
