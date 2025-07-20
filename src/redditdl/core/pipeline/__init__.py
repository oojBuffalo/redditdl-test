"""
Pipeline Infrastructure

Core pipeline components implementing the Pipeline & Filter architectural pattern.
Provides abstract interfaces and execution framework for processing Reddit content
through a series of configurable stages.
"""

from .interfaces import PipelineStage, PipelineContext, PipelineResult
from .executor import PipelineExecutor

__all__ = [
    'PipelineStage',
    'PipelineContext',
    'PipelineResult', 
    'PipelineExecutor'
]