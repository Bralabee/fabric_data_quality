"""
Data Quality Validator
======================

Core validation engine using Great Expectations.
"""

import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
    import great_expectations as gx
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        InMemoryStoreBackendDefaults,
    )
    from great_expectations.core import (
        ExpectationSuite,
        ExpectationConfiguration,
    )
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
    AbstractDataContext = Any  # Define for type hinting if GE not available

from .config_loader import ConfigLoader
from .constants import DEFAULT_VALIDATION_THRESHOLD

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Core data quality validator that executes Great Expectations checks.
    
    Features:
    - YAML-based configuration
    - Support for pandas DataFrames
    - Detailed validation reporting
    - Reusable across projects
    
    Example:
        >>> validator = DataQualityValidator(config_path='config/checks.yml')
        >>> results = validator.validate(df)
        >>> if results['success']:
        ...     print("All checks passed!")
    """
    
    def __init__(self, config_path: Optional[str] = None, config_dict: Optional[Dict] = None):
        """
        Initialize validator.
        
        Args:
            config_path: Path to YAML configuration file
            config_dict: Dictionary configuration (alternative to file)
        """
        if not GX_AVAILABLE:
            raise ImportError(
                "Great Expectations not installed. "
                "Install with: pip install great-expectations"
            )
        
        if pd is None:
            raise ImportError(
                "Pandas not installed. "
                "Install with: pip install pandas"
            )
        
        # Load configuration
        if config_path:
            loader = ConfigLoader()
            self.config = loader.load(config_path)
        elif config_dict:
            self.config = config_dict
        else:
            raise ValueError("Either config_path or config_dict must be provided")
        
        # Initialize GE context
        self.context = self._initialize_context()
        
        logger.info(f"DataQualityValidator initialized with {len(self.config.get('expectations', []))} expectations")
    
    def _initialize_context(self) -> AbstractDataContext:
        """Initialize Great Expectations context."""
        data_context_config = DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults(),
        )
        
        context = gx.get_context(
            project_config=data_context_config,
            mode="ephemeral"
        )
        
        return context
    
    def validate(
        self,
        df: pd.DataFrame,
        batch_name: Optional[str] = None,
        suite_name: Optional[str] = None,
        threshold: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Validate a DataFrame against configured expectations.
        
        Args:
            df: DataFrame to validate
            batch_name: Name for this validation batch
            suite_name: Override suite name from config
            threshold: Success threshold percentage (0-100). Defaults to config value or 100.0
        
        Returns:
            Dictionary with validation results:
                - success: bool
                - evaluated_checks: int
                - failed_checks: int
                - success_rate: float
                - details: list of failures (if any)
                - timestamp: str
        """
        suite_name = suite_name or self.config.get('suite_name', 'default_suite')
        batch_name = batch_name or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Determine threshold
        if threshold is None:
            threshold = self.config.get('threshold')
        
        logger.info(f"Running validation: suite='{suite_name}', batch='{batch_name}', threshold={threshold if threshold is not None else 'Custom'}%")
        
        # Create expectation suite
        self._create_expectation_suite(suite_name)
        
        # Create datasource
        datasource_name = "pandas_datasource"
        try:
            datasource = self.context.sources.add_pandas(datasource_name)
        except Exception:
            datasource = self.context.get_datasource(datasource_name)
        
        # Create data asset
        data_asset_name = self.config.get('data_asset_name', 'runtime_data')
        try:
            data_asset = datasource.add_dataframe_asset(name=data_asset_name)
        except Exception:
            data_asset = datasource.get_asset(data_asset_name)
        
        # Build batch request
        batch_request = data_asset.build_batch_request(dataframe=df)
        
        # Create and run checkpoint
        checkpoint_name = f"checkpoint_{suite_name}"
        checkpoint_config = {
            "name": checkpoint_name,
            "validations": [{
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }],
        }
        
        try:
            checkpoint = self.context.add_checkpoint(**checkpoint_config)
        except Exception:
            checkpoint = self.context.get_checkpoint(checkpoint_name)
        
        # Execute validation
        results = checkpoint.run()
        validation_result = results.list_validation_results()[0]
        
        # Format results
        summary = self._format_results(validation_result, batch_name, suite_name, threshold)
        
        # Include the validation result for detailed access
        summary['validation_result'] = validation_result
        
        return summary
    
    def _create_expectation_suite(self, suite_name: str) -> None:
        """Create or get an expectation suite and populate it with configured expectations.
        
        Args:
            suite_name: Name of the expectation suite
        """
        # Add expectations from config to the suite
        from great_expectations.core import ExpectationConfiguration
        
        # Create a new suite with expectations
        suite = ExpectationSuite(expectation_suite_name=suite_name)
        
        for expectation_config in self.config.get('expectations', []):
            exp_config = ExpectationConfiguration(
                expectation_type=expectation_config['expectation_type'],
                kwargs=expectation_config.get('kwargs', {}),
                meta=expectation_config.get('meta', {})
            )
            suite.add_expectation(expectation_configuration=exp_config)
        
        # For GE 0.18.x ephemeral contexts, use add_or_update_expectation_suite
        try:
            self.context.add_or_update_expectation_suite(expectation_suite=suite)
        except AttributeError:
            # Fallback for older API
            try:
                self.context.add_expectation_suite(expectation_suite=suite)
            except Exception:
                try:
                    self.context.suites.add(suite)
                except Exception as e:
                    logger.warning(f"Could not save expectation suite: {e}")
    
    def _format_results(
        self,
        validation_result,
        batch_name: str,
        suite_name: str,
        threshold: Optional[float] = None
    ) -> Dict[str, Any]:
        """Format validation results into summary dictionary."""
        
        stats = validation_result.statistics
        
        # Calculate overall success rate
        evaluated = stats.get('evaluated_expectations', 0)
        successful = stats.get('successful_expectations', 0)
        success_rate = stats.get('success_percent')
        if success_rate is None and evaluated > 0:
            success_rate = (successful / evaluated) * 100
        elif success_rate is None:
            success_rate = 0
            
        # Get quality thresholds from config
        quality_thresholds = self.config.get('quality_thresholds', {})
        
        # Calculate success per severity
        severity_stats = {}
        
        # Initialize stats for known severities
        for severity in quality_thresholds.keys():
            severity_stats[severity] = {'total': 0, 'passed': 0}
            
        # Also track 'unknown' severity
        severity_stats['unknown'] = {'total': 0, 'passed': 0}

        for result in validation_result.results:
            meta = result.expectation_config.meta or {}
            severity = meta.get('severity', 'unknown')
            
            if severity not in severity_stats:
                severity_stats[severity] = {'total': 0, 'passed': 0}
                
            severity_stats[severity]['total'] += 1
            if result.success:
                severity_stats[severity]['passed'] += 1
        
        # Check thresholds
        threshold_failures = []
        is_success = True
        
        # 1. Check global threshold (legacy support)
        if threshold is not None:
            if threshold >= DEFAULT_VALIDATION_THRESHOLD:
                if not validation_result.success:
                    is_success = False
                    threshold_failures.append(f"Global threshold {DEFAULT_VALIDATION_THRESHOLD}% failed (actual: {success_rate:.1f}%)")
            elif success_rate < threshold:
                is_success = False
                threshold_failures.append(f"Global threshold {threshold}% failed (actual: {success_rate:.1f}%)")
        elif not quality_thresholds:
            # Fallback: if no thresholds defined at all, enforce 100% success
            if not validation_result.success:
                is_success = False
                threshold_failures.append(f"Global threshold 100% failed (actual: {success_rate:.1f}%)")
            
        # 2. Check per-severity thresholds
        for severity, s_stats in severity_stats.items():
            if s_stats['total'] > 0:
                s_rate = (s_stats['passed'] / s_stats['total']) * 100
                s_threshold = quality_thresholds.get(severity)
                
                if s_threshold is not None and s_rate < s_threshold:
                    is_success = False
                    threshold_failures.append(f"Severity '{severity}' threshold {s_threshold}% failed (actual: {s_rate:.1f}%)")
        
        summary = {
            'success': is_success,
            'suite_name': suite_name,
            'batch_name': batch_name,
            'timestamp': datetime.now().isoformat(),
            'evaluated_checks': evaluated,
            'successful_checks': successful,
            'failed_checks': stats.get('unsuccessful_expectations', 0),
            'success_rate': success_rate,
            'threshold': threshold,
            'quality_thresholds': quality_thresholds,
            'severity_stats': severity_stats,
            'threshold_failures': threshold_failures,
            'statistics': {
                'evaluated_expectations': evaluated,
                'successful_expectations': successful,
                'unsuccessful_expectations': stats.get('unsuccessful_expectations', 0),
                'success_percent': success_rate
            }
        }
        
        # Add details for failed expectations
        if not validation_result.success:
            failed = []
            for result in validation_result.results:
                if not result.success:
                    meta = result.expectation_config.meta or {}
                    failed.append({
                        'expectation': result.expectation_config.expectation_type,
                        'column': result.expectation_config.kwargs.get('column', 'N/A'),
                        'severity': meta.get('severity', 'unknown'),
                        'details': result.result
                    })
            summary['failed_expectations'] = failed
        
        # Log summary
        if summary['success']:
            logger.info(f"Validation PASSED: {summary['success_rate']:.1f}% success rate")
        else:
            logger.error(f"Validation FAILED: {summary['failed_checks']} checks failed. Reasons: {', '.join(threshold_failures)}")
        
        return summary
    
    def get_expectation_list(self) -> List[Dict[str, Any]]:
        """
        Get list of configured expectations.
        
        Returns:
            List of expectation configurations
        """
        return self.config.get('expectations', [])
    
    def get_config_summary(self) -> Dict[str, Any]:
        """
        Get summary of configuration.
        
        Returns:
            Dictionary with config summary
        """
        return {
            'data_source': self.config.get('data_source', {}),
            'suite_name': self.config.get('suite_name', 'default_suite'),
            'expectation_count': len(self.config.get('expectations', [])),
            'expectations': [exp['expectation_type'] for exp in self.config.get('expectations', [])]
        }
