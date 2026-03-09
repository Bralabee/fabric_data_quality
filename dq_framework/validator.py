"""
Data Quality Validator
======================

Core validation engine using Great Expectations 1.x.
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Any

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
    import great_expectations as gx
    from great_expectations.expectations.registry import get_expectation_impl

    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False

from .config_loader import ConfigLoader
from .constants import DEFAULT_VALIDATION_THRESHOLD

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Core data quality validator that executes Great Expectations checks.

    Features:
    - YAML-based configuration
    - Support for pandas DataFrames
    - Detailed validation reporting with severity-based thresholds
    - Reusable across projects

    Example:
        >>> validator = DataQualityValidator(config_path='config/checks.yml')
        >>> results = validator.validate(df)
        >>> if results['success']:
        ...     print("All checks passed!")
    """

    def __init__(
        self,
        config_path: str | None = None,
        config_dict: dict[str, Any] | None = None,
    ):
        """
        Initialize validator.

        Args:
            config_path: Path to YAML configuration file
            config_dict: Dictionary configuration (alternative to file)
        """
        if not GX_AVAILABLE:
            raise ImportError(
                "Great Expectations not installed. Install with: pip install great-expectations"
            )

        if pd is None:
            raise ImportError("Pandas not installed. Install with: pip install pandas")

        # Load configuration
        if config_path:
            loader = ConfigLoader()
            self.config = loader.load(config_path)
        elif config_dict:
            self.config = config_dict
        else:
            raise ValueError("Either config_path or config_dict must be provided")

        logger.info(
            f"DataQualityValidator initialized with "
            f"{len(self.config.get('expectations', []))} expectations"
        )

    def validate(
        self,
        df: "pd.DataFrame",
        batch_name: str | None = None,
        suite_name: str | None = None,
        threshold: float | None = None,
    ) -> dict[str, Any]:
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
        suite_name = suite_name or self.config.get("suite_name", "default_suite")
        batch_name = batch_name or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Determine threshold
        if threshold is None:
            threshold = self.config.get("threshold")

        logger.info(
            f"Running validation: suite='{suite_name}', batch='{batch_name}', "
            f"threshold={threshold if threshold is not None else 'Custom'}%"
        )

        # Use a fresh ephemeral context per validation to avoid name collisions
        context = gx.get_context()

        # Unique suffix to prevent name collisions across calls
        uid = uuid.uuid4().hex[:8]

        # Create data source, asset, and batch definition
        data_source = context.data_sources.add_pandas(f"pandas_{uid}")
        data_asset = data_source.add_dataframe_asset(
            name=self.config.get("data_asset_name", f"asset_{uid}")
        )
        batch_definition = data_asset.add_batch_definition_whole_dataframe(f"batch_{uid}")

        # Build expectation suite from config
        suite = self._build_expectation_suite(context, suite_name, uid)

        # Create validation definition and checkpoint
        validation_def = gx.ValidationDefinition(
            data=batch_definition, suite=suite, name=f"vd_{uid}"
        )
        validation_def = context.validation_definitions.add(validation_def)

        checkpoint = gx.Checkpoint(
            name=f"checkpoint_{uid}",
            validation_definitions=[validation_def],
        )
        checkpoint = context.checkpoints.add(checkpoint)

        # Execute validation
        checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

        # Extract the single validation result
        validation_result = next(iter(checkpoint_result.run_results.values()))

        # Format results
        summary = self._format_results(validation_result, batch_name, suite_name, threshold)

        # Include the validation result for detailed access
        summary["validation_result"] = validation_result

        return summary

    def _build_expectation_suite(self, context, suite_name: str, uid: str) -> "gx.ExpectationSuite":
        """Build a GX ExpectationSuite from configured expectations.

        Args:
            context: GX DataContext
            suite_name: Name for the suite
            uid: Unique suffix for naming

        Returns:
            The created ExpectationSuite
        """
        suite = gx.ExpectationSuite(name=f"{suite_name}_{uid}")

        for expectation_config in self.config.get("expectations", []):
            exp_type = expectation_config["expectation_type"]
            kwargs = expectation_config.get("kwargs", {})
            meta = expectation_config.get("meta", {})

            try:
                exp_class = get_expectation_impl(exp_type)
                expectation = exp_class(**kwargs, meta=meta)
                suite.add_expectation(expectation)
            except Exception as e:
                logger.warning(f"Could not create expectation '{exp_type}': {e}")

        suite = context.suites.add(suite)
        return suite

    def _format_results(
        self,
        validation_result,
        batch_name: str,
        suite_name: str,
        threshold: float | None = None,
    ) -> dict[str, Any]:
        """Format validation results into summary dictionary."""

        stats = validation_result.statistics

        # Calculate overall success rate
        evaluated = stats.get("evaluated_expectations", 0)
        successful = stats.get("successful_expectations", 0)
        success_rate = stats.get("success_percent")
        if success_rate is None and evaluated > 0:
            success_rate = (successful / evaluated) * 100
        elif success_rate is None:
            success_rate = 0

        # Get quality thresholds from config
        quality_thresholds = self.config.get("quality_thresholds", {})

        # Calculate success per severity
        severity_stats: dict[str, dict[str, int]] = {}

        # Initialize stats for known severities
        for severity in quality_thresholds:
            severity_stats[severity] = {"total": 0, "passed": 0}

        # Also track 'unknown' severity
        severity_stats["unknown"] = {"total": 0, "passed": 0}

        for result in validation_result.results:
            meta = result.expectation_config.meta or {}
            severity = meta.get("severity", "unknown")

            if severity not in severity_stats:
                severity_stats[severity] = {"total": 0, "passed": 0}

            severity_stats[severity]["total"] += 1
            if result.success:
                severity_stats[severity]["passed"] += 1

        # Check thresholds
        threshold_failures = []
        is_success = True

        # 1. Check global threshold (legacy support)
        if threshold is not None:
            if threshold >= DEFAULT_VALIDATION_THRESHOLD:
                if not validation_result.success:
                    is_success = False
                    threshold_failures.append(
                        f"Global threshold {DEFAULT_VALIDATION_THRESHOLD}% failed "
                        f"(actual: {success_rate:.1f}%)"
                    )
            elif success_rate < threshold:
                is_success = False
                threshold_failures.append(
                    f"Global threshold {threshold}% failed (actual: {success_rate:.1f}%)"
                )
        elif not quality_thresholds:
            # Fallback: if no thresholds defined at all, enforce 100% success
            if not validation_result.success:
                is_success = False
                threshold_failures.append(
                    f"Global threshold 100% failed (actual: {success_rate:.1f}%)"
                )

        # 2. Check per-severity thresholds
        for severity, s_stats in severity_stats.items():
            if s_stats["total"] > 0:
                s_rate = (s_stats["passed"] / s_stats["total"]) * 100
                s_threshold = quality_thresholds.get(severity)

                if s_threshold is not None and s_rate < s_threshold:
                    is_success = False
                    threshold_failures.append(
                        f"Severity '{severity}' threshold {s_threshold}% failed "
                        f"(actual: {s_rate:.1f}%)"
                    )

        summary = {
            "success": is_success,
            "suite_name": suite_name,
            "batch_name": batch_name,
            "timestamp": datetime.now().isoformat(),
            "evaluated_checks": evaluated,
            "successful_checks": successful,
            "failed_checks": stats.get("unsuccessful_expectations", 0),
            "success_rate": success_rate,
            "threshold": threshold,
            "quality_thresholds": quality_thresholds,
            "severity_stats": severity_stats,
            "threshold_failures": threshold_failures,
            "statistics": {
                "evaluated_expectations": evaluated,
                "successful_expectations": successful,
                "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
                "success_percent": success_rate,
            },
        }

        # Add details for failed expectations
        if not validation_result.success:
            failed = []
            for result in validation_result.results:
                if not result.success:
                    meta = result.expectation_config.meta or {}
                    kwargs = dict(result.expectation_config.kwargs)
                    kwargs.pop("batch_id", None)  # Remove GX internal field
                    failed.append(
                        {
                            "expectation": result.expectation_config.type,
                            "column": kwargs.get("column", "N/A"),
                            "severity": meta.get("severity", "unknown"),
                            "details": dict(result.result) if result.result else {},
                        }
                    )
            summary["failed_expectations"] = failed

        # Log summary
        if summary["success"]:
            logger.info(f"Validation PASSED: {summary['success_rate']:.1f}% success rate")
        else:
            logger.error(
                f"Validation FAILED: {summary['failed_checks']} checks failed. "
                f"Reasons: {', '.join(threshold_failures)}"
            )

        return summary

    def get_expectation_list(self) -> list[dict[str, Any]]:
        """
        Get list of configured expectations.

        Returns:
            List of expectation configurations
        """
        return self.config.get("expectations", [])

    def get_config_summary(self) -> dict[str, Any]:
        """
        Get summary of configuration.

        Returns:
            Dictionary with config summary
        """
        return {
            "data_source": self.config.get("data_source", {}),
            "suite_name": self.config.get("suite_name", "default_suite"),
            "expectation_count": len(self.config.get("expectations", [])),
            "expectations": [
                exp["expectation_type"] for exp in self.config.get("expectations", [])
            ],
        }
