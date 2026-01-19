"""
Data Profiler
==============

Analyzes data to automatically generate appropriate data quality expectations.
This tool examines your data and suggests validation rules based on patterns found.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

from .constants import (
    ID_UNIQUENESS_THRESHOLD,
    CATEGORICAL_UNIQUENESS_THRESHOLD,
    DATE_DETECTION_THRESHOLD,
    TEXT_LENGTH_THRESHOLD,
    TYPE_DETECTION_SAMPLE_SIZE,
    MAX_UNIQUE_VALUES_DISPLAY,
    QUALITY_SCORE_NULL_WEIGHT,
    QUALITY_SCORE_UNIQUENESS_WEIGHT,
    UNIQUENESS_SWEET_SPOT,
    DEFAULT_NULL_TOLERANCE,
    ID_NULL_THRESHOLD_FOR_UNIQUENESS,
    DEFAULT_QUALITY_THRESHOLDS,
)

logger = logging.getLogger(__name__)


class DataProfiler:
    """
    Profiles data and generates appropriate data quality expectations.
    
    This class analyzes DataFrames to understand:
    - Column types and patterns
    - Null value percentages
    - Unique value counts
    - Value ranges and distributions
    - Common patterns (dates, emails, IDs, etc.)
    
    Then generates YAML configuration with appropriate expectations.
    
    Example:
        >>> import pandas as pd
        >>> from dq_framework import DataProfiler
        >>> 
        >>> df = pd.read_csv('my_data.csv')
        >>> profiler = DataProfiler(df)
        >>> 
        >>> # Get profile summary
        >>> profile = profiler.profile()
        >>> print(f"Analyzed {profile['row_count']} rows")
        >>> 
        >>> # Generate validation config
        >>> config = profiler.generate_expectations(
        ...     validation_name="my_data_validation",
        ...     severity_threshold="medium"
        ... )
        >>> 
        >>> # Save to YAML
        >>> profiler.save_config(config, 'my_validation.yml')
    """
    
    def __init__(self, df: pd.DataFrame, sample_size: Optional[int] = None):
        """
        Initialize profiler with a DataFrame.
        
        Args:
            df: DataFrame to profile
            sample_size: If provided, sample this many rows for analysis (for large datasets)
        """
        self.df = df if sample_size is None else df.sample(min(sample_size, len(df)))
        self.full_row_count = len(df)
        self.sampled = sample_size is not None and sample_size < len(df)
        self.profile_results = None
        
    def profile(self) -> Dict[str, Any]:
        """
        Profile the DataFrame and return summary statistics.
        
        Returns:
            Dictionary with profiling results including:
            - row_count: Total rows
            - column_count: Total columns
            - columns: Detailed info per column
            - data_quality_score: Overall score (0-100)
        """
        logger.info(f"Profiling data: {len(self.df)} rows, {len(self.df.columns)} columns")
        
        profile = {
            'row_count': self.full_row_count,
            'sampled_rows': len(self.df) if self.sampled else None,
            'column_count': len(self.df.columns),
            'columns': {},
            'profiled_at': datetime.now().isoformat(),
        }
        
        for col in self.df.columns:
            profile['columns'][col] = self._profile_column(col)
        
        # Calculate overall data quality score
        profile['data_quality_score'] = self._calculate_quality_score(profile['columns'])
        
        self.profile_results = profile
        return profile
    
    def _profile_column(self, col: str) -> Dict[str, Any]:
        """Profile a single column."""
        series = self.df[col]
        total_rows = len(series)
        
        if total_rows == 0:
            return {
                'dtype': str(series.dtype),
                'null_count': 0,
                'null_percent': 0.0,
                'unique_count': 0,
                'unique_percent': 0.0,
                'sample_values': [],
                'detected_type': 'empty'
            }
        
        col_profile = {
            'dtype': str(series.dtype),
            'null_count': int(series.isna().sum()),
            'null_percent': float(series.isna().sum() / total_rows * 100),
            'unique_count': int(series.nunique()),
            'unique_percent': float(series.nunique() / total_rows * 100),
            'detected_type': 'unknown'
        }
        
        # Non-null values for further analysis
        non_null = series.dropna()
        
        if len(non_null) > 0:
            # Detect column type/pattern
            col_profile['detected_type'] = self._detect_column_type(non_null)
            
            # Add statistics based on type
            if pd.api.types.is_numeric_dtype(series):
                col_profile['min'] = float(non_null.min())
                col_profile['max'] = float(non_null.max())
                col_profile['mean'] = float(non_null.mean())
                col_profile['median'] = float(non_null.median())
                col_profile['std'] = float(non_null.std())
                
            elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                col_profile['min_length'] = int(non_null.astype(str).str.len().min())
                col_profile['max_length'] = int(non_null.astype(str).str.len().max())
                col_profile['avg_length'] = float(non_null.astype(str).str.len().mean())
                
                # Sample values
                if col_profile['unique_count'] <= MAX_UNIQUE_VALUES_DISPLAY:
                    col_profile['unique_values'] = non_null.unique().tolist()[:MAX_UNIQUE_VALUES_DISPLAY]
                else:
                    col_profile['sample_values'] = non_null.head(5).tolist()
        
        return col_profile
    
    def _detect_column_type(self, series: pd.Series) -> str:
        """Detect the semantic type of a column."""
        sample = series.astype(str).head(TYPE_DETECTION_SAMPLE_SIZE)
        
        # Date patterns
        if series.dtype == 'datetime64[ns]' or self._looks_like_date(sample):
            return 'date'
        
        # ID patterns (mostly numeric, high uniqueness)
        if series.nunique() / len(series) > ID_UNIQUENESS_THRESHOLD and ('id' in series.name.lower() or 'uniqid' in series.name.lower()):
            return 'id'
        
        # Code patterns (alphanumeric, limited length, moderate uniqueness)
        if 'code' in series.name.lower() or 'nr' in series.name.lower():
            return 'code'
        
        # Numeric
        if pd.api.types.is_numeric_dtype(series):
            if 'amount' in series.name.lower() or 'value' in series.name.lower() or 'price' in series.name.lower():
                return 'monetary'
            elif 'percent' in series.name.lower() or 'rate' in series.name.lower():
                return 'percentage'
            return 'numeric'
        
        # Categorical (low uniqueness)
        if series.nunique() / len(series) < CATEGORICAL_UNIQUENESS_THRESHOLD:
            return 'categorical'
        
        # Text
        if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            avg_len = series.astype(str).str.len().mean()
            if avg_len > TEXT_LENGTH_THRESHOLD:
                return 'text'
            return 'string'
        
        return 'unknown'
    
    def _looks_like_date(self, sample: pd.Series) -> bool:
        """Check if string column looks like dates."""
        try:
            # Try to convert to datetime
            converted = pd.to_datetime(sample, errors='coerce')
            # Check if we have a significant number of valid dates
            valid_dates = converted.notna().sum()
            return valid_dates / len(sample) > DATE_DETECTION_THRESHOLD  # >80% must be valid dates
        except (ValueError, TypeError, OverflowError) as e:
            logger.debug(f"Date detection failed for sample: {e}")
            return False
    
    def _calculate_quality_score(self, columns: Dict[str, Any]) -> float:
        """Calculate overall data quality score (0-100)."""
        if not columns:
            return 0.0
        
        scores = []
        for col_info in columns.values():
            # Penalize high null percentages
            null_penalty = col_info['null_percent'] / 100
            
            # Reward appropriate uniqueness (not all same, not all unique unless it's an ID)
            uniqueness = col_info['unique_percent'] / 100
            if col_info.get('detected_type') == 'id':
                uniqueness_score = uniqueness  # IDs should be unique
            else:
                uniqueness_score = 1 - abs(UNIQUENESS_SWEET_SPOT - uniqueness)  # Sweet spot around 50%
            
            col_score = (1 - null_penalty) * QUALITY_SCORE_NULL_WEIGHT + uniqueness_score * QUALITY_SCORE_UNIQUENESS_WEIGHT
            scores.append(col_score * 100)
        
        return sum(scores) / len(scores)
    
    def generate_expectations(
        self,
        validation_name: str,
        description: Optional[str] = None,
        severity_threshold: str = "medium",
        include_structural: bool = True,
        include_completeness: bool = True,
        include_validity: bool = True,
        null_tolerance: float = 5.0,
        quality_thresholds: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Generate data quality expectations based on profiling results.
        
        Args:
            validation_name: Name for the validation suite
            description: Optional description
            severity_threshold: Default severity level ('critical', 'high', 'medium', 'low')
            include_structural: Include structural checks (table shape, columns exist)
            include_completeness: Include null/completeness checks
            include_validity: Include type/range/pattern checks
            null_tolerance: Percentage of nulls to tolerate before flagging (default 5%)
            quality_thresholds: Dictionary of success thresholds per severity level
        
        Returns:
            Dictionary in YAML config format
        """
        if self.profile_results is None:
            self.profile()
            
        # Default thresholds if not provided
        if quality_thresholds is None:
            quality_thresholds = DEFAULT_QUALITY_THRESHOLDS.copy()
        
        config = {
            'validation_name': validation_name,
            'description': description or f"Auto-generated validation for {validation_name}",
            'generated_at': datetime.now().isoformat(),
            'data_source': {
                'type': 'runtime_dataframe',
                'profiled_rows': self.profile_results['row_count'],
                'profiled_columns': self.profile_results['column_count'],
            },
            'quality_thresholds': quality_thresholds,
            'expectations': []
        }
        
        expectations = []
        
        # Structural expectations
        if include_structural:
            expectations.extend(self._generate_structural_expectations(severity_threshold))
        
        # Column-level expectations
        for col, col_info in self.profile_results['columns'].items():
            if include_completeness:
                expectations.extend(
                    self._generate_completeness_expectations(col, col_info, null_tolerance, severity_threshold)
                )
            
            if include_validity:
                expectations.extend(
                    self._generate_validity_expectations(col, col_info, severity_threshold)
                )
        
        config['expectations'] = expectations
        
        return config
    
    def _generate_structural_expectations(self, severity: str) -> List[Dict[str, Any]]:
        """Generate table-level structural expectations."""
        return [
            {
                'expectation_type': 'expect_table_row_count_to_be_between',
                'kwargs': {
                    'min_value': 1,
                    'max_value': None,
                },
                'meta': {
                    'severity': 'critical',
                    'description': 'Table should not be empty',
                }
            },
            {
                'expectation_type': 'expect_table_column_count_to_equal',
                'kwargs': {
                    'value': self.profile_results['column_count'],
                },
                'meta': {
                    'severity': 'high',
                    'description': 'Detect schema drift - column count should remain stable',
                }
            },
        ]
    
    def _generate_completeness_expectations(
        self, col: str, col_info: Dict[str, Any], null_tolerance: float, severity: str
    ) -> List[Dict[str, Any]]:
        """Generate completeness expectations for a column."""
        expectations = []
        
        # Always check column exists
        expectations.append({
            'expectation_type': 'expect_column_to_exist',
            'kwargs': {'column': col},
            'meta': {
                'severity': 'critical',
                'description': f'Column {col} must exist in the data',
            }
        })
        
        # Check for nulls if current null rate is below tolerance
        if col_info['null_percent'] < null_tolerance:
            expectations.append({
                'expectation_type': 'expect_column_values_to_not_be_null',
                'kwargs': {'column': col},
                'meta': {
                    'severity': 'high' if col_info['detected_type'] == 'id' else severity,
                    'description': f'Column {col} should have minimal nulls (currently {col_info["null_percent"]:.1f}%)',
                    'observed_null_percent': col_info['null_percent'],
                }
            })
        
        return expectations
    
    def _generate_validity_expectations(
        self, col: str, col_info: Dict[str, Any], severity: str
    ) -> List[Dict[str, Any]]:
        """Generate validity expectations for a column."""
        expectations = []
        detected_type = col_info.get('detected_type', 'unknown')
        
        # ID columns should be unique
        if detected_type == 'id' and col_info['null_percent'] < ID_NULL_THRESHOLD_FOR_UNIQUENESS:
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_unique',
                'kwargs': {'column': col},
                'meta': {
                    'severity': 'critical',
                    'description': f'ID column {col} must have unique values',
                }
            })
        
        # Numeric ranges
        if detected_type in ('numeric', 'monetary') and 'min' in col_info:
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_between',
                'kwargs': {
                    'column': col,
                    'min_value': col_info['min'],
                    'max_value': col_info['max'],
                },
                'meta': {
                    'severity': severity,
                    'description': f'Values should be within observed range [{col_info["min"]}, {col_info["max"]}]',
                }
            })
        
        # Categorical - check value set
        if detected_type == 'categorical' and 'unique_values' in col_info:
            expectations.append({
                'expectation_type': 'expect_column_values_to_be_in_set',
                'kwargs': {
                    'column': col,
                    'value_set': col_info['unique_values'],
                },
                'meta': {
                    'severity': severity,
                    'description': f'Values should be one of the known categories',
                }
            })
        
        # String length
        if 'min_length' in col_info and detected_type in ('string', 'code'):
            expectations.append({
                'expectation_type': 'expect_column_value_lengths_to_be_between',
                'kwargs': {
                    'column': col,
                    'min_value': col_info['min_length'],
                    'max_value': col_info['max_length'],
                },
                'meta': {
                    'severity': 'low',
                    'description': f'String length should be within observed range',
                }
            })
        
        return expectations
    
    def save_config(self, config: Dict[str, Any], output_path: str) -> None:
        """
        Save generated config to YAML file.
        
        Args:
            config: Configuration dictionary from generate_expectations()
            output_path: Path to save YAML file
        """
        import yaml
        
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Saved configuration to {output_path}")
        logger.info(f"Configuration saved: {output_path}")
        logger.info(f"   - {len(config['expectations'])} expectations generated")
    
    def print_summary(self) -> None:
        """Print a human-readable summary of the profiling results."""
        if self.profile_results is None:
            self.profile()
        
        profile = self.profile_results
        
        print("=" * 80)
        print("DATA PROFILE SUMMARY")
        print("=" * 80)
        print(f"Rows: {profile['row_count']:,}")
        print(f"Columns: {profile['column_count']}")
        print(f"Data Quality Score: {profile['data_quality_score']:.1f}/100")
        print()
        
        print("COLUMN DETAILS:")
        print("-" * 80)
        print(f"{'Column':<30} {'Type':<15} {'Nulls':<10} {'Unique':<10}")
        print("-" * 80)
        
        for col, info in profile['columns'].items():
            col_display = col[:28] + '..' if len(col) > 30 else col
            detected_type = info.get('detected_type', 'unknown')
            null_pct = f"{info['null_percent']:.1f}%"
            unique_pct = f"{info['unique_percent']:.1f}%"
            
            print(f"{col_display:<30} {detected_type:<15} {null_pct:<10} {unique_pct:<10}")
        
        print("=" * 80)
