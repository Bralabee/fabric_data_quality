"""
Constants for the Data Quality Framework.

This module centralizes all magic numbers and configuration defaults
to improve maintainability and allow environment-specific overrides.

Categories:
    - Validation Thresholds: Success rate thresholds for validation
    - Data Profiling: Thresholds for type detection and analysis
    - Data Loading: File size and row limits for memory protection
    - Fabric Integration: Settings specific to MS Fabric environments

Usage:
    from dq_framework.constants import DEFAULT_VALIDATION_THRESHOLD
"""

# =============================================================================
# VALIDATION THRESHOLDS
# =============================================================================

# Default success threshold percentage (0-100) for validation to pass
DEFAULT_VALIDATION_THRESHOLD = 100.0

# Quality thresholds by severity level
QUALITY_THRESHOLD_CRITICAL = 100.0
QUALITY_THRESHOLD_HIGH = 95.0
QUALITY_THRESHOLD_MEDIUM = 80.0
QUALITY_THRESHOLD_LOW = 50.0

# Default quality thresholds dictionary
DEFAULT_QUALITY_THRESHOLDS = {
    "critical": QUALITY_THRESHOLD_CRITICAL,
    "high": QUALITY_THRESHOLD_HIGH,
    "medium": QUALITY_THRESHOLD_MEDIUM,
    "low": QUALITY_THRESHOLD_LOW,
}

# =============================================================================
# DATA PROFILING - TYPE DETECTION
# =============================================================================

# Uniqueness ratio threshold to detect ID columns (> 90% unique values)
ID_UNIQUENESS_THRESHOLD = 0.9

# Uniqueness ratio threshold to detect categorical columns (< 5% unique values)
CATEGORICAL_UNIQUENESS_THRESHOLD = 0.05

# Minimum valid date ratio to classify column as date type (> 80% valid)
DATE_DETECTION_THRESHOLD = 0.8

# Strict mode for date detection (reject numeric-looking values)
# When True, values that look like numeric IDs will not be classified as dates
STRICT_DATE_DETECTION = False

# Minimum ratio of non-numeric characters required to consider as date
# Used in strict mode to reject pure numeric strings (e.g., "20210115" could be ID or date)
DATE_NON_NUMERIC_RATIO = 0.2

# Average string length threshold to classify as text vs string (> 50 chars)
TEXT_LENGTH_THRESHOLD = 50

# =============================================================================
# DATA PROFILING - SAMPLING & LIMITS
# =============================================================================

# Sample size for type detection analysis
TYPE_DETECTION_SAMPLE_SIZE = 100

# Maximum number of sample values to store for categorical columns
MAX_SAMPLE_VALUES = 10

# Maximum number of unique values to store for low-cardinality columns
MAX_UNIQUE_VALUES_DISPLAY = 10

# =============================================================================
# DATA PROFILING - QUALITY SCORING
# =============================================================================

# Weight for null penalty in quality score calculation (60%)
QUALITY_SCORE_NULL_WEIGHT = 0.6

# Weight for uniqueness in quality score calculation (40%)
QUALITY_SCORE_UNIQUENESS_WEIGHT = 0.4

# Sweet spot for uniqueness ratio (50% unique is ideal for non-ID columns)
UNIQUENESS_SWEET_SPOT = 0.5

# =============================================================================
# DATA PROFILING - EXPECTATION GENERATION
# =============================================================================

# Default null tolerance percentage for generating completeness expectations
DEFAULT_NULL_TOLERANCE = 5.0

# Null percentage threshold below which to generate uniqueness checks for IDs
ID_NULL_THRESHOLD_FOR_UNIQUENESS = 50

# =============================================================================
# DATA LOADING - MEMORY PROTECTION
# =============================================================================

# File size threshold in MB to trigger auto-sampling
LARGE_FILE_SIZE_MB = 500

# Default number of rows to auto-sample for large files
DEFAULT_AUTO_SAMPLE_ROWS = 100000

# =============================================================================
# FABRIC INTEGRATION
# =============================================================================

# Maximum bytes to read for configuration files in Fabric
FABRIC_CONFIG_MAX_BYTES = 1000000

# Row count threshold for auto-sampling in Fabric
FABRIC_LARGE_DATASET_THRESHOLD = 100000

# Sample fraction for large datasets in Fabric (10%)
FABRIC_SAMPLE_FRACTION = 0.1

# Maximum number of failed expectations to display in failure handling
MAX_FAILURE_DISPLAY = 10

# =============================================================================
# STORAGE DEFAULTS
# =============================================================================

# Default directory for local validation results storage
DEFAULT_RESULTS_DIR = "dq_results"

# Default Lakehouse path for Fabric validation results storage
DEFAULT_FABRIC_RESULTS_DIR = "Files/dq_results"
