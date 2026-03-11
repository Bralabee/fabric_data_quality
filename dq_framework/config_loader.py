"""
Configuration Loader
====================

Loads and validates YAML configuration files for data quality checks.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def _validate_alerts_section(data: Any, section_name: str = "alerts") -> None:
    """Validate the optional alerts configuration section."""
    if not isinstance(data, dict):
        raise ValueError(f"'{section_name}' must be a dict, got {type(data).__name__}")
    if "channels" in data:
        if not isinstance(data["channels"], list):
            raise ValueError(f"'{section_name}.channels' must be a list, got {type(data['channels']).__name__}")
        for i, channel in enumerate(data["channels"]):
            if not isinstance(channel, dict) or "type" not in channel:
                raise ValueError(f"'{section_name}.channels[{i}]' must be a dict with a 'type' key")


def _validate_history_section(data: Any, section_name: str = "history") -> None:
    """Validate the optional history configuration section."""
    if not isinstance(data, dict):
        raise ValueError(f"'{section_name}' must be a dict, got {type(data).__name__}")
    if "retention_days" in data:
        val = data["retention_days"]
        if not isinstance(val, int) or val <= 0:
            raise ValueError(f"'{section_name}.retention_days' must be a positive integer, got {val!r}")


def _validate_schema_tracking_section(data: Any, section_name: str = "schema_tracking") -> None:
    """Validate the optional schema_tracking configuration section."""
    if not isinstance(data, dict):
        raise ValueError(f"'{section_name}' must be a dict, got {type(data).__name__}")


OPTIONAL_SECTION_VALIDATORS = {
    "alerts": _validate_alerts_section,
    "history": _validate_history_section,
    "schema_tracking": _validate_schema_tracking_section,
}


class ConfigLoader:
    """
    Loads and validates YAML configuration files for data quality.

    Example:
        >>> loader = ConfigLoader()
        >>> config = loader.load('config/my_checks.yml')
        >>> print(config['expectations'])
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load(self, config_path: Any) -> Any:
        """
        Load configuration from YAML file, dictionary, or list of them.

        Args:
            config_path: Path to YAML configuration file, configuration dictionary, or list of them

        Returns:
            Dictionary containing configuration or list of configurations

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid
        """
        if isinstance(config_path, list):
            return [self.load(path) for path in config_path]

        if isinstance(config_path, dict):
            config = config_path
            self.validate(config)
            return config

        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            with open(config_file) as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file: {e}") from e

        # Validate configuration structure
        self.validate(config)

        self.logger.info(f"Loaded configuration from {config_path}")
        return config

    def validate(self, config: dict[str, Any]) -> None:
        """
        Validate that configuration has required structure.

        Args:
            config: Configuration dictionary

        Raises:
            ValueError: If configuration is invalid
        """
        # Check for validation_name
        if "validation_name" not in config:
            raise ValueError("Configuration must contain 'validation_name' key")

        # Check for expectations
        if "expectations" not in config:
            raise ValueError("Configuration must contain 'expectations' key")

        if not isinstance(config["expectations"], list):
            raise ValueError("'expectations' must be a list")

        # Validate each expectation
        for i, expectation in enumerate(config["expectations"]):
            if "expectation_type" not in expectation:
                raise ValueError(f"Expectation {i} missing 'expectation_type'")

            if "kwargs" not in expectation:
                raise ValueError(f"Expectation {i} missing 'kwargs'")

        # Validate optional sections if present
        for section_name, validator_fn in OPTIONAL_SECTION_VALIDATORS.items():
            if section_name in config:
                validator_fn(config[section_name], section_name=section_name)

        self.logger.debug(f"Configuration validated: {len(config['expectations'])} expectations")

    def load_multiple(self, config_paths: list) -> dict[str, dict[str, Any]]:
        """
        Load multiple configuration files.

        Args:
            config_paths: List of paths to config files

        Returns:
            Dictionary mapping file names to configurations
        """
        configs = {}
        for path in config_paths:
            config = self.load(path)
            configs[Path(path).stem] = config

        return configs

    def merge_configs(self, config_paths: list) -> dict[str, Any]:
        """
        Merge multiple configuration files into one.

        Args:
            config_paths: List of paths to config files

        Returns:
            Merged configuration dictionary
        """
        merged_config = {
            "validation_name": "merged_validation",
            "expectations": [],
            "data_source": {},
        }

        for path in config_paths:
            config = self.load(path)

            # Merge expectations
            if "expectations" in config:
                merged_config["expectations"].extend(config["expectations"])

            # Keep first data_source info
            if "data_source" in config and not merged_config["data_source"]:
                merged_config["data_source"] = config["data_source"]

        self.logger.info(f"Merged {len(config_paths)} configurations")
        return merged_config

    @staticmethod
    def validate_yaml_syntax(config_path: str) -> bool:
        """
        Check if YAML file has valid syntax without loading it.

        Args:
            config_path: Path to YAML file

        Returns:
            True if valid, False otherwise
        """
        try:
            with open(config_path) as f:
                yaml.safe_load(f)
            return True
        except (yaml.YAMLError, FileNotFoundError):
            return False
