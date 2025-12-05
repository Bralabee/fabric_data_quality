"""
Configuration Loader
====================

Loads and validates YAML configuration files for data quality checks.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


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
    
    def load(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
        
        Returns:
            Dictionary containing configuration
        
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file: {e}")
        
        # Validate configuration structure
        self._validate_config(config)
        
        self.logger.info(f"Loaded configuration from {config_path}")
        return config
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate that configuration has required structure.
        
        Args:
            config: Configuration dictionary
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Check for expectations
        if 'expectations' not in config:
            raise ValueError("Configuration must contain 'expectations' key")
        
        if not isinstance(config['expectations'], list):
            raise ValueError("'expectations' must be a list")
        
        # Validate each expectation
        for i, expectation in enumerate(config['expectations']):
            if 'expectation_type' not in expectation:
                raise ValueError(f"Expectation {i} missing 'expectation_type'")
            
            if 'kwargs' not in expectation:
                raise ValueError(f"Expectation {i} missing 'kwargs'")
        
        self.logger.debug(f"Configuration validated: {len(config['expectations'])} expectations")
    
    def load_multiple(self, config_paths: list) -> Dict[str, Dict[str, Any]]:
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
    
    def merge_configs(self, config_paths: list) -> Dict[str, Any]:
        """
        Merge multiple configuration files into one.
        
        Args:
            config_paths: List of paths to config files
        
        Returns:
            Merged configuration dictionary
        """
        merged_config = {
            'expectations': [],
            'data_source': {}
        }
        
        for path in config_paths:
            config = self.load(path)
            
            # Merge expectations
            if 'expectations' in config:
                merged_config['expectations'].extend(config['expectations'])
            
            # Keep first data_source info
            if 'data_source' in config and not merged_config['data_source']:
                merged_config['data_source'] = config['data_source']
        
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
            with open(config_path, 'r') as f:
                yaml.safe_load(f)
            return True
        except (yaml.YAMLError, FileNotFoundError):
            return False
