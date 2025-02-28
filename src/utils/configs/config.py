"""
config.py

This module provides a singleton-based configuration manager that reads settings from a `config.json` file.

The `_Config` class loads the configuration only once and provides access to critical settings
such as version, repository link, user agent, and contact email. It handles errors gracefully,
falling back to default values or raising exceptions for missing critical fields.

Usage Example:
    from config import CONFIG

    print(CONFIG.VERSION)  # Retrieves the version from config.json
    print(CONFIG.REPO_LINK)  # Retrieves the repository link
    print(CONFIG.USER_AGENT)  # Retrieves the user agent string
    print(CONFIG.CONTACT_EMAIL)  # Retrieves the contact email, or uses an environment variable fallback

Logging:
    - Errors related to missing or corrupted `config.json` are logged in `logs/config/confg.log`.

"""


import os
import json
from pathlib import Path
import logging
from loggers import LoggerManager

_logger = LoggerManager(Path("logs", "config"), level=logging.WARNING).get_logger("config.log")

class _Config:
    """
    Singleton configuration manager that loads settings from a `config.json` file.

    This class implements lazy loading, meaning the configuration is only read from the file
    when it is first accessed. It ensures that the configuration is loaded only once per runtime.

    Attributes:
        _config (dict): A class-level cache for storing configuration values.
    
    Methods:
        _load_config(): Loads configuration from `config.json`, handling errors gracefully.
        _get(key, default=None): Fetches a configuration value, ensuring the config is loaded first.
    
    Properties:
        VERSION: Retrieves the version from the config file.
        REPO_LINK: Retrieves the repository link from the config file.
        USER_AGENT: Retrieves the user agent string.
        CONTACT_EMAIL: Retrieves the contact email, with an optional environment variable fallback.
    """

    _config = None  # Lazy loading (only loads when needed)

    def _load_config(self):
        """
        Loads the configuration from `config.json` only once.

        If the file is missing or corrupted, it logs an error and defaults to an empty dictionary.
        Ensures that required fields ("version", "repo_link", "user_agent") are present, raising
        a `ValueError` if they are missing.
        """
        if _Config._config is None:  # Ensure it's loaded only once
            try:
                with open("config.json", "r") as _Config_file:
                    _Config._config = json.load(_Config_file)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                _logger.error(f"⚠️ Config file error: {e}")
                _Config._config = {}  # Use empty dict if file is missing or corrupted
            
            missing = [key for key in ["version", "repo_link", "user_agent"] if not self._get(key)]
            if missing:
                raise ValueError(f"🚨 Config is missing critical fields: {', '.join(missing)}")

    def _get(self, key, default=None):
        """
        Retrieves a configuration value from the loaded JSON file.

        Args:
            key (str): The configuration key to retrieve.
            default (optional): The default value to return if the key is missing.

        Returns:
            The value associated with `key`, or `default` if the key is not found.
        """
        self._load_config()
        return _Config._config.get(key, default)

    @property
    def VERSION(self):
        """Returns the software version from `config.json`."""
        return self._get("version", "1.0")

    @property
    def REPO_LINK(self):
        """Returns the repository link from `config.json`."""
        return self._get("repo_link", "https://github.com/MarikTik/crypto-history")

    @property
    def USER_AGENT(self):
        """Returns the user agent string from `config.json`."""
        return self._get("user_agent", "CoinbaseDataFetcher")

    @property
    def CONTACT_EMAIL(self):
        """Returns the contact email from `config.json`, or falls back to the `EMAIL` environment variable."""
        return self._get("email", os.getenv("EMAIL", ""))

# Create a singleton instance of the configuration
CONFIG = _Config()
