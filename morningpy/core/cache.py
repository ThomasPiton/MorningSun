import json
import os
from typing import Optional, Any


class Cache:
    """
    Persistent cache for storing authentication data (apikey, maas_token, waf_token).
    
    The cache is stored in a JSON file at morningpy/core/cache.json.
    On each save, previous values are replaced.
    """

    def __init__(self, cache_filename: str = "cache.json"):
        # Locate the cache file in the morningpy/core/ directory
        base_dir = os.path.join(os.path.dirname(__file__))
        os.makedirs(base_dir, exist_ok=True)
        self.cache_path = os.path.join(base_dir, cache_filename)
        self._cache = self._load_cache()

    def _load_cache(self) -> dict:
        """Load existing cache from JSON file."""
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                # If corrupted, reset
                return {}
        return {}

    def _save_cache(self):
        """Write current cache to disk, overwriting old data."""
        with open(self.cache_path, "w") as f:
            json.dump(self._cache, f, indent=2)

    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache."""
        return self._cache.get(key)

    def set(self, key: str, value: Any):
        """Store or update a key-value pair in the cache."""
        if value:  # only store if not empty
            self._cache[key] = value
            self._save_cache()

    def clear(self):
        """Clear all cached data."""
        self._cache = {}
        self._save_cache()
