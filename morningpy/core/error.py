"""
Custom exceptions for the Morningpy package.

This module defines a hierarchy of exceptions for handling various error conditions
in the Morningpy API client and data extraction classes.
"""


class MorningpyError(Exception):
    """
    Base exception for all Morningpy package errors.
    
    All custom exceptions in the package inherit from this class,
    allowing users to catch all package-specific errors with a single except clause.
    
    Examples
    --------
    >>> try:
    ...     # Some Morningpy operation
    ...     pass
    ... except MorningpyError as e:
    ...     print(f"Morningpy error occurred: {e}")
    """
    pass


class ValidationError(MorningpyError):
    """
    Raised when input validation fails.
    
    This exception is raised when user-provided parameters fail validation checks,
    such as invalid dates, out-of-range values, or incompatible parameter combinations.
    
    Examples
    --------
    >>> raise ValidationError("Date must be in YYYY-MM-DD format")
    >>> raise ValidationError("Invalid security_type: must be 'stock', 'fund', or 'etf'")
    """
    pass


class ParamsInvalidError(ValidationError):
    """
    Raised when API parameters are invalid or missing.
    
    This is a specialized validation error for API request parameters that don't
    meet the API's requirements or constraints.
    
    Examples
    --------
    >>> raise ParamsInvalidError("Required parameter 'security_id' is missing")
    >>> raise ParamsInvalidError("Parameter 'page_size' must be between 1 and 100")
    """
    pass


class APIError(MorningpyError):
    """
    Base exception for API-related errors.
    
    Raised when the API returns an error response or encounters an issue
    during the request/response cycle.
    
    Attributes
    ----------
    status_code : int, optional
        HTTP status code from the API response.
    response : dict, optional
        Full API response if available.
    
    Examples
    --------
    >>> raise APIError("API returned error: Unauthorized", status_code=401)
    """
    def __init__(self, message: str, status_code: int = None, response: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class APIConnectionError(APIError):
    """
    Raised when unable to connect to the API.
    
    This exception is raised for network-related issues such as connection timeouts,
    DNS failures, or unreachable endpoints.
    
    Examples
    --------
    >>> raise APIConnectionError("Failed to connect to api.morningstar.com: Connection timeout")
    >>> raise APIConnectionError("Network unreachable")
    """
    pass


class APIResponseError(APIError):
    """
    Raised when API returns an unexpected or malformed response.
    
    This includes cases where the API returns a success status code but the
    response body is missing expected fields or has an invalid structure.
    
    Examples
    --------
    >>> raise APIResponseError("Expected 'data' field not found in response")
    >>> raise APIResponseError("Invalid JSON response")
    """
    pass


class RateLimitError(APIError):
    """
    Raised when API rate limit is exceeded.
    
    Attributes
    ----------
    retry_after : int, optional
        Number of seconds to wait before retrying (from Retry-After header).
    
    Examples
    --------
    >>> raise RateLimitError("Rate limit exceeded", retry_after=60)
    """
    def __init__(self, message: str, retry_after: int = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class AuthenticationError(APIError):
    """
    Raised when API authentication fails.
    
    This exception is raised for 401 Unauthorized or 403 Forbidden responses,
    indicating invalid or missing credentials.
    
    Examples
    --------
    >>> raise AuthenticationError("Invalid bearer token")
    >>> raise AuthenticationError("API key expired")
    """
    pass


class NumberOfQueryError(MorningpyError):
    """
    Raised when the number of queries exceeds allowed limits.
    
    This exception is raised when batch operations or requests exceed the
    maximum number of queries allowed per request or time period.
    
    Attributes
    ----------
    max_queries : int, optional
        Maximum number of queries allowed.
    current_queries : int, optional
        Number of queries attempted.
    
    Examples
    --------
    >>> raise NumberOfQueryError(
    ...     "Maximum 100 securities per batch request",
    ...     max_queries=100,
    ...     current_queries=150
    ... )
    """
    def __init__(self, message: str, max_queries: int = None, current_queries: int = None):
        super().__init__(message)
        self.max_queries = max_queries
        self.current_queries = current_queries


class DataNotFoundError(MorningpyError):
    """
    Raised when requested data is not available.
    
    This exception is raised when a valid request returns no data, such as
    querying for a non-existent security or a date range with no records.
    
    Examples
    --------
    >>> raise DataNotFoundError("No data found for security_id: INVALID123")
    >>> raise DataNotFoundError("No price history available for date range")
    """
    pass


class DataProcessingError(MorningpyError):
    """
    Raised when data processing or transformation fails.
    
    This exception is raised during internal data processing operations, such as
    parsing responses, transforming data structures, or applying calculations.
    
    Examples
    --------
    >>> raise DataProcessingError("Failed to parse date field: invalid format")
    >>> raise DataProcessingError("Unable to calculate returns: missing price data")
    """
    pass


class ConfigurationError(MorningpyError):
    """
    Raised when configuration is invalid or missing.
    
    This exception is raised for issues with package configuration, such as
    missing required settings or invalid configuration values.
    
    Examples
    --------
    >>> raise ConfigurationError("API_URL not configured")
    >>> raise ConfigurationError("Invalid authentication type in config")
    """
    pass