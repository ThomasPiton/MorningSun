from morningpy.core.auth import AuthType

class NewsConfig:
    REQUIRED_AUTH: AuthType = AuthType.BEARER_TOKEN
    API_URL = ""
    PARAMS = {}
    VALID_FREQUENCY = {}
    MAPPING_FREQUENCY = {}
    RENAME_COLUMNS = {}
    STRING_COLUMNS = []
    NUMERIC_COLUMNS = []
    FINAL_COLUMNS = STRING_COLUMNS + NUMERIC_COLUMNS