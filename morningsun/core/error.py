
class MorningSunError(Exception):
    pass

class APIError(MorningSunError):
    pass

class ValidationError(MorningSunError):
    pass
    
class ParamsInvalidError(MorningSunError):
    pass

class NumberOfQueryError(MorningSunError):
    pass

class APIConnectionError(MorningSunError):
    pass

class SendError(MorningSunError):
    pass