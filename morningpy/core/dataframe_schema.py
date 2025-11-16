from dataclasses import dataclass
from typing import get_type_hints, Dict, Optional

@dataclass
class DataFrameSchema:
    """
    Base schema for DataFrame validation.
    
    This class is designed to be inherited and used to define the expected 
    structure of a pandas DataFrame. Each dataclass field represents a 
    DataFrame column and its annotated Python type is mapped to a pandas dtype.
    
    Supported mappings:
        - int  -> 'Int64'
        - float -> 'float64'
        - str -> 'string'
        - bool -> 'boolean'
        - Optional[...] variants map to the same dtypes but allow missing values.
        
    Any unsupported type defaults to pandas 'object' dtype.
    """

    def to_dtype_dict(self) -> Dict[str, type]:
        """
        Convert annotated dataclass fields to a pandas-compatible dtype dictionary.

        Returns
        -------
        Dict[str, type]
            A mapping of field names (DataFrame column names) to pandas dtype strings.
            
        Notes
        -----
        - Optional[...] types are resolved to their underlying type.
        - Unknown or complex types return 'object'.
        """
        
        type_hints = get_type_hints(self)
        
        dtype_map = {
            int: 'Int64',
            float: 'float64',
            str: 'string',
            bool: 'boolean',
            Optional[int]: 'Int64',
            Optional[float]: 'float64',
            Optional[str]: 'string',
            Optional[bool]: 'boolean',
        }
        
        dtypes = {}
        for field_name, field_type in type_hints.items():
            origin = getattr(field_type, '__origin__', None)

            # Handle Optional[...] types
            if origin is type(None) or str(field_type).startswith('Optional'):
                args = getattr(field_type, '__args__', ())
                field_type = args[0] if args else field_type
            
            dtypes[field_name] = dtype_map.get(field_type, 'object')
        
        return dtypes
