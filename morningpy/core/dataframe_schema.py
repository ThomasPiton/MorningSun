from dataclasses import dataclass
from typing import Optional, Dict, get_type_hints

@dataclass
class DataFrameSchema:
    """Base schema for DataFrame validation"""
    
    def to_dtype_dict(self) -> Dict[str, type]:
        """Convert dataclass fields to pandas dtypes"""
        type_hints = get_type_hints(self)
        dtype_map = {
            int: 'Int64',  # Nullable integer
            float: 'float64',
            str: 'string',  # Nullable string
            bool: 'boolean',
            Optional[int]: 'Int64',
            Optional[float]: 'float64',
            Optional[str]: 'string',
            Optional[bool]: 'boolean',
        }
        
        dtypes = {}
        for field_name, field_type in type_hints.items():
            origin = getattr(field_type, '__origin__', None)
            if origin is type(None) or str(field_type).startswith('Optional'):
                args = getattr(field_type, '__args__', ())
                field_type = args[0] if args else field_type
            
            dtypes[field_name] = dtype_map.get(field_type, 'object')
        
        return dtypes