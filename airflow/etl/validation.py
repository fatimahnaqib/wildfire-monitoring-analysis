"""
Data validation module for wildfire records.

This module provides validation functions for wildfire data records
from the NASA FIRMS API to ensure data quality and consistency.
"""

import logging
from typing import Dict, Any, List, Set

# Configure logging
logger = logging.getLogger(__name__)

# Validation constants
REQUIRED_FIELDS: List[str] = [
    'latitude', 'longitude', 'bright_ti4', 'bright_ti5', 'scan', 'track',
    'acq_date', 'acq_time', 'satellite', 'confidence', 'version', 'frp', 'daynight'
]

VALID_CONFIDENCE: Set[str] = {"n", "h", "l"}
VALID_DAYNIGHT: Set[str] = {"D", "N"}
VALID_SATELLITES: Set[str] = {"N", "N20", "N21"}

FLOAT_FIELDS: List[str] = [
    'latitude', 'longitude', 'bright_ti4', 'bright_ti5', 
    'scan', 'track', 'frp'
]


class ValidationError(Exception):
    """Custom exception for validation errors."""


def validate_required_fields(record: Dict[str, Any]) -> bool:
    """
    Validate that all required fields are present and not null.
    
    Args:
        record: Dictionary containing wildfire record data
        
    Returns:
        bool: True if all required fields are present and valid
        
    Raises:
        ValidationError: If required fields are missing or null
    """
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] in (None, '', 'null'):
            error_msg = f"Missing or null field: {field}"
            logger.warning(error_msg)
            raise ValidationError(error_msg)
    return True


def validate_numeric_fields(record: Dict[str, Any]) -> bool:
    """
    Validate that numeric fields can be converted to float.
    
    Args:
        record: Dictionary containing wildfire record data
        
    Returns:
        bool: True if all numeric fields are valid
        
    Raises:
        ValidationError: If numeric conversion fails
    """
    for field in FLOAT_FIELDS:
        try:
            float(record[field])
        except (ValueError, TypeError):
            error_msg = f"Invalid numeric value for field '{field}': {record[field]}"
            logger.warning(error_msg)
            raise ValidationError(error_msg)
    return True


def validate_enum_fields(record: Dict[str, Any]) -> bool:
    """
    Validate that enum fields have valid values.
    
    Args:
        record: Dictionary containing wildfire record data
        
    Returns:
        bool: True if all enum fields are valid
        
    Raises:
        ValidationError: If enum values are invalid
    """
    # Validate confidence
    if record['confidence'].lower() not in VALID_CONFIDENCE:
        error_msg = f"Invalid confidence value: {record['confidence']}"
        logger.warning(error_msg)
        raise ValidationError(error_msg)
    
    # Validate daynight
    if record['daynight'] not in VALID_DAYNIGHT:
        error_msg = f"Invalid daynight value: {record['daynight']}"
        logger.warning(error_msg)
        raise ValidationError(error_msg)
    
    # Validate satellite
    if record['satellite'] not in VALID_SATELLITES:
        error_msg = f"Invalid satellite: {record['satellite']}"
        logger.warning(error_msg)
        raise ValidationError(error_msg)
    
    return True


def is_valid_record(record: Dict[str, Any]) -> bool:
    """
    Validate a complete wildfire record.
    
    This function performs comprehensive validation including:
    - Required field presence
    - Numeric field conversion
    - Enum value validation
    
    Args:
        record: Dictionary containing wildfire record data
        
    Returns:
        bool: True if record is valid, False otherwise
        
    Note:
        This function catches ValidationError exceptions and returns False
        for invalid records, making it safe for use in data processing pipelines.
    """
    try:
        validate_required_fields(record)
        validate_numeric_fields(record)
        validate_enum_fields(record)
        return True
    except ValidationError as e:
        logger.warning(f"Record validation failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        return False


def get_validation_summary(records: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get a summary of validation results for a batch of records.
    
    Args:
        records: List of wildfire record dictionaries
        
    Returns:
        Dict containing validation statistics
    """
    total_records = len(records)
    valid_records = sum(1 for record in records if is_valid_record(record))
    invalid_records = total_records - valid_records
    
    return {
        'total_records': total_records,
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'validation_rate': valid_records / total_records if total_records > 0 else 0
    }
