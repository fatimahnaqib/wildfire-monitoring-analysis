import logging

REQUIRED_FIELDS = [
    'latitude', 'longitude', 'bright_ti4', 'bright_ti5', 'scan', 'track',
    'acq_date', 'acq_time', 'satellite', 'confidence', 'version', 'frp', 'daynight'
]

VALID_CONFIDENCE = {"n", "h", "l"}
VALID_DAYNIGHT = {"D", "N"}
VALID_SATELLITES = {"N", "N20", "N21"}

def is_valid_record(record):
    try:
        # Ensure required fields are present
        for field in REQUIRED_FIELDS:
            if field not in record or record[field] in (None, '', 'null'):
                logging.warning(f"Missing or null field: {field}")
                return False

        # Validate types
        float_fields = ['latitude', 'longitude', 'bright_ti4', 'bright_ti5', 'scan', 'track', 'frp']
        for f in float_fields:
            float(record[f])  # Will raise ValueError if not convertible

        # Validate enums
        if record['confidence'].lower() not in VALID_CONFIDENCE:
            logging.warning(f"Invalid confidence value: {record['confidence']}")
            return False

        if record['daynight'] not in VALID_DAYNIGHT:
            logging.warning(f"Invalid daynight value: {record['daynight']}")
            return False

        if record['satellite'] not in VALID_SATELLITES:
            logging.warning(f"Invalid satellite: {record['satellite']}")
            return False

        return True

    except Exception as e:
        logging.warning(f"Validation error: {e}")
        return False
