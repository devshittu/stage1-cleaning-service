"""
src/utils/json_sanitizer.py

Working JSON sanitization that handles unescaped quotes in strings.
NO CLASS DEPENDENCIES - pure functions only.
"""

import json
import re
import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger("ingestion_service")


def sanitize_and_parse_json(json_string: str, line_number: int = 0) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Parse JSON with multiple fallback strategies.

    Args:
        json_string: Raw JSON string to parse
        line_number: Line number for logging

    Returns:
        Tuple of (parsed_dict, error_message)
        - Success: (dict, None)
        - Failure: (None, error_message)
    """
    if not json_string or not json_string.strip():
        return None, "Empty line"

    original = json_string.strip()

    # Strategy 1: Direct parse (works for valid JSON)
    try:
        result = json.loads(original)
        # Post-processing: Fix malformed URLs
        result = _fix_malformed_urls(result)
        return result, None
    except json.JSONDecodeError as e:
        first_error = f"{e.msg} at position {e.pos}"
        error_pos = e.pos
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"

    # Strategy 2: Fix unescaped quotes in string values
    try:
        cleaned = _fix_unescaped_quotes(original, error_pos)
        result = json.loads(cleaned)
        result = _fix_malformed_urls(result)
        logger.info(f"Line {line_number}: Fixed unescaped quotes")
        return result, None
    except Exception as e:
        logger.debug(f"Line {line_number}: Quote fix failed: {e}")

    # Strategy 3: Fix Unicode issues
    try:
        cleaned = _fix_unicode_issues(original)
        result = json.loads(cleaned)
        result = _fix_malformed_urls(result)
        logger.info(f"Line {line_number}: Fixed Unicode issues")
        return result, None
    except Exception as e:
        logger.debug(f"Line {line_number}: Unicode fix failed: {e}")

    # Strategy 4: Combined fixes
    try:
        cleaned = _fix_unicode_issues(original)
        cleaned = _fix_unescaped_quotes(cleaned, 0)
        result = json.loads(cleaned)
        result = _fix_malformed_urls(result)
        logger.info(f"Line {line_number}: Fixed with combined strategy")
        return result, None
    except Exception as e:
        logger.debug(f"Line {line_number}: Combined fix failed: {e}")

    # Strategy 5: Aggressive field extraction
    try:
        result = _extract_fields_aggressive(original)
        if result and 'document_id' in result and 'text' in result:
            result = _fix_malformed_urls(result)
            logger.warning(f"Line {line_number}: Used aggressive extraction")
            return result, None
    except Exception as e:
        logger.debug(f"Line {line_number}: Extraction failed: {e}")

    return None, f"All parsing failed. {first_error}"


def _fix_unescaped_quotes(json_str: str, error_pos: int = 0) -> str:
    """
    Fix unescaped quotes inside JSON string values.
    
    Strategy: Parse character by character, track if we're inside a string value,
    and escape any unescaped quotes found inside string values.
    """
    if not json_str:
        return json_str

    result = []
    i = 0
    in_string = False
    in_field_name = False
    after_colon = False
    escape_next = False

    while i < len(json_str):
        char = json_str[i]

        # Handle escape sequences
        if escape_next:
            result.append(char)
            escape_next = False
            i += 1
            continue

        if char == '\\':
            result.append(char)
            escape_next = True
            i += 1
            continue

        # Handle quotes
        if char == '"':
            if not in_string:
                # Starting a string (either field name or value)
                in_string = True
                if after_colon:
                    in_field_name = False
                else:
                    in_field_name = True
                result.append(char)
            else:
                # Potentially ending a string
                # Look ahead to see if this is truly the end
                next_char_idx = i + 1

                # Skip whitespace
                while next_char_idx < len(json_str) and json_str[next_char_idx] in ' \t\n\r':
                    next_char_idx += 1

                # Check what comes after
                if next_char_idx < len(json_str):
                    next_char = json_str[next_char_idx]

                    if in_field_name and next_char == ':':
                        # This is the end of a field name
                        in_string = False
                        in_field_name = False
                        after_colon = True
                        result.append(char)
                    elif next_char in ',}':
                        # This is the end of a value
                        in_string = False
                        after_colon = False
                        result.append(char)
                    else:
                        # This quote is inside the string value - escape it!
                        result.append('\\')
                        result.append(char)
                else:
                    # End of JSON string
                    in_string = False
                    result.append(char)

        elif char == ':' and not in_string:
            after_colon = True
            result.append(char)

        elif char in ',{}' and not in_string:
            after_colon = False
            result.append(char)

        else:
            result.append(char)

        i += 1

    return ''.join(result)


def _fix_unicode_issues(text: str) -> str:
    """Fix common Unicode issues that break JSON parsing."""
    replacements = {
        '\u201c': '\\"',  # Left double quote → escaped quote
        '\u201d': '\\"',  # Right double quote → escaped quote
        '\u2018': "'",    # Left single quote
        '\u2019': "'",    # Right single quote
        '\u2014': '--',   # Em dash
        '\u2013': '-',    # En dash
        '\u2026': '...',  # Ellipsis
        '\u00a0': ' ',    # Non-breaking space
        '\u200b': '',     # Zero-width space
        '\u200c': '',     # Zero-width non-joiner
        '\u200d': '',     # Zero-width joiner
        '\ufeff': '',     # BOM
        '\u2028': ' ',    # Line separator
        '\u2029': ' ',    # Paragraph separator
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # Remove control characters except tab, newline, carriage return
    text = ''.join(c for c in text if ord(c) >= 32 or c in '\t\n\r')

    return text


def _fix_malformed_urls(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fix common URL issues in the document data.
    - Fix httpss:// → https://
    - Fix httpp:// → http://
    - Remove invalid URL schemes
    """
    if not isinstance(data, dict):
        return data

    url_fields = ['source_url', 'cleaned_source_url']

    for field in url_fields:
        if field in data and isinstance(data[field], str):
            url = data[field]
            # Fix double scheme characters
            url = url.replace('httpss://', 'https://')
            url = url.replace('httpp://', 'http://')
            url = url.replace('httpps://', 'https://')
            # Ensure it starts with a valid scheme
            if url and not url.startswith(('http://', 'https://')):
                if '://' in url:
                    # Has a scheme but it's invalid - try to fix
                    url = 'https://' + url.split('://', 1)[1]
            data[field] = url

    return data


def _extract_fields_aggressive(json_string: str) -> Optional[Dict[str, Any]]:
    """
    Last resort: Extract fields using regex patterns.
    Only extracts minimum required fields.
    """
    result = {}

    # Extract document_id
    doc_match = re.search(r'"document_id"\s*:\s*"([^"]+)"', json_string)
    if not doc_match:
        return None
    result['document_id'] = doc_match.group(1)

    # Extract text - this is tricky with embedded quotes
    # Find "text":"
    text_start_match = re.search(r'"text"\s*:\s*"', json_string)
    if text_start_match:
        start = text_start_match.end()

        # Find the end by looking for ","field" pattern or "}
        pos = start
        text_chars = []

        while pos < len(json_string):
            char = json_string[pos]

            if char == '\\' and pos + 1 < len(json_string):
                # Skip escape sequence
                text_chars.append(char)
                text_chars.append(json_string[pos + 1])
                pos += 2
                continue

            if char == '"':
                # Check if this ends the text field
                check_pos = pos + 1
                while check_pos < len(json_string) and json_string[check_pos] in ' \t\n\r':
                    check_pos += 1

                if check_pos < len(json_string) and json_string[check_pos] in ',}':
                    # Found the end!
                    break
                else:
                    # Quote inside the text
                    text_chars.append(char)
            else:
                text_chars.append(char)

            pos += 1

        result['text'] = ''.join(text_chars)

    if 'text' not in result:
        return None

    # Extract optional fields with simple regex
    for field in ['title', 'excerpt', 'author', 'source_url', 'publication_date']:
        match = re.search(f'"{field}"\\s*:\\s*"([^"]*)"', json_string)
        if match:
            result[field] = match.group(1)

    return result


# Test function for development
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Test with problematic line
    test = '{"document_id":"test","text":"He said "hello" to me"}'
    result, error = sanitize_and_parse_json(test, 1)

    if result:
        print(f"✅ SUCCESS: {result}")
    else:
        print(f"❌ FAILED: {error}")

# src/utils/json_sanitizer.py
