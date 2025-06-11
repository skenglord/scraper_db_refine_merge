import re
import html
from typing import Optional

def normalize_whitespace(text: Optional[str]) -> Optional[str]:
    """
    Normalizes whitespace in a string.
    - Strips leading/trailing whitespace.
    - Replaces multiple internal whitespace characters (spaces, tabs, newlines)
      with a single space.
    - Returns None if the input is None.
    """
    if text is None:
        return None

    text = text.strip()
    text = re.sub(r'\s+', ' ', text) # \s matches any whitespace char (space, tab, newline, etc.)

    return text if text else None # Return None if text becomes empty after stripping

def clean_html_entities(text: Optional[str]) -> Optional[str]:
    """
    Converts HTML character entities (e.g., &amp;, &lt;, &nbsp;) in a string
    to their corresponding Unicode characters.
    - Returns None if the input is None.
    """
    if text is None:
        return None

    # html.unescape handles named and numeric character references.
    # It also handles entities like &nbsp; correctly (converts to a normal space).
    return html.unescape(text)

def clean_and_normalize_text(text: Optional[str]) -> Optional[str]:
    """
    Applies a sequence of cleaning operations: HTML entity decoding and whitespace normalization.
    """
    if text is None:
        return None

    cleaned_text = clean_html_entities(text)
    if cleaned_text is None: # Should not happen if input text was not None
        return None

    normalized_text = normalize_whitespace(cleaned_text)
    return normalized_text

# Example Usage (can be run directly for testing)
if __name__ == "__main__":
    test_strings = [
        "  Hello \n\t World  ",
        "Text&nbsp;with&amp;entities&lt;&gt;&#39;&quot;",
        "  Multiple   \n   spaces and \t tabs  ",
        None,
        "Already clean",
        "   ", # Just spaces
        "&lt;p&gt;Some HTML content&lt;/p&gt;"
    ]

    print("--- normalize_whitespace ---")
    for s in test_strings:
        print(f"Original: '{s}' -> Normalized: '{normalize_whitespace(s)}'")

    print("\n--- clean_html_entities ---")
    for s in test_strings:
        print(f"Original: '{s}' -> Cleaned Entities: '{clean_html_entities(s)}'")

    print("\n--- clean_and_normalize_text ---")
    for s in test_strings:
        print(f"Original: '{s}' -> Cleaned & Normalized: '{clean_and_normalize_text(s)}'")

    # Test case for empty string after normalization
    empty_after_strip = "   \n\t   "
    print(f"Original: '{empty_after_strip}' -> Cleaned & Normalized: '{clean_and_normalize_text(empty_after_strip)}' (Expected: None)")
```
