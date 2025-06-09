from typing import List, Dict

try:
    import lxml.html  # type: ignore
    HAS_LXML = True
except ImportError:  # pragma: no cover - optional dependency
    HAS_LXML = False

def extract_xpath_mbh(html: str, xpaths: List[str]) -> Dict[str, List[str]]:
    """
    Extracts text content from HTML using a list of XPath expressions.
    Requires the 'lxml' package to be installed.
    Adapted from mono_basic_html.BasicHTMLScraper.extract_xpath.

    Args:
        html: The HTML content as a string.
        xpaths: A list of XPath expression strings.

    Returns:
        A dictionary where keys are the XPath expressions and values are lists
        of extracted text strings for each node found by that expression.

    Raises:
        RuntimeError: If 'lxml' package is not installed.
    """
    if not HAS_LXML:
        raise RuntimeError("XPath extraction requires the 'lxml' package. Please install it (e.g., pip install lxml).")

    try:
        tree = lxml.html.fromstring(html)
    except Exception as e: # Catch potential errors during parsing, e.g. badly formed HTML for lxml
        # print(f"Error parsing HTML with lxml: {e}", file=sys.stderr) # Optional: log error
        # Depending on desired behavior, could return empty results or re-raise
        return {xp: [] for xp in xpaths} # Return empty results for all xpaths if parsing fails

    results: Dict[str, List[str]] = {}
    for xp in xpaths:
        try:
            nodes = tree.xpath(xp)
            texts: List[str] = []
            for node in nodes:
                if isinstance(node, str): # lxml.etree._ElementUnicodeResult
                    texts.append(node.strip())
                elif hasattr(node, "text_content"): # For elements
                    texts.append(node.text_content().strip())
                else: # For attributes or other node types that convert to string
                    texts.append(str(node).strip())
            results[xp] = texts
        except Exception as e: # Catch errors during XPath evaluation for a specific expression
            # print(f"Error evaluating XPath '{xp}': {e}", file=sys.stderr) # Optional: log error
            results[xp] = [] # Return empty list for this failing XPath

    return results
