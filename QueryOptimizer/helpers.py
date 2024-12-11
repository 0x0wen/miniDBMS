import re
def isAlphanumeric(string):
    """
    Checks if a string is alphanumeric or a floating-point number using regex.
    
    :param string: The string to check.
    :return: True if the string is alphanumeric or a float, False otherwise.
    """
    return bool(re.fullmatch(r"[a-zA-Z0-9]+|[0-9]+\.[0-9]+", string))

def isAlphanumericWithQuotes(string):
    """
    Checks if a string is alphanumeric, a floating-point number, or contains quotes.
    
    :param string: The string to check.
    :return: True if the string matches the pattern, False otherwise.
    """
    return bool(re.fullmatch(r"[a-zA-Z0-9'\"]+|[0-9]+\.[0-9]+", string))