import re
def isAlphanumeric(string):
    """
    Checks if a string is alphanumeric using regex.
    
    :param string: The string to check.
    :return: True if the string is alphanumeric, False otherwise.
    """
    return bool(re.fullmatch(r"[a-zA-Z0-9]+", string))

def isAlphanumericWithQuotesAndUnderscoreAndDots(string):
    return bool(re.fullmatch(r'[a-zA-Z0-9\'\"._\s]+', string))
