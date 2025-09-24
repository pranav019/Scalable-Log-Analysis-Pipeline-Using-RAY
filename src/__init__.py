"""
src package initializer for the scalable log analysis
In Python, a folder becomes a package if it contains an __init__.py file.
Not strictly necessary, but very helpful for readability and documentation.

Without __init__.py, Python wouldn’t know src is a package.
"""

# __all__ is a special variable in Python that controls what gets imported when someone writes:

__all__ = ["data_processing", "core_logic", "utils"]
