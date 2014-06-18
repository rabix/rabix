import os


def insert_suffix(file_name, suffix):
    """
    Add a suffix before extension.

    >>> insert_suffix('file.txt', 'trimmed')
    'file.trimmed.txt'
    >>> insert_suffix('/path/to/file.trimmed.txt', 'sorted')
    'file.trimmed.sorted.txt'
    """
    name = os.path.basename(file_name)
    base, ext = os.path.splitext(name)
    return ''.join([base, '.', suffix, ext])


def change_ext(file_name, ext):
    """
    Change file extension.
    >>> change_ext('file.txt', 'csv')
    'file.csv'
    >>> change_ext('/path/to/file', 'txt')
    'file.txt'
    """
    name = os.path.basename(file_name)
    base, old_ext = os.path.splitext(name)
    return ''.join([base, '.', ext])
