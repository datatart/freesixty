import os
import pytest
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import freesixty

def test_query_splitting():
    pass


def test_data_storing():
    pass


def test_initialize_analyticsreporting():
    pass


def test___exists():
    filename_1 = 'file://' + os.path.join(os.path.dirname(__file__), __file__)
    filename_2 = 'filename:///nonexistent_file'
    filename_3 = 'file:///nonexistent_file'
    filename_4 = 's3:///nonexistent_file'

    assert freesixty._exists(filename_1)

    with pytest.raises(NotImplementedError):
        freesixty._exists(filename_2)

    assert not freesixty._exists(filename_3)

    with pytest.raises(ValueError):
        freesixty._exists(filename_4)
