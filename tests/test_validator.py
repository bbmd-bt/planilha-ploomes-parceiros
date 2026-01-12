import sys
sys.path.insert(0, 'src')
from validator import is_valid_cnj, is_valid_email, is_valid_phone

def test_is_valid_cnj():
    assert is_valid_cnj('12345678901234567890') == True
    assert is_valid_cnj('123456789') == False
    assert is_valid_cnj(None) == False

def test_is_valid_email():
    assert is_valid_email('test@example.com') == True
    assert is_valid_email('invalid') == False

def test_is_valid_phone():
    assert is_valid_phone('(11) 99999-9999') == True
    assert is_valid_phone('123') == False

if __name__ == '__main__':
    test_is_valid_cnj()
    test_is_valid_email()
    test_is_valid_phone()
    print('All tests passed')
