import sys
sys.path.insert(0, 'src')
from normalizers import normalize_cnj, normalize_phone, normalize_email, normalize_produto, extract_first_value

def test_normalize_cnj():
    assert normalize_cnj('12345678901234567890') == '1234567-89.0123.4.56.7890'
    assert normalize_cnj('123456789') is None

def test_normalize_phone():
    assert normalize_phone('11999999999') == '(11) 99999-9999'
    assert normalize_phone('123') is None

def test_normalize_email():
    assert normalize_email('Test@Example.Com') == 'test@example.com'
    assert normalize_email('invalid') == ''

def test_normalize_produto():
    assert normalize_produto('Honorários') == 'Honorários'
    assert normalize_produto('invalid') == 'À Definir'

def test_extract_first_value():
    assert extract_first_value('value1; value2') == 'value1'
    assert extract_first_value('') == ''

if __name__ == '__main__':
    test_normalize_cnj()
    test_normalize_phone()
    test_normalize_email()
    test_normalize_produto()
    test_extract_first_value()
    print('All tests passed')
