import sys

sys.path.insert(0, "src")
from validator import is_valid_cnj, is_valid_email, is_valid_phone


def test_is_valid_cnj():
    assert is_valid_cnj("12345678901234567890")
    assert not is_valid_cnj("123456789")
    assert not is_valid_cnj(None)


def test_is_valid_email():
    assert is_valid_email("test@example.com")
    assert not is_valid_email("invalid")


def test_is_valid_phone():
    assert is_valid_phone("(11) 99999-9999")
    assert not is_valid_phone("123")


if __name__ == "__main__":
    test_is_valid_cnj()
    test_is_valid_email()
    test_is_valid_phone()
    print("All tests passed")
