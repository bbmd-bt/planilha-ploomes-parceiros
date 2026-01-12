import re


def is_valid_cnj(cnj_str: str) -> bool:
    if not cnj_str:
        return False
    digits = re.sub(r"\D", "", cnj_str)
    return len(digits) == 20


def is_valid_email(email_str: str) -> bool:
    if not email_str:
        return False
    # Regex simples para email
    return bool(re.match(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$", email_str.strip()))


def is_valid_phone(phone_str: str) -> bool:
    if not phone_str:
        return False
    digits = re.sub(r"\D", "", phone_str)
    return len(digits) in (10, 11)
