from app.application.services.sanitizer import sanitize


def test_email_is_redacted():
    assert "[EMAIL]" in sanitize("contact me at user@example.com please")
    assert "user@example.com" not in sanitize("contact me at user@example.com please")


def test_phone_is_redacted():
    result = sanitize("call me at 555-123-4567")
    assert "[PHONE]" in result
    assert "555-123-4567" not in result


def test_ssn_is_redacted():
    result = sanitize("my ssn is 123-45-6789")
    assert "[SSN]" in result
    assert "123-45-6789" not in result


def test_multiple_pii_types_redacted():
    text = "email: a@b.com, phone: 555-111-2222, ssn: 111-22-3333"
    result = sanitize(text)
    assert "[EMAIL]" in result
    assert "[PHONE]" in result
    assert "[SSN]" in result
    assert "a@b.com" not in result


def test_clean_text_passes_through():
    text = "What is the capital of France?"
    assert sanitize(text) == text


def test_truncation_at_max_length():
    long_text = "a" * 1000
    result = sanitize(long_text, max_length=500)
    assert len(result) == 500


def test_truncation_default_500():
    long_text = "b" * 800
    result = sanitize(long_text)
    assert len(result) == 500