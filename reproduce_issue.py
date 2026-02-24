from services.upload_service import _validate_datetime_format, UploadParsingError
from datetime import datetime

def test_date_validation():
    # Test cases
    test_cases = [
        ("2026-01-01", True, "2026-01-01 00"), # Existing valid format -> default 00
        ("2026-01-01 00", True, "2026-01-01 00"), # Target format -> exact match
        ("2026/01/01", True, "2026-01-01 00"), # Slash format -> default 00
        ("2026-02-18 00", True, "2026-02-18 00"), # User example
    ]

    print("Running date validation tests...")
    
    for date_str, expected_success, expected_output in test_cases:
        print(f"Testing input: '{date_str}'")
        try:
            # Mock row number 1, field name 'Test Field'
            result = _validate_datetime_format(date_str, 1, "Test Field")
            print(f"  Result: {result}")
            
            if not expected_success:
                print(f"  [FAIL] Expected failure but got success.")
            elif expected_output and result != expected_output:
                 print(f"  [FAIL] Output mismatch. Expected {expected_output}, got {result}")
            else:
                 print(f"  [PASS]")

        except UploadParsingError as e:
            print(f"  Caught expected error: {e}")
            if expected_success:
                print(f"  [FAIL] Expected success but got error.")
            else:
                print(f"  [PASS] Successfully rejected (expected).")
        except Exception as e:
            print(f"  [ERROR] Unexpected exception: {e}")

if __name__ == "__main__":
    test_date_validation()
