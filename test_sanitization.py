import sys
sys.path.append('.')

from function_app import sanitize_metadata_value

# Test cases for metadata sanitization
test_cases = [
    ("Normal text", "Normal text"),
    ("Text with\nnewlines\rand\ttabs", "Text with newlines and tabs"),
    ("Text with special chars: !@#$%^&*()", "Text with special chars: !@#$%^&*()"),
    ("Text with unicode: éñüñ", "Text with unicode: "),  # Non-ASCII removed
    ("   Multiple   spaces   ", "Multiple spaces"),
    ("Control char: \x1f test", "Control char:  test"),
    ("Very long text " * 50, "Very long text " * 12 + "Very long text Very long text Very long text Very long text Very long text Very long text Very lon..."),
    ("", ""),
    (None, "")
]

print("Testing metadata sanitization...")
for i, (input_val, expected) in enumerate(test_cases):
    result = sanitize_metadata_value(input_val)
    status = "✅ PASS" if result == expected else "❌ FAIL"
    print(f"Test {i+1}: {status}")
    if result != expected:
        print(f"  Input: {repr(input_val)}")
        print(f"  Expected: {repr(expected)}")
        print(f"  Got: {repr(result)}")
    print()
