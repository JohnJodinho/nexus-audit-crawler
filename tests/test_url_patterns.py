from app.api.url_patterns import (
    PATTERN_GRAMMAR_VERSION,
    normalize_pattern,
    compile_patterns,
    evaluate_url,
    validate_pattern,
    validate_pattern_list,
)


def test_pattern_contract_version_and_normalization():
    assert PATTERN_GRAMMAR_VERSION == "1"
    assert normalize_pattern("/courses/") == "/courses"
    assert normalize_pattern("https://EXAMPLE.com:443/courses/") == (
        "https://example.com/courses"
    )


def test_supported_pattern_examples_are_valid():
    patterns = (
        "/courses",
        "/courses/",
        "/courses/*",
        "/courses/**",
        "/**/*.jpg",
        "/courses/**/*.html",
        "/blog/*/author",
        "/blog/**/author",
    )
    assert all(validate_pattern(pattern) is None for pattern in patterns)


def test_invalid_patterns_return_explanations():
    assert validate_pattern("") is not None
    assert validate_pattern("courses") is not None
    assert validate_pattern("/courses?draft=true") is not None
    assert validate_pattern("/courses/[draft]") is not None
    assert validate_pattern("/courses/***") is not None
    assert validate_pattern("https://other.example/courses", target_origin="https://example.com")
    assert validate_pattern("https://[invalid/courses") is not None


def test_pattern_list_issues_identify_field_and_position():
    issues = validate_pattern_list(
        ["/valid", "courses"],
        field="config.exclude_patterns",
    )
    assert issues == [
        {
            "field": "config.exclude_patterns",
            "code": "INVALID_URL_PATTERN",
            "index": 1,
            "pattern": "courses",
            "message": "Relative patterns must start with '/'.",
        }
    ]


def test_matcher_supports_segment_wildcards_and_exclude_precedence():
    includes = compile_patterns(
        ["/courses/**"],
        rule_type="include",
        target_origin="https://example.com",
    )
    excludes = compile_patterns(
        ["/courses/private/**"],
        rule_type="exclude",
        target_origin="https://example.com",
    )

    included = evaluate_url(
        "https://example.com/courses/python",
        target_origin="https://example.com",
        include_rules=includes,
        exclude_rules=excludes,
    )
    excluded = evaluate_url(
        "https://example.com/courses/private/secret",
        target_origin="https://example.com",
        include_rules=includes,
        exclude_rules=excludes,
    )

    assert included.decision == "included"
    assert excluded.decision == "excluded"
    assert excluded.matched_rule is not None
    assert excluded.matched_rule.rule_type == "exclude"


def test_matcher_distinguishes_one_segment_and_many_segments():
    rules = compile_patterns(
        ["/blog/*/author"],
        rule_type="include",
        target_origin="https://example.com",
    )
    assert evaluate_url(
        "https://example.com/blog/team/author",
        target_origin="https://example.com",
        include_rules=rules,
    ).decision == "included"
    assert evaluate_url(
        "https://example.com/blog/team/platform/author",
        target_origin="https://example.com",
        include_rules=rules,
    ).decision == "excluded"


def test_exclude_only_unmatched_url_remains_included():
    rules = compile_patterns(
        ["/private/**"],
        rule_type="exclude",
        target_origin="https://example.com",
    )
    decision = evaluate_url(
        "https://example.com/public",
        target_origin="https://example.com",
        exclude_rules=rules,
    )
    assert decision.decision == "included"


def test_resource_type_exclusion_is_backend_enforced():
    decision = evaluate_url(
        "https://example.com/assets/site.css",
        target_origin="https://example.com",
        exclude_resource_types=("stylesheet",),
    )
    assert decision.decision == "excluded"
    assert decision.reason == "excluded_resource_type:stylesheet"
