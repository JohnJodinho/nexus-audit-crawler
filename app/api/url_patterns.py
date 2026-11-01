"""Versioned URL-pattern contract shared by API validation and clients."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from urllib.parse import urlsplit

from app.utils.utilities import canonicalize_url

PATTERN_GRAMMAR_VERSION = "1"
MAX_PATTERN_LENGTH = 512
MAX_PATTERNS_PER_RULE = 100
SUPPORTED_RESOURCE_TYPES = (
    "image",
    "document",
    "stylesheet",
    "script",
    "font",
    "media",
)


@dataclass(frozen=True)
class PatternValidationIssue:
    """A stable, frontend-displayable pattern validation failure."""

    message: str
    index: int | None = None


@dataclass(frozen=True)
class CompiledPattern:
    """A validated rule ready for deterministic URL matching."""

    source: str
    normalized: str
    rule_type: str
    rule_id: str
    literal_segments: int
    wildcard_count: int
    broad_wildcards: int
    literal_characters: int

    @property
    def specificity(self) -> tuple[int, int, int, int]:
        return (
            self.literal_segments,
            -self.broad_wildcards,
            -self.wildcard_count,
            self.literal_characters,
        )


@dataclass(frozen=True)
class PatternDecision:
    """The URL filtering result and the rule that produced it."""

    decision: str
    reason: str
    normalized_url: str
    normalized_path: str
    matched_rule: CompiledPattern | None = None


_RESOURCE_SUFFIXES = {
    "image": (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".avif"),
    "document": (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"),
    "stylesheet": (".css",),
    "script": (".js", ".mjs", ".tsx", ".jsx"),
    "font": (".woff", ".woff2", ".ttf", ".otf", ".eot"),
    "media": (".mp4", ".webm", ".mp3", ".wav", ".mov", ".avi"),
}


def _normalize_path(path: str) -> str:
    normalized = path or "/"
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    if len(normalized) > 1:
        normalized = normalized.rstrip("/")
    return normalized


def _origin(parts) -> str:
    hostname = (parts.hostname or "").lower()
    port = parts.port
    if (parts.scheme.lower(), port) in {("http", 80), ("https", 443)}:
        port = None
    return f"{parts.scheme.lower()}://{hostname}{f':{port}' if port else ''}"


def normalize_pattern(pattern: str, *, target_origin: str | None = None) -> str:
    """Normalize a valid relative or same-origin URL pattern."""
    candidate = pattern.strip()
    try:
        parts = urlsplit(candidate)
    except ValueError as exc:
        raise ValueError(f"Pattern URL is invalid: {exc}.") from exc
    if parts.scheme or parts.netloc:
        normalized_origin = _origin(parts)
        if target_origin and normalized_origin != target_origin:
            raise ValueError("Absolute patterns must use the crawl's origin.")
        return f"{normalized_origin}{_normalize_path(parts.path)}"
    return _normalize_path(candidate)


def _validate_basic_pattern(candidate: str) -> PatternValidationIssue | None:
    if not candidate:
        return PatternValidationIssue("Pattern must not be empty.")
    if len(candidate) > MAX_PATTERN_LENGTH:
        return PatternValidationIssue(
            f"Pattern must be at most {MAX_PATTERN_LENGTH} characters."
        )
    return None


def _validate_url_parts(
    candidate: str,
    parts,
    target_origin: str | None,
) -> tuple[PatternValidationIssue | None, str]:
    if parts.fragment or parts.query:
        return (
            PatternValidationIssue(
                "Patterns must not contain a query string or fragment."
            ),
            "",
        )
    if parts.scheme and parts.scheme.lower() not in {"http", "https"}:
        return PatternValidationIssue("Absolute patterns must use http or https."), ""
    if parts.scheme or parts.netloc:
        if not parts.hostname:
            return PatternValidationIssue(
                "Absolute patterns must include a hostname."
            ), ""
        try:
            return None, normalize_pattern(candidate, target_origin=target_origin)
        except (ValueError, UnicodeError) as exc:
            return PatternValidationIssue(str(exc)), ""
    if not candidate.startswith("/"):
        return PatternValidationIssue("Relative patterns must start with '/'."), ""
    return None, candidate


def _validate_wildcards(candidate: str, normalized: str) -> PatternValidationIssue | None:
    if "***" in normalized:
        return PatternValidationIssue(
            "Use '*' for one path segment or '**' for zero or more segments."
        )
    if any(character in candidate for character in "[]{}\\"):
        return PatternValidationIssue(
            "Only path segments, '*', and '**' are supported; regex syntax is not allowed."
        )
    for segment in normalized.split("/"):
        if "*" in segment and segment.count("*") not in {1, 2}:
            return PatternValidationIssue(
                "A path segment may contain one '*' or one '**' wildcard."
            )
    return None


def validate_pattern(
    pattern: str,
    *,
    target_origin: str | None = None,
) -> PatternValidationIssue | None:
    """Validate pattern syntax without evaluating it against a URL."""
    if not isinstance(pattern, str):
        return PatternValidationIssue("Pattern must be a string.")

    candidate = pattern.strip()
    basic_issue = _validate_basic_pattern(candidate)
    if basic_issue is not None:
        return basic_issue
    try:
        parts = urlsplit(candidate)
    except ValueError as exc:
        return PatternValidationIssue(f"Pattern URL is invalid: {exc}.")
    parts_issue, normalized = _validate_url_parts(candidate, parts, target_origin)
    if parts_issue is not None:
        return parts_issue
    return _validate_wildcards(candidate, normalized)


def validate_pattern_list(
    patterns: list[str],
    *,
    field: str,
    target_origin: str | None = None,
) -> list[dict[str, object]]:
    """Return structured validation issues for a request pattern list."""
    issues: list[dict[str, object]] = []
    if len(patterns) > MAX_PATTERNS_PER_RULE:
        issues.append(
            {
                "field": field,
                "code": "TOO_MANY_PATTERNS",
                "index": None,
                "pattern": None,
                "message": f"{field} supports at most {MAX_PATTERNS_PER_RULE} patterns.",
            }
        )
        return issues

    for index, pattern in enumerate(patterns):
        issue = validate_pattern(pattern, target_origin=target_origin)
        if issue is not None:
            issues.append(
                {
                    "field": field,
                    "code": "INVALID_URL_PATTERN",
                    "index": index,
                    "pattern": pattern,
                    "message": issue.message,
                }
            )
    return issues


def normalize_pattern_list(
    patterns: list[str],
    *,
    target_origin: str,
) -> list[str]:
    """Return canonical pattern values after validating the complete list."""
    issues = validate_pattern_list(
        patterns,
        field="config.patterns",
        target_origin=target_origin,
    )
    if issues:
        raise ValueError(issues)
    return [normalize_pattern(pattern, target_origin=target_origin) for pattern in patterns]


def compile_patterns(
    patterns: list[str],
    *,
    rule_type: str,
    target_origin: str,
) -> tuple[CompiledPattern, ...]:
    """Validate and compile a rule list for crawl-time matching."""
    compiled: list[CompiledPattern] = []
    issues = validate_pattern_list(
        patterns,
        field=f"config.{rule_type}_patterns",
        target_origin=target_origin,
    )
    if issues:
        raise ValueError(issues)

    for index, source in enumerate(patterns):
        normalized = normalize_pattern(source, target_origin=target_origin)
        segments = tuple(segment for segment in normalized.split("/") if segment)
        wildcard_count = normalized.count("*")
        broad_wildcards = normalized.count("**")
        literal_characters = len(normalized.replace("*", ""))
        digest = hashlib.sha256(
            f"{rule_type}:{index}:{normalized}".encode("utf-8")
        ).hexdigest()[:16]
        compiled.append(
            CompiledPattern(
                source=source,
                normalized=normalized,
                rule_type=rule_type,
                rule_id=f"{rule_type}:{digest}",
                literal_segments=sum("*" not in segment for segment in segments),
                wildcard_count=wildcard_count,
                broad_wildcards=broad_wildcards,
                literal_characters=literal_characters,
            )
        )
    return tuple(compiled)


def _segment_matches(pattern: str, value: str) -> bool:
        if pattern == "**":
            return True
        expression = re.escape(pattern).replace(r"\*", "[^/]*")
        return re.fullmatch(expression, value, flags=re.IGNORECASE) is not None


def _path_matches(pattern: str, path: str) -> bool:
        pattern_parts = tuple(part for part in pattern.split("/") if part)
        path_parts = tuple(part for part in path.split("/") if part)

        def match(pattern_index: int, path_index: int) -> bool:
            if pattern_index == len(pattern_parts):
                return path_index == len(path_parts)
            segment = pattern_parts[pattern_index]
            if segment == "**":
                return any(
                    match(pattern_index + 1, candidate_index)
                    for candidate_index in range(path_index, len(path_parts) + 1)
                )
            return (
                path_index < len(path_parts)
                and _segment_matches(segment, path_parts[path_index])
                and match(pattern_index + 1, path_index + 1)
            )

        return match(0, 0)


def _matching_rules(
    rules: tuple[CompiledPattern, ...],
    normalized_path: str,
) -> list[CompiledPattern]:
    return [
        rule
        for rule in rules
        if _path_matches(urlsplit(rule.normalized).path, normalized_path)
    ]


def _resource_type(path: str) -> str | None:
    return next(
        (
            resource
            for resource, suffixes in _RESOURCE_SUFFIXES.items()
            if path.lower().endswith(suffixes)
        ),
        None,
    )


def _select_winning_rule(
    include_rules: list[CompiledPattern],
    exclude_rules: list[CompiledPattern],
) -> CompiledPattern | None:
    candidates = [*include_rules, *exclude_rules]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda rule: (rule.specificity, rule.rule_type == "exclude"),
    )


def _resource_exclusion_decision(
    *,
    normalized_url: str,
    normalized_path: str,
    excluded_types: tuple[str, ...],
) -> PatternDecision | None:
    resource_type = _resource_type(normalized_path)
    if resource_type not in excluded_types:
        return None
    return PatternDecision(
        "excluded",
        f"excluded_resource_type:{resource_type}",
        normalized_url,
        normalized_path,
    )


def _rule_decision(
    *,
    normalized_url: str,
    normalized_path: str,
    include_rules: tuple[CompiledPattern, ...],
    exclude_rules: tuple[CompiledPattern, ...],
) -> PatternDecision:
    matching_includes = _matching_rules(include_rules, normalized_path)
    matching_excludes = _matching_rules(exclude_rules, normalized_path)
    if not include_rules and not exclude_rules:
        return PatternDecision("included", "no_rules", normalized_url, normalized_path)
    if include_rules and not matching_includes:
        return PatternDecision(
            "excluded",
            "include_no_match",
            normalized_url,
            normalized_path,
        )
    winner = _select_winning_rule(matching_includes, matching_excludes)
    if winner is None:
        return PatternDecision("included", "no_rule_match", normalized_url, normalized_path)
    decision = "excluded" if winner.rule_type == "exclude" else "included"
    return PatternDecision(
        decision,
        f"matched_{winner.rule_type}",
        normalized_url,
        normalized_path,
        winner,
    )


def evaluate_url(
    url: str,
    *,
    target_origin: str,
    include_rules: tuple[CompiledPattern, ...] = (),
    exclude_rules: tuple[CompiledPattern, ...] = (),
    exclude_resource_types: tuple[str, ...] = (),
) -> PatternDecision:
    """Evaluate one URL using the shared include/exclude precedence rules."""
    normalized_url = canonicalize_url(url)
    parsed = urlsplit(normalized_url)
    normalized_path = _normalize_path(parsed.path)
    if _origin(parsed) != target_origin:
        return PatternDecision(
            "excluded",
            "outside_origin",
            normalized_url,
            normalized_path,
        )
    resource_decision = _resource_exclusion_decision(
        normalized_url=normalized_url,
        normalized_path=normalized_path,
        excluded_types=exclude_resource_types,
    )
    return resource_decision or _rule_decision(
        normalized_url=normalized_url,
        normalized_path=normalized_path,
        include_rules=include_rules,
        exclude_rules=exclude_rules,
    )

    for index, pattern in enumerate(patterns):
        issue = validate_pattern(pattern, target_origin=target_origin)
        if issue is not None:
            issues.append(
                {
                    "field": field,
                    "code": "INVALID_URL_PATTERN",
                    "index": index,
                    "pattern": pattern,
                    "message": issue.message,
                }
            )
    return issues
