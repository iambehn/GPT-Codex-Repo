"""Small YAML subset loader for repo-owned config and starter asset files.

This intentionally supports only the features currently used in this repo:
- mappings
- lists
- quoted or bare strings
- ints / floats / booleans
- inline lists like ["a", "b"]
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?$")


@dataclass
class _Line:
    indent: int
    content: str


def load_yaml_file(path: str | Path) -> Any:
    return loads(Path(path).read_text(encoding="utf-8"))


def dump_yaml_file(path: str | Path, data: Any) -> None:
    Path(path).write_text(dumps(data), encoding="utf-8")


def dumps(data: Any) -> str:
    return _dump_node(data, 0).rstrip() + "\n"


def loads(text: str) -> Any:
    lines = _tokenize(text)
    if not lines:
        return {}
    data, index = _parse_node(lines, 0, lines[0].indent)
    if index != len(lines):
        raise ValueError("YAML parsing did not consume the full document")
    return data


def _tokenize(text: str) -> list[_Line]:
    result: list[_Line] = []
    for raw_line in text.splitlines():
        if not raw_line.strip():
            continue
        stripped = raw_line.lstrip(" ")
        if stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(stripped)
        result.append(_Line(indent=indent, content=stripped))
    return result


def _parse_node(lines: list[_Line], index: int, indent: int) -> tuple[Any, int]:
    if index >= len(lines):
        return {}, index
    line = lines[index]
    if line.indent != indent:
        raise ValueError(f"Unexpected indent at line: {line.content}")
    if line.content.startswith("- "):
        return _parse_list(lines, index, indent)
    return _parse_dict(lines, index, indent)


def _parse_dict(lines: list[_Line], index: int, indent: int) -> tuple[dict[str, Any], int]:
    result: dict[str, Any] = {}
    while index < len(lines):
        line = lines[index]
        if line.indent < indent:
            break
        if line.indent != indent:
            raise ValueError(f"Unexpected indent inside mapping: {line.content}")
        if line.content.startswith("- "):
            break

        key, raw_value = _split_key_value(line.content)
        index += 1

        if raw_value == "":
            if index < len(lines) and lines[index].indent > indent:
                child, index = _parse_node(lines, index, lines[index].indent)
                result[key] = child
            else:
                result[key] = None
        else:
            result[key] = _parse_value(raw_value)
    return result, index


def _parse_list(lines: list[_Line], index: int, indent: int) -> tuple[list[Any], int]:
    result: list[Any] = []
    while index < len(lines):
        line = lines[index]
        if line.indent < indent:
            break
        if line.indent != indent or not line.content.startswith("- "):
            break

        item_content = line.content[2:].strip()
        index += 1

        if item_content == "":
            if index < len(lines) and lines[index].indent > indent:
                child, index = _parse_node(lines, index, lines[index].indent)
                result.append(child)
            else:
                result.append(None)
            continue

        if _looks_like_mapping(item_content):
            key, raw_value = _split_key_value(item_content)
            item: dict[str, Any] = {}
            if raw_value == "":
                if index < len(lines) and lines[index].indent > indent:
                    child, index = _parse_node(lines, index, lines[index].indent)
                    item[key] = child
                else:
                    item[key] = None
            else:
                item[key] = _parse_value(raw_value)

            while index < len(lines):
                next_line = lines[index]
                if next_line.indent <= indent:
                    break
                if next_line.indent == indent and next_line.content.startswith("- "):
                    break
                child_map, index = _parse_dict(lines, index, indent + 2)
                item.update(child_map)
            result.append(item)
            continue

        result.append(_parse_value(item_content))
    return result, index


def _split_key_value(content: str) -> tuple[str, str]:
    key, raw_value = content.split(":", 1)
    return key.strip(), raw_value.strip()


def _looks_like_mapping(content: str) -> bool:
    if content.startswith("[") or content.startswith("{"):
        return False
    return ":" in content


def _parse_value(raw: str) -> Any:
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1]
    if raw.startswith("[") and raw.endswith("]"):
        return ast.literal_eval(raw)
    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if _NUMBER_RE.match(raw):
        return float(raw) if "." in raw else int(raw)
    return raw


def _dump_node(value: Any, indent: int) -> str:
    if isinstance(value, dict):
        return _dump_dict(value, indent)
    if isinstance(value, list):
        return _dump_list(value, indent)
    return (" " * indent) + _dump_scalar(value) + "\n"


def _dump_dict(value: dict[str, Any], indent: int) -> str:
    if not value:
        return (" " * indent) + "{}\n"

    lines: list[str] = []
    for key, item in value.items():
        prefix = (" " * indent) + f"{key}:"
        if isinstance(item, (dict, list)):
            lines.append(prefix + "\n")
            lines.append(_dump_node(item, indent + 2))
        else:
            lines.append(prefix + f" {_dump_scalar(item)}\n")
    return "".join(lines)


def _dump_list(value: list[Any], indent: int) -> str:
    if not value:
        return (" " * indent) + "[]\n"

    lines: list[str] = []
    for item in value:
        prefix = (" " * indent) + "- "
        if isinstance(item, dict):
            if not item:
                lines.append(prefix + "{}\n")
                continue
            first = True
            for key, child in item.items():
                if first and not isinstance(child, (dict, list)):
                    lines.append(prefix + f"{key}: {_dump_scalar(child)}\n")
                    first = False
                    continue
                if first:
                    lines.append(prefix + f"{key}:\n")
                    lines.append(_dump_node(child, indent + 4))
                    first = False
                    continue
                child_prefix = (" " * (indent + 2)) + f"{key}:"
                if isinstance(child, (dict, list)):
                    lines.append(child_prefix + "\n")
                    lines.append(_dump_node(child, indent + 4))
                else:
                    lines.append(child_prefix + f" {_dump_scalar(child)}\n")
            continue
        if isinstance(item, list):
            lines.append(prefix.rstrip() + "\n")
            lines.append(_dump_node(item, indent + 2))
            continue
        lines.append(prefix + _dump_scalar(item) + "\n")
    return "".join(lines)


def _dump_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "" or any(ch in text for ch in [":", "#", "\n", '"', "'", "[", "]", "{", "}"]) or text.strip() != text:
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return text
