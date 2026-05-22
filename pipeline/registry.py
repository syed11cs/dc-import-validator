"""Load and validate the canonical pipeline step registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

try:
    import yaml
except ImportError:  # pragma: no cover - exercised when PyYAML missing
    yaml = None  # type: ignore[assignment]

_REGISTRY_DIR = Path(__file__).resolve().parent
_DEFAULT_REGISTRY_PATH = _REGISTRY_DIR / "registry.yaml"


@dataclass(frozen=True)
class Substep:
    id: str
    label: str
    optional: bool = False


@dataclass(frozen=True)
class Step:
    id: str
    index: int
    label: str
    blocking: bool
    substeps: tuple[Substep, ...]


@dataclass(frozen=True)
class Registry:
    schema_version: str
    steps: tuple[Step, ...]
    legacy_markers: dict[str, str]

    @property
    def step_count(self) -> int:
        return len(self.steps)


def default_registry_path() -> Path:
    return _DEFAULT_REGISTRY_PATH


def load_registry(path: Path | str | None = None) -> Registry:
    """Load registry.yaml from disk and return a validated Registry."""
    registry_path = Path(path) if path is not None else _DEFAULT_REGISTRY_PATH
    if not registry_path.is_file():
        raise FileNotFoundError(f"Pipeline registry not found: {registry_path}")
    if yaml is None:
        raise ImportError(
            "PyYAML is required to load pipeline/registry.yaml. "
            "Install with: pip install PyYAML"
        )
    with registry_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"Registry root must be a mapping, got {type(raw).__name__}")
    return parse_registry(raw)


def parse_registry(data: Mapping[str, Any]) -> Registry:
    """Parse and validate a registry dict (for tests and programmatic use)."""
    schema_version = data.get("schema_version")
    if not schema_version or not isinstance(schema_version, str):
        raise ValueError("registry.schema_version must be a non-empty string")

    raw_steps = data.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise ValueError("registry.steps must be a non-empty list")

    steps: list[Step] = []
    seen_ids: set[str] = set()
    seen_indices: set[int] = set()

    for i, raw_step in enumerate(raw_steps):
        if not isinstance(raw_step, dict):
            raise ValueError(f"steps[{i}] must be a mapping")
        step_id = _require_str(raw_step, "id", f"steps[{i}]")
        if step_id in seen_ids:
            raise ValueError(f"duplicate step id: {step_id!r}")
        seen_ids.add(step_id)

        index = raw_step.get("index")
        if not isinstance(index, int) or index < 0:
            raise ValueError(f"steps[{i}].index must be a non-negative integer")
        if index in seen_indices:
            raise ValueError(f"duplicate step index: {index}")
        seen_indices.add(index)

        label = _require_str(raw_step, "label", f"steps[{i}]")
        blocking = raw_step.get("blocking")
        if not isinstance(blocking, bool):
            raise ValueError(f"steps[{i}].blocking must be a boolean")

        substeps = _parse_substeps(raw_step.get("substeps"), step_id)
        steps.append(
            Step(
                id=step_id,
                index=index,
                label=label,
                blocking=blocking,
                substeps=substeps,
            )
        )

    steps.sort(key=lambda s: s.index)
    expected_indices = list(range(len(steps)))
    actual_indices = [s.index for s in steps]
    if actual_indices != expected_indices:
        raise ValueError(
            f"step indices must be contiguous 0..{len(steps) - 1}, got {actual_indices}"
        )

    legacy_raw = data.get("legacy_markers")
    if not isinstance(legacy_raw, dict) or not legacy_raw:
        raise ValueError("registry.legacy_markers must be a non-empty mapping")

    legacy_markers: dict[str, str] = {}
    for marker, target_id in legacy_raw.items():
        if not isinstance(marker, str) or not marker.strip():
            raise ValueError("legacy_markers keys must be non-empty strings")
        if not isinstance(target_id, str) or not target_id.strip():
            raise ValueError(f"legacy_markers[{marker!r}] must be a non-empty string")
        if target_id not in seen_ids:
            raise ValueError(
                f"legacy_markers[{marker!r}] references unknown step id {target_id!r}"
            )
        legacy_markers[marker] = target_id

    return Registry(
        schema_version=schema_version,
        steps=tuple(steps),
        legacy_markers=legacy_markers,
    )


def step_by_id(registry: Registry, step_id: str) -> Step | None:
    for step in registry.steps:
        if step.id == step_id:
            return step
    return None


def step_by_index(registry: Registry, index: int) -> Step | None:
    for step in registry.steps:
        if step.index == index:
            return step
    return None


def resolve_legacy_marker(registry: Registry, line: str) -> Step | None:
    """Resolve a stdout line (e.g. ::STEP::2.4:Differ) to a Step via legacy_markers."""
    normalized = line.strip()
    if not normalized:
        return None
    step_id = registry.legacy_markers.get(normalized)
    if step_id is None:
        return None
    return step_by_id(registry, step_id)


def registry_as_dict(registry: Registry) -> dict[str, Any]:
    """Serialize Registry for GET /api/pipeline/registry."""
    return {
        "schema_version": registry.schema_version,
        "steps": [
            {
                "id": s.id,
                "index": s.index,
                "label": s.label,
                "blocking": s.blocking,
                "substeps": [
                    {
                        "id": sub.id,
                        "label": sub.label,
                        "optional": sub.optional,
                    }
                    for sub in s.substeps
                ],
            }
            for s in registry.steps
        ],
        "legacy_markers": dict(registry.legacy_markers),
    }


def _parse_substeps(raw: Any, step_id: str) -> tuple[Substep, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError(f"steps[{step_id!r}].substeps must be a list")
    substeps: list[Substep] = []
    seen_sub_ids: set[str] = set()
    for j, raw_sub in enumerate(raw):
        if not isinstance(raw_sub, dict):
            raise ValueError(f"steps[{step_id!r}].substeps[{j}] must be a mapping")
        sub_id = _require_str(raw_sub, "id", f"steps[{step_id}].substeps[{j}]")
        if sub_id in seen_sub_ids:
            raise ValueError(f"duplicate substep id {sub_id!r} in step {step_id!r}")
        seen_sub_ids.add(sub_id)
        sub_label = _require_str(raw_sub, "label", f"steps[{step_id}].substeps[{j}]")
        optional = raw_sub.get("optional", False)
        if not isinstance(optional, bool):
            raise ValueError(
                f"steps[{step_id!r}].substeps[{j}].optional must be a boolean"
            )
        substeps.append(Substep(id=sub_id, label=sub_label, optional=optional))
    return tuple(substeps)


def _require_str(data: Mapping[str, Any], key: str, context: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}.{key} must be a non-empty string")
    return value.strip()
