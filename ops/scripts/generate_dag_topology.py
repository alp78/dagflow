#!/usr/bin/env python3
"""Generate DAG topology JSON from Dagster asset definitions.

Introspects the actual Dagster Definitions object to produce a JSON file
describing nodes (ops) and edges (dependencies) for each workflow.
The UI loads this topology so the visual DAG is always true to Dagster.

Usage:
    python ops/scripts/generate_dag_topology.py [--out apps/ui-react/src/generated]

The output is one JSON file per workflow plus an index:
    <out>/dag_topology.json
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "config.json").exists():
            return parent
    raise FileNotFoundError("config.json not found")


def _add_to_path() -> None:
    root = _repo_root()
    # Add all workflow src dirs and the dagster app src dir to sys.path
    dagster_src = root / "apps" / "dagster" / "src"
    if str(dagster_src) not in sys.path:
        sys.path.insert(0, str(dagster_src))
    config = json.loads((root / "config.json").read_text())
    for wf in config["workflows"]:
        wf_src = root / wf["path"] / "src"
        if str(wf_src) not in sys.path:
            sys.path.insert(0, str(wf_src))
    # internal packages
    for pkg_dir in (root / "packages").iterdir():
        if not pkg_dir.is_dir():
            continue
        src_dir = pkg_dir / "src"
        candidate = src_dir if src_dir.is_dir() else pkg_dir
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))


def _build_topology(workflow_config: dict[str, Any]) -> dict[str, Any]:
    """Build the DAG topology for a single workflow by introspecting Dagster."""
    mod = __import__(f"{workflow_config['package']}.definitions", fromlist=["ASSETS", "JOBS"])
    assets_list = mod.ASSETS
    jobs_list = mod.JOBS

    # Collect all AssetsDefinition objects and build the full key→info map
    from dagster import AssetKey

    # Map: asset_key → { op_name, group_name, keys_in_op, upstream_keys, description, metadata }
    key_info: dict[str, dict[str, Any]] = {}
    op_keys: dict[str, list[str]] = defaultdict(list)  # op_name → [asset_key_str, ...]

    for assets_def in assets_list:
        op_name = assets_def.op.name
        for asset_key in assets_def.keys:
            key_str = asset_key.to_user_string()
            upstream = set()
            if hasattr(assets_def, "asset_deps"):
                deps = assets_def.asset_deps.get(asset_key, set())
                upstream = {k.to_user_string() for k in deps}
            # Get group name from the asset spec if available
            group_name = None
            if hasattr(assets_def, "group_names_by_key"):
                group_name = assets_def.group_names_by_key.get(asset_key)
            # Get description
            desc = None
            if hasattr(assets_def, "descriptions_by_key"):
                desc = assets_def.descriptions_by_key.get(asset_key)
            # Get metadata
            meta = {}
            if hasattr(assets_def, "metadata_by_key"):
                raw_meta = assets_def.metadata_by_key.get(asset_key, {})
                for mk, mv in raw_meta.items():
                    meta[mk] = mv.value if hasattr(mv, "value") else mv

            key_info[key_str] = {
                "asset_key": key_str,
                "op_name": op_name,
                "group_name": group_name,
                "upstream_keys": sorted(upstream),
                "description": desc,
                "metadata": meta,
            }
            op_keys[op_name].append(key_str)

    # Build asset-level nodes: one node per asset key
    all_asset_key_strs = set(key_info.keys())
    KEEP_META_PREFIXES = ("dagflow_",)

    # Resolve upstream keys: dbt sources reference keys that may map to
    # assets produced by other ops (e.g., raw table keys from load_to_raw).
    # Build a lookup of all known asset keys.
    for key_str, info in key_info.items():
        resolved_upstream: list[str] = []
        for dep in info["upstream_keys"]:
            if dep in all_asset_key_strs:
                resolved_upstream.append(dep)
            # else: external dep (not in this pipeline), skip
        info["upstream_keys"] = resolved_upstream

    # Expand multi-step assets into individual sub-step nodes.
    # Assets like capture_sources (4 sub-steps) and review_snapshot (2 sub-steps)
    # are single Dagster ops that internally record multiple pipeline steps.
    # We split them so each sub-step becomes its own DAG node.
    from dagflow_dagster.execution import pipeline_step_definitions
    pipeline_code = workflow_config["name"]
    step_defs = pipeline_step_definitions(pipeline_code)

    # Build a map: group_name → [step_def, ...]
    group_steps: dict[str, list[Any]] = defaultdict(list)
    for sd in step_defs:
        group_steps[sd.group].append(sd)

    # Identify assets to expand: single assets whose group has multiple step defs
    # AND the asset is the only one in that group (not multi-output like dbt_assets)
    assets_per_group: dict[str, list[str]] = defaultdict(list)
    for key_str, info in key_info.items():
        if info.get("group_name"):
            assets_per_group[info["group_name"]].append(key_str)

    expand_keys: set[str] = set()
    for group_name, asset_keys_in_group in assets_per_group.items():
        steps_in_group = group_steps.get(group_name, [])
        if len(asset_keys_in_group) == 1 and len(steps_in_group) > 1:
            expand_keys.add(asset_keys_in_group[0])

    # Build dataset aliases automatically from step definitions and asset keys.
    # For each asset key fragment, find step keys that share a common substring.
    all_frags = set()
    for k in key_info:
        frag = k.split("__")[-1] if "__" in k else k
        all_frags.add(frag)
        all_frags.add(frag.replace("raw_", "").replace("_export", "").replace("_capture", ""))

    def _stem(word: str) -> str:
        """Naive stemming: strip trailing s/es for plural matching."""
        if word.endswith("ies"):
            return word[:-3] + "y"
        if word.endswith("es"):
            return word[:-2]
        if word.endswith("s") and not word.endswith("ss"):
            return word[:-1]
        return word

    def _strip_pipeline_prefix(s: str) -> str:
        """Remove the pipeline_code prefix from step keys/node IDs."""
        prefix = workflow_config["name"] + "_"
        return s[len(prefix):] if s.startswith(prefix) else s

    def _matches_dataset(frag: str, step_key: str) -> bool:
        clean_frag = _strip_pipeline_prefix(frag).replace("raw_", "").replace("_export", "").replace("_capture", "").replace("_review", "")
        clean_step = _strip_pipeline_prefix(step_key).replace("raw_", "").replace("_export", "").replace("_capture", "").replace("_review", "")
        if not clean_frag or not clean_step:
            return False
        if clean_frag in clean_step or clean_step in clean_frag:
            return True
        frag_words = {_stem(w) for w in clean_frag.split("_") if len(w) > 2}
        step_words = {_stem(w) for w in clean_step.split("_") if len(w) > 2}
        return bool(frag_words & step_words)

    # For each expandable asset, replace it with sub-step nodes
    for expand_key in expand_keys:
        parent_info = key_info.pop(expand_key)
        all_asset_key_strs.discard(expand_key)
        parent_group = parent_info["group_name"]
        parent_upstream = parent_info["upstream_keys"]
        sub_steps = group_steps[parent_group]

        # Find downstream assets that depended on the parent
        downstream_of_parent = [
            k for k, info in key_info.items()
            if expand_key in info["upstream_keys"]
        ]

        # Create one node per sub-step
        sub_node_ids: list[str] = []
        for sd in sorted(sub_steps, key=lambda s: s.sort_order):
            sub_id = f"{expand_key}__{sd.step_key}"
            sub_node_ids.append(sub_id)
            all_asset_key_strs.add(sub_id)

            # Try to match this sub-step to a specific downstream/upstream asset
            # by checking if the asset's dataset concept appears in the step key
            DATASET_ALIASES: dict[str, list[str]] = {
                "securities": ["security_master", "securities"],
                "security_master": ["security_master", "securities"],
                "holdings": ["shareholder_holdings", "holdings", "13f"],
                "shareholder_holdings": ["shareholder_holdings", "holdings", "13f"],
                "tickers": ["tickers", "sec_company_tickers"],
                "facts": ["facts", "sec_company_facts"],
                "filers": ["filers", "13f_filers"],
            }

            matched_upstream: list[str] = []
            for us_key in parent_upstream:
                us_frag = us_key.split("__")[-1] if "__" in us_key else us_key
                if _matches_dataset(us_frag, sd.step_key):
                    matched_upstream.append(us_key)

            key_info[sub_id] = {
                "asset_key": sub_id,
                "op_name": parent_info["op_name"],
                "group_name": parent_group,
                "upstream_keys": matched_upstream if matched_upstream else parent_upstream[:],
                "metadata": parent_info.get("metadata", {}),
                "_pipeline_code": pipeline_code,
            }
            op_keys[parent_info["op_name"]].append(sub_id)

        # Track which downstreams were matched to specific sub-steps
        matched_ds_keys: set[str] = set()
        # Re-run matching to determine which downstream goes to which sub-step
        ds_to_sub: dict[str, str] = {}
        for sd in sorted(sub_steps, key=lambda s: s.sort_order):
            sub_id = f"{expand_key}__{sd.step_key}"
            for ds_key in downstream_of_parent:
                ds_frag = ds_key.split("__")[-1] if "__" in ds_key else ds_key
                if _matches_dataset(ds_frag, sd.step_key) and ds_key not in ds_to_sub:
                    ds_to_sub[ds_key] = sub_id

        # Apply: replace parent dep with the specific sub-step
        for ds_key in downstream_of_parent:
            ds_info = key_info[ds_key]
            if ds_key in ds_to_sub:
                ds_info["upstream_keys"] = [
                    ds_to_sub[ds_key] if u == expand_key else u
                    for u in ds_info["upstream_keys"]
                ]
            elif expand_key in ds_info["upstream_keys"]:
                # Unmatched: depend on all sub-steps
                idx = ds_info["upstream_keys"].index(expand_key)
                ds_info["upstream_keys"] = (
                    ds_info["upstream_keys"][:idx]
                    + sub_node_ids
                    + ds_info["upstream_keys"][idx + 1:]
                )
            ds_info["upstream_keys"] = sorted(set(ds_info["upstream_keys"]))

    # Build edges from asset-level dependencies (including expanded sub-steps)
    edges: list[list[str]] = []
    for key_str, info in key_info.items():
        for dep in info["upstream_keys"]:
            if dep in all_asset_key_strs:
                edges.append([dep, key_str])

    # Cross-job edges: detect disconnected subgraphs and link them
    edge_targets = {e[1] for e in edges}
    root_nodes_set = [k for k in all_asset_key_strs if k not in edge_targets]
    terminal_nodes_set = [k for k in all_asset_key_strs if not any(e[0] == k for e in edges)]

    adj_cj: dict[str, set[str]] = defaultdict(set)
    for e in edges:
        adj_cj[e[0]].add(e[1])
        adj_cj[e[1]].add(e[0])
    visited_cj: set[str] = set()
    components_cj: list[set[str]] = []
    for k in all_asset_key_strs:
        if k in visited_cj:
            continue
        comp: set[str] = set()
        queue_cj = [k]
        while queue_cj:
            curr = queue_cj.pop()
            if curr in visited_cj:
                continue
            visited_cj.add(curr)
            comp.add(curr)
            queue_cj.extend(adj_cj[curr] - visited_cj)
        components_cj.append(comp)

    if len(components_cj) > 1:
        layer_order = {"source_capture": 0, "contract_load": 1, "warehouse_transform": 2, "review": 3, "validated_export": 4}
        def comp_order_key(c: set[str]) -> int:
            groups = {key_info[k]["group_name"] for k in c if key_info[k].get("group_name")}
            return min(layer_order.get(g, 99) for g in groups) if groups else 99
        comp_sorted = sorted(components_cj, key=comp_order_key)
        for i in range(len(comp_sorted) - 1):
            prev_comp = comp_sorted[i]
            next_comp = comp_sorted[i + 1]
            prev_terms = [k for k in prev_comp if k in terminal_nodes_set]
            next_roots = [k for k in next_comp if k in root_nodes_set]
            # Match terminals to roots by dataset concept where possible
            for term in prev_terms:
                term_frag = term.split("__")[-1] if "__" in term else term
                matched_roots = [
                    r for r in next_roots
                    if _matches_dataset(r.split("__")[-1] if "__" in r else r, term_frag)
                    or _matches_dataset(term_frag, r.split("__")[-1] if "__" in r else r)
                ]
                targets = matched_roots if matched_roots else next_roots
                for root in targets:
                    edges.append([term, root])
                    key_info[root]["upstream_keys"] = sorted(
                        set(key_info[root]["upstream_keys"]) | {term}
                    )

    # Use Graphviz dot engine for optimal DAG layout
    import graphviz
    dot = graphviz.Digraph(format="plain")
    dot.attr(rankdir="LR", nodesep="0.4", ranksep="0.6")
    dot.attr("node", shape="box", width="0.8", height="0.4")

    # Group nodes by Dagster group for rank constraints
    group_keys: dict[str, list[str]] = defaultdict(list)
    for key_str, info in key_info.items():
        g = info.get("group_name") or "other"
        group_keys[g].append(key_str)

    for key_str in key_info:
        short = key_str.split("__")[-1] if "__" in key_str else key_str
        dot.node(key_str, label=short)

    # Force nodes sharing the same op to the same rank (column),
    # unless there's a dependency between them.
    edge_set = {(e[0], e[1]) for e in edges}
    op_to_nodes: dict[str, list[str]] = defaultdict(list)
    for key_str, info in key_info.items():
        op_to_nodes[info["op_name"]].append(key_str)

    for op_name, op_node_keys in op_to_nodes.items():
        if len(op_node_keys) <= 1:
            continue
        has_internal_dep = any(
            (a, b) in edge_set or (b, a) in edge_set
            for i, a in enumerate(op_node_keys) for b in op_node_keys[i + 1:]
        )
        if not has_internal_dep:
            with dot.subgraph() as s:
                s.attr(rank="same")
                for k in op_node_keys:
                    s.node(k)

    for src, dst in edges:
        dot.edge(src, dst)

    # Parse the plain-text output to extract node positions
    plain = dot.pipe(format="plain").decode("utf-8")
    col_map: dict[str, float] = {}
    row_map: dict[str, float] = {}

    for line in plain.splitlines():
        parts = line.split()
        if parts[0] == "node":
            node_id = parts[1]
            x = float(parts[2])
            y = float(parts[3])
            col_map[node_id] = x
            row_map[node_id] = y

    # Use Graphviz coordinates directly as col/row.
    # Graphviz Y is bottom-to-top; flip for screen (top=0).
    # The UI SVG multiplies col by (nodeW+gapX)≈104px and row by (nodeH+gapY)≈42px.
    # Graphviz uses inches with ranksep/nodesep controlling spacing.
    # We normalize so that adjacent ranks map to ~1 grid unit apart.
    y_max = max(row_map.values()) if row_map else 0
    x_min = min(col_map.values()) if col_map else 0
    x_max = max(col_map.values()) if col_map else 0

    # X → integer column indices (uniform spacing), Y → keep Graphviz's vertical
    # positioning (preserves crossing-free layout), just flip and normalize.
    unique_xs = sorted(set(col_map.values()))
    x_to_col = {x: float(i) for i, x in enumerate(unique_xs)}

    unique_ys = sorted(set(row_map.values()))
    y_gaps = [unique_ys[i + 1] - unique_ys[i] for i in range(len(unique_ys) - 1) if unique_ys[i + 1] - unique_ys[i] > 0.1] if len(unique_ys) > 1 else [1.0]
    y_unit = sorted(y_gaps)[len(y_gaps) // 2] if y_gaps else 1.0  # median gap

    for key in list(col_map.keys()):
        col_map[key] = x_to_col[col_map[key]]
        row_map[key] = (y_max - row_map[key]) / y_unit

    # Build nodes
    nodes = []
    for key_str, info in key_info.items():
        raw_meta = info.get("metadata", {})
        lite_meta = {
            k: _safe_value(v) for k, v in raw_meta.items()
            if any(k.startswith(p) for p in KEEP_META_PREFIXES)
        }
        label = _asset_label(key_str, info)
        nodes.append({
            "id": key_str,
            "label": label,
            "stepKey": info["op_name"],
            "col": col_map.get(key_str, 0),
            "row": row_map[key_str],
            "assetKeys": [key_str],
            "groupName": info["group_name"],
            "groups": [info["group_name"]] if info["group_name"] else [],
            "metadata": lite_meta,
        })

    # Assign pipeline step keys to each node.
    node_by_id = {node["id"]: node for node in nodes}
    assigned_step_keys: set[str] = set()

    # Pass 1: expanded sub-step nodes (step key embedded in node ID)
    for node in nodes:
        matched = [sd for sd in step_defs if sd.step_key in node["id"]]
        if matched:
            node["stepKeys"] = [sd.step_key for sd in matched]
            assigned_step_keys.update(node["stepKeys"])

    # Pass 2: regular nodes — match by op name
    for node in nodes:
        if "stepKeys" in node:
            continue
        matching = [sd.step_key for sd in step_defs if sd.step_key == node["stepKey"] and sd.step_key not in assigned_step_keys]
        if matching:
            node["stepKeys"] = matching
            assigned_step_keys.update(matching)

    # Pass 3: unassigned step keys → match to nodes by group + dataset fragment
    unassigned = [sd for sd in step_defs if sd.step_key not in assigned_step_keys]
    for sd in unassigned:
        best = None
        for node in sorted(nodes, key=lambda n: n["col"], reverse=True):
            if sd.group in (node.get("groups") or []):
                node_frag = node["id"].split("__")[-1] if "__" in node["id"] else node["id"]
                if _matches_dataset(node_frag, sd.step_key):
                    best = node
                    break
        if not best:
            # Fallback: assign to the latest (highest col) node in the same group
            group_nodes = [n for n in nodes if sd.group in (n.get("groups") or [])]
            if group_nodes:
                best = max(group_nodes, key=lambda n: n["col"])
        if best:
            best.setdefault("stepKeys", []).append(sd.step_key)
            assigned_step_keys.add(sd.step_key)

    # Ensure every node has stepKeys
    for node in nodes:
        if "stepKeys" not in node:
            node["stepKeys"] = [node["stepKey"]]

    return {
        "workflow": workflow_config["name"],
        "nodes": sorted(nodes, key=lambda n: (n["col"], n["row"])),
        "edges": edges,
    }


def _asset_label(key_str: str, info: dict[str, Any]) -> str:
    """Generate a short label for an asset node from its key and metadata."""
    # Use the dagflow business name if available
    meta = info.get("metadata", {})
    biz_name = None
    for k in ("dagflow_business_name",):
        v = meta.get(k)
        if isinstance(v, str) and v:
            biz_name = v
            break

    # For expanded sub-step nodes, use the step definition label
    from dagflow_dagster.execution import pipeline_step_definition
    parts_list = key_str.split("__")
    if len(parts_list) >= 3:
        step_key = parts_list[-1]
        sd = pipeline_step_definition(info.get("_pipeline_code", ""), step_key)
        if sd and sd.label:
            return _wrap_label(sd.label)

    if biz_name:
        return _wrap_label(biz_name)

    # Fallback: humanize the short key
    short = parts_list[-1] if "__" in key_str else key_str
    words = short.replace("_", " ").strip().split()
    return _wrap_label(" ".join(w.capitalize() for w in words))


def _wrap_label(text: str, max_line: int = 12) -> str:
    """Wrap text into two lines, keeping each line under max_line chars."""
    if len(text) <= max_line:
        return text
    words = text.split()
    if len(words) <= 1:
        return text
    # Try splitting at each word boundary, pick the most balanced split
    best_split = 1
    best_diff = float("inf")
    for i in range(1, len(words)):
        line1 = " ".join(words[:i])
        line2 = " ".join(words[i:])
        diff = abs(len(line1) - len(line2))
        if diff < best_diff:
            best_diff = diff
            best_split = i
    return " ".join(words[:best_split]) + "\n" + " ".join(words[best_split:])


class _SafeEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if hasattr(o, "value"):
            return o.value
        if isinstance(o, (set, frozenset)):
            return sorted(str(x) for x in o)
        return str(o)

    def encode(self, o: Any) -> str:
        return super().encode(_safe_value(o))


def _safe_value(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (int, float)):
        return v
    if hasattr(v, "value") and not isinstance(v, (dict, list)):
        try:
            return _safe_value(v.value)
        except Exception:
            return str(v)
    if isinstance(v, dict):
        out = {}
        for k, val in v.items():
            try:
                out[str(k)] = _safe_value(val)
            except Exception:
                out[str(k)] = str(val)
        return out
    if isinstance(v, (list, tuple)):
        out_list = []
        for x in v:
            try:
                out_list.append(_safe_value(x))
            except Exception:
                out_list.append(str(x))
        return out_list
    if isinstance(v, (set, frozenset)):
        return sorted(str(x) for x in v)
    return str(v)


def _serialize_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """Convert metadata values to JSON-safe types."""
    return _safe_value(meta)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Generate DAG topology JSON from Dagster definitions")
    parser.add_argument("--out", default="apps/ui-react/src/generated", help="Output directory")
    args = parser.parse_args()

    _add_to_path()

    root = _repo_root()
    config = json.loads((root / "config.json").read_text())

    all_topologies: dict[str, Any] = {}
    for wf in config["workflows"]:
        print(f"Generating topology for {wf['name']}...")
        try:
            topo = _build_topology(wf)
            all_topologies[wf["name"]] = topo
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            continue

    out_dir = root / args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dag_topology.json"
    out_path.write_text(json.dumps(_safe_value(all_topologies), indent=2) + "\n")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
