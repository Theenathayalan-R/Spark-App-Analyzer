"""Map Spark history events to repository source code locations.

Enhanced features:
- Multiple call-site pattern detection (Python/Scala/Java style)
- Stage annotation correlation via in-code markers (# SPARK_STAGE: 12,13)
- Language-aware snippet highlighting (infers from extension)
- Combines job description file:line and stage annotations

Supported job description patterns (examples):
  "job_42 at application.py:120"
  "MyETLJob some/path/pipeline.scala:57"
  "com.example.Job.run(Job.scala:45)" (stack style)

Stage annotations (any language comment style):
  # SPARK_STAGE: 12
  # SPARK_STAGES: 12, 13
  // SPARK_STAGE: 44
  /* SPARK_STAGE: 99 */
  @spark_stage(123)    # Python decorator
"""
from __future__ import annotations
import os
import re
from typing import List, Dict, Any, Optional, Tuple, Set

SNIPPET_RADIUS = 3  # lines before/after

# Regex patterns for call-sites
CALLSITE_PATTERNS = [
    re.compile(r" at ([\\w./\\-]+\.(?:py|scala|java|sql)):(\d+)", re.IGNORECASE),
    re.compile(r"([\\w./\\-]+\.(?:py|scala|java|sql)):(\d+)", re.IGNORECASE),
    re.compile(r"\(([^()]+\.(?:py|scala|java|sql)):(\d+)\)"),  # stack frame style
]

# Stage annotation patterns
STAGE_ANNOTATION_PATTERNS = [
    re.compile(r"SPARK_STAGE[S]?\s*:?\s*([0-9 ,]+)"),
    re.compile(r"@spark_stage\((\d+)\)"),
]

COMMENT_PREFIXES = ('#', '//', '/*', '*')

EXT_LANG_MAP = {
    '.py': 'python',
    '.scala': 'scala',
    '.java': 'java',
    '.sql': 'sql'
}

class CodeMapper:
    def __init__(self, repo_root: str):
        self.repo_root = os.path.abspath(repo_root)
        self._file_cache: Dict[str, List[str]] = {}
        self.stage_annotation_index: Dict[int, List[Dict[str, Any]]] = {}
        self._build_stage_annotation_index()

    def _build_stage_annotation_index(self) -> None:
        for root, _dirs, files in os.walk(self.repo_root):
            for fname in files:
                if not fname.endswith(('.py', '.scala', '.java', '.sql')):
                    continue
                fpath = os.path.join(root, fname)
                lines = self._get_file_lines(fpath)
                for idx, line in enumerate(lines, start=1):
                    if not any(p in line for p in ('SPARK_STAGE', 'spark_stage')):
                        continue
                    for pat in STAGE_ANNOTATION_PATTERNS:
                        m = pat.search(line)
                        if m:
                            raw = m.group(1)
                            stage_ids = self._parse_stage_ids(raw)
                            if not stage_ids:
                                continue
                            snippet, start_line = self._extract_snippet(fpath, idx)
                            lang = self._infer_language(fpath)
                            for sid in stage_ids:
                                self.stage_annotation_index.setdefault(sid, []).append({
                                    'file': os.path.relpath(fpath, self.repo_root),
                                    'line': idx,
                                    'snippet': snippet,
                                    'snippet_start_line': start_line,
                                    'language': lang,
                                    'source': 'annotation'
                                })

    @staticmethod
    def _parse_stage_ids(raw: str) -> List[int]:
        ids: List[int] = []
        for token in re.split(r"[ ,]+", raw.strip()):
            if token.isdigit():
                try:
                    ids.append(int(token))
                except ValueError:
                    continue
        return ids

    def map_jobs_to_code(self, jobs: List[Dict[str, Any]], performance_issues: List[Any]) -> List[Dict[str, Any]]:
        stage_issue_map = self._build_stage_issue_map(performance_issues)
        mappings: List[Dict[str, Any]] = []
        seen: Set[Tuple[int, str, int]] = set()
        for job in jobs:
            desc = job.get('description') or ''
            file_line = self._extract_file_line(desc)
            job_id_val = job.get('job_id')
            try:
                job_id_int = int(job_id_val) if job_id_val is not None else -1
            except Exception:
                job_id_int = -1
            if file_line:
                file_path, line_no = file_line
                snippet, start_line = self._extract_snippet(file_path, line_no)
                lang = self._infer_language(file_path)
                related_issues = self._collect_issue_categories(job, stage_issue_map)
                key = (job_id_int, os.path.relpath(file_path, self.repo_root), line_no)
                if key not in seen:
                    mappings.append({
                        'job_id': job.get('job_id'),
                        'file': key[1],
                        'line': line_no,
                        'snippet': snippet,
                        'snippet_start_line': start_line,
                        'related_issue_categories': sorted(list(related_issues)),
                        'language': lang,
                        'source': 'callsite'
                    })
                    seen.add(key)
            # Stage annotation enrichment
            for sid in job.get('stage_ids', []) or []:
                for ann in self.stage_annotation_index.get(sid, []):
                    key = (job_id_int, ann['file'], ann['line'])
                    if key in seen:
                        continue
                    related_issues = self._collect_issue_categories(job, stage_issue_map)
                    m = ann.copy()
                    m.update({
                        'job_id': job.get('job_id'),
                        'related_issue_categories': sorted(list(related_issues)),
                        'stage_id': sid
                    })
                    mappings.append(m)
                    seen.add(key)
        return mappings

    def _extract_file_line(self, text: str) -> Optional[Tuple[str, int]]:
        for pat in CALLSITE_PATTERNS:
            m = pat.search(text)
            if m:
                rel_path, line_str = m.group(1), m.group(2)
                try:
                    line_no = int(line_str)
                except ValueError:
                    continue
                file_path = self._resolve_source_path(rel_path)
                if file_path:
                    return file_path, line_no
        return None

    def _build_stage_issue_map(self, performance_issues: List[Any]) -> Dict[int, List[str]]:
        mapping: Dict[int, List[str]] = {}
        for issue in performance_issues:
            affected = getattr(issue, 'affected_stages', None) or []
            category = getattr(issue, 'category', 'unknown')
            for stg in affected:
                mapping.setdefault(stg, []).append(category)
        return mapping

    def _collect_issue_categories(self, job: Dict[str, Any], stage_issue_map: Dict[int, List[str]]) -> set:
        categories = set()
        for stg in job.get('stage_ids', []) or []:
            for cat in stage_issue_map.get(stg, []):
                categories.add(cat)
        return categories

    def _resolve_source_path(self, rel_path: str) -> Optional[str]:
        candidate = os.path.join(self.repo_root, rel_path)
        if os.path.isfile(candidate):
            return candidate
        fname = os.path.basename(rel_path)
        for root, _dirs, files in os.walk(self.repo_root):
            if fname in files:
                return os.path.join(root, fname)
        return None

    def _extract_snippet(self, file_path: str, center_line: int) -> Tuple[str, int]:
        lines = self._get_file_lines(file_path)
        start = max(1, center_line - SNIPPET_RADIUS)
        end = min(len(lines), center_line + SNIPPET_RADIUS)
        snippet_lines = []
        for ln in range(start, end + 1):
            prefix = '>> ' if ln == center_line else '   '
            snippet_lines.append(f"{prefix}{ln:5d}: {lines[ln-1].rstrip()}")
        return '\n'.join(snippet_lines), start

    def _get_file_lines(self, file_path: str) -> List[str]:
        if file_path not in self._file_cache:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    self._file_cache[file_path] = f.readlines()
            except Exception:
                self._file_cache[file_path] = []
        return self._file_cache[file_path]

    def _infer_language(self, file_path: str) -> str:
        ext = os.path.splitext(file_path)[1].lower()
        return EXT_LANG_MAP.get(ext, 'python')  # default python for highlighting

__all__ = ["CodeMapper"]
