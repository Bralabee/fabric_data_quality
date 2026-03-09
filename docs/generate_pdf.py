#!/usr/bin/env python3
"""
Generate a modern, minimalist PDF from the dual platform architecture MD file.
Uses WeasyPrint for high-quality PDF rendering with custom CSS.
"""

from pathlib import Path

import markdown
from weasyprint import HTML

# --- Paths ---
DOCS_DIR = Path(__file__).parent
MD_FILE = DOCS_DIR / "dual_platform_architecture_justification.md"
PDF_FILE = DOCS_DIR / "dual_platform_architecture_justification.pdf"

# --- Read Markdown ---
md_content = MD_FILE.read_text(encoding="utf-8")

# --- Pre-process: protect reference citations [N] from markdown parser ---
import re


# Replace inline citations like [1], [13] with a placeholder before markdown processing
# but NOT markdown links like [text](url) or reference list entries at start of line
def protect_citations(text):
    # Protect inline citations: space/punctuation + [N] but not [text](url)
    # Match [N] that is NOT followed by ( which would make it a markdown link
    return re.sub(r'\[(\d{1,2})\](?!\()', r'CITE_REF_\1_END', text)

md_content = protect_citations(md_content)

# --- Replace emoji with styled HTML spans for consistent rendering ---
EMOJI_MAP = {
    "✅": '<span class="status-yes">YES</span>',
    "❌": '<span class="status-no">NO</span>',
    "⚠️": '<span class="status-warn">PARTIAL</span>',
    "🔄": '<span class="status-prog">IN PROGRESS</span>',
}

# --- Convert Markdown to HTML ---
html_body = markdown.markdown(
    md_content,
    extensions=["tables", "fenced_code", "codehilite", "toc", "smarty"],
    extension_configs={
        "codehilite": {"css_class": "code-block", "guess_lang": False},
    },
)

# Apply emoji replacements after HTML conversion
for emoji, replacement in EMOJI_MAP.items():
    html_body = html_body.replace(emoji, replacement)

# --- Post-process: restore citation references as styled superscripts ---
def restore_citations(html):
    return re.sub(
        r'CITE_REF_(\d{1,2})_END',
        r'<sup class="cite-ref">[\1]</sup>',
        html,
    )

html_body = restore_citations(html_body)

# --- Post-process: restyle the "Short Version" section as a highlight card ---
SHORT_VERSION_HTML = """
<div class="exec-summary">
  <div class="exec-summary-title">Executive Summary</div>
  <p>I built the AIMS Data Platform and Data Quality Framework to run in <strong>two places</strong>: on Azure DSVMs (outside Fabric) <em>and</em> in Microsoft Fabric. This was a deliberate architectural choice.</p>
  <div class="exec-grid">
    <div class="exec-card">
      <div class="exec-metric">~80%</div>
      <div class="exec-desc">of dev work incurs <strong>no additional Fabric CU charges</strong> &mdash; ~30 hrs/week on DSVMs vs ~8 hrs/week in Fabric per engineer</div>
    </div>
    <div class="exec-card">
      <div class="exec-metric">10&ndash;50&times;</div>
      <div class="exec-desc"><strong>Faster iteration</strong> on DSVMs vs waiting for Fabric notebook sessions</div>
    </div>
    <div class="exec-card">
      <div class="exec-metric">0%</div>
      <div class="exec-desc"><strong>Vendor lock-in</strong> &mdash; code is portable to Databricks or any other platform</div>
    </div>
    <div class="exec-card">
      <div class="exec-metric">74 / 74</div>
      <div class="exec-desc"><strong>Full test coverage</strong> &mdash; proper pytest runs on the DSVMs</div>
    </div>
  </div>
  <p class="exec-footer">This document explains what I did, why I did it, and how it works.</p>
</div>
"""

# Replace the Short Version content
short_version_pattern = re.compile(
    r'(<h2[^>]*>The Short Version</h2>).*?(?=<hr)',
    re.DOTALL,
)
html_body = short_version_pattern.sub(r'\1' + SHORT_VERSION_HTML, html_body)

# --- Post-process: replace the risk register table with styled cards ---
RISK_REGISTER_HTML = """
<div class="risk-register">
  <div class="risk-card risk-high-impact">
    <div class="risk-header">
      <span class="risk-id">R1</span>
      <span class="risk-badges"><span class="risk-badge likelihood-med">Likelihood: Medium</span> <span class="risk-badge impact-high">Impact: High</span></span>
    </div>
    <div class="risk-body">
      <div class="risk-title">Local/cloud environment drift causes production bugs</div>
      <div class="risk-detail"><strong>Mitigation:</strong> CI pipeline runs full test suite against both environments on every PR</div>
      <div class="risk-owner">Owner: Data Engineering Lead</div>
    </div>
  </div>
  <div class="risk-card">
    <div class="risk-header">
      <span class="risk-id">R2</span>
      <span class="risk-badges"><span class="risk-badge likelihood-low">Likelihood: Low</span> <span class="risk-badge impact-med">Impact: Medium</span></span>
    </div>
    <div class="risk-body">
      <div class="risk-title">Fabric pricing model changes, invalidating cost case</div>
      <div class="risk-detail"><strong>Mitigation:</strong> Annual pricing review; architecture remains beneficial for speed &amp; portability regardless</div>
      <div class="risk-owner">Owner: Tech Lead</div>
    </div>
  </div>
  <div class="risk-card">
    <div class="risk-header">
      <span class="risk-id">R3</span>
      <span class="risk-badges"><span class="risk-badge likelihood-med">Likelihood: Medium</span> <span class="risk-badge impact-med">Impact: Medium</span></span>
    </div>
    <div class="risk-body">
      <div class="risk-title">New team members struggle with dual-environment setup</div>
      <div class="risk-detail"><strong>Mitigation:</strong> Onboarding guide, pre-configured Conda <code>environment.yml</code>, pair programming during first sprint</div>
      <div class="risk-owner">Owner: Team Lead</div>
    </div>
  </div>
  <div class="risk-card risk-high-impact">
    <div class="risk-header">
      <span class="risk-id">R4</span>
      <span class="risk-badges"><span class="risk-badge likelihood-low">Likelihood: Low</span> <span class="risk-badge impact-high">Impact: High</span></span>
    </div>
    <div class="risk-body">
      <div class="risk-title">Local development bypasses data governance controls</div>
      <div class="risk-detail"><strong>Mitigation:</strong> No production data on DSVMs; all data access via OneLake RBAC; branch protection enforced</div>
      <div class="risk-owner">Owner: Data Governance Lead</div>
    </div>
  </div>
  <div class="risk-card">
    <div class="risk-header">
      <span class="risk-id">R5</span>
      <span class="risk-badges"><span class="risk-badge likelihood-low">Likelihood: Low</span> <span class="risk-badge impact-med">Impact: Medium</span></span>
    </div>
    <div class="risk-body">
      <div class="risk-title">Fabric API breaking changes affect adapter layer</div>
      <div class="risk-detail"><strong>Mitigation:</strong> Adapter layer isolated behind <code>PlatformFileOps</code> interface; pinned dependency versions</div>
      <div class="risk-owner">Owner: Data Engineering Lead</div>
    </div>
  </div>
</div>
"""

# Replace the risk register table
risk_pattern = re.compile(
    r'(<h3[^>]*>Risk Register</h3>).*?(?=<blockquote)',
    re.DOTALL,
)
html_body = risk_pattern.sub(r'\1' + RISK_REGISTER_HTML, html_body)

# --- Post-process: replace the 3 timeline tables with a single styled timeline ---
import re

TIMELINE_HTML = """
<div class="timeline">
  <div class="timeline-phase">
    <div class="phase-header">
      <span class="phase-badge phase-1">Phase 1</span>
      <span class="phase-title">Local Development</span>
      <span class="phase-weeks">Weeks 1–6</span>
    </div>
    <div class="phase-items">
      <div class="timeline-item"><span class="week-label">Wk 1–2</span><span class="week-task">Architecture &amp; design docs</span></div>
      <div class="timeline-item"><span class="week-label">Wk 2–5</span><span class="week-task">Core Python library</span></div>
      <div class="timeline-item"><span class="week-label">Wk 2–4</span><span class="week-task">CLI scripts for profiling / validation</span></div>
      <div class="timeline-item"><span class="week-label">Wk 5–6</span><span class="week-task">Test coverage (hit 100%)</span></div>
    </div>
  </div>
  <div class="timeline-phase">
    <div class="phase-header">
      <span class="phase-badge phase-2">Phase 2</span>
      <span class="phase-title">Cloud Integration</span>
      <span class="phase-weeks">Weeks 6–9</span>
    </div>
    <div class="phase-items">
      <div class="timeline-item"><span class="week-label">Wk 6–8</span><span class="week-task">Fabric connector (<code>FabricDataQualityRunner</code>)</span></div>
      <div class="timeline-item"><span class="week-label">Wk 8–9</span><span class="week-task">Packaging into <code>.whl</code></span></div>
      <div class="timeline-item"><span class="week-label">Wk 9</span><span class="week-task">Deploy to Fabric Environment</span></div>
    </div>
  </div>
  <div class="timeline-phase">
    <div class="phase-header">
      <span class="phase-badge phase-3">Phase 3</span>
      <span class="phase-title">Validation</span>
      <span class="phase-weeks">Weeks 9–12</span>
    </div>
    <div class="phase-items">
      <div class="timeline-item"><span class="week-label">Wk 9–11</span><span class="week-task">End-to-end testing</span></div>
      <div class="timeline-item"><span class="week-label">Wk 11–12</span><span class="week-task">Go live</span></div>
    </div>
  </div>
</div>
"""

# Find and replace the timeline section in the HTML.
# The pattern: <h3>The Timeline</h3> ... up to <h3>Where Things Stand Now</h3>
timeline_pattern = re.compile(
    r'(<h3[^>]*>The Timeline</h3>).*?(?=<h3[^>]*>Where Things Stand Now</h3>)',
    re.DOTALL,
)
html_body = timeline_pattern.sub(r'\1' + TIMELINE_HTML, html_body)

# --- Modern Minimalist CSS ---
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

@page {
    size: A4;
    margin: 25mm 22mm 28mm 22mm;

    @top-right {
        content: "HS2-DATA-ARCH-2026-001";
        font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;
        font-size: 7pt;
        color: #94a3b8;
        letter-spacing: 0.5pt;
    }

    @bottom-center {
        content: counter(page) " / " counter(pages);
        font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;
        font-size: 7.5pt;
        color: #94a3b8;
    }

    @bottom-right {
        content: "Confidential";
        font-family: 'Inter', 'Helvetica Neue', Arial, sans-serif;
        font-size: 7pt;
        color: #cbd5e1;
        letter-spacing: 1pt;
        text-transform: uppercase;
    }
}

@page :first {
    @top-right { content: none; }
    @bottom-right { content: none; }
}

/* ── Base ────────────────────────────────────────────── */
* {
    box-sizing: border-box;
}

body {
    font-family: 'Inter', 'Helvetica Neue', -apple-system, Arial, sans-serif;
    font-size: 9.5pt;
    line-height: 1.7;
    color: #1e293b;
    max-width: 100%;
    font-weight: 400;
    -webkit-font-smoothing: antialiased;
}

/* ── Headings ────────────────────────────────────────── */
h1 {
    font-size: 24pt;
    font-weight: 700;
    color: #0f172a;
    margin-top: 0;
    margin-bottom: 4pt;
    letter-spacing: -0.5pt;
    line-height: 1.2;
}

h2:first-of-type {
    font-size: 13pt;
    font-weight: 400;
    color: #475569;
    margin-top: 0;
    margin-bottom: 20pt;
    padding-bottom: 14pt;
    border-bottom: 2.5pt solid #3b82f6;
    letter-spacing: -0.2pt;
}

h2 {
    font-size: 14pt;
    font-weight: 600;
    color: #0f172a;
    margin-top: 28pt;
    margin-bottom: 10pt;
    padding-bottom: 5pt;
    border-bottom: 1.5pt solid #e2e8f0;
    letter-spacing: -0.3pt;
    page-break-after: avoid;
}

h3 {
    font-size: 11pt;
    font-weight: 600;
    color: #1e293b;
    margin-top: 18pt;
    margin-bottom: 6pt;
    page-break-after: avoid;
}

h4 {
    font-size: 10pt;
    font-weight: 600;
    color: #334155;
    margin-top: 14pt;
    margin-bottom: 4pt;
}

/* ── Paragraphs & text ───────────────────────────────── */
p {
    margin: 0 0 8pt 0;
    orphans: 3;
    widows: 3;
}

strong {
    font-weight: 600;
    color: #0f172a;
}

em {
    font-style: italic;
    color: #475569;
}

a {
    color: #3b82f6;
    text-decoration: none;
}

/* ── Citation references ───────────────────────────── */
.cite-ref {
    font-size: 6.5pt;
    color: #3b82f6;
    font-weight: 600;
    vertical-align: super;
    line-height: 0;
    margin-left: 1pt;
}

/* ── Executive summary card ─────────────────────── */
.exec-summary {
    margin: 8pt 0 12pt 0;
    page-break-inside: avoid;
}

.exec-summary > p {
    font-size: 9.5pt;
    color: #334155;
    margin-bottom: 12pt;
    line-height: 1.7;
}

.exec-summary-title {
    display: none; /* Visually hidden; h2 already says "The Short Version" */
}

.exec-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 8pt;
    margin-bottom: 12pt;
}

.exec-card {
    flex: 1 1 calc(50% - 8pt);
    background: #f8fafc;
    border: 0.5pt solid #e2e8f0;
    border-left: 3pt solid #3b82f6;
    border-radius: 0 4pt 4pt 0;
    padding: 10pt 12pt;
}

.exec-metric {
    font-size: 18pt;
    font-weight: 700;
    color: #1e40af;
    line-height: 1.2;
    margin-bottom: 3pt;
}

.exec-desc {
    font-size: 8pt;
    color: #475569;
    line-height: 1.5;
}

.exec-desc strong {
    color: #1e293b;
}

.exec-footer {
    font-size: 8.5pt;
    color: #64748b;
    font-style: italic;
}

/* ── Metadata block (doc ref, author, etc.) ──────────── */
p:nth-of-type(1), p:nth-of-type(2), p:nth-of-type(3), p:nth-of-type(4) {
    font-size: 8.5pt;
    color: #64748b;
    margin-bottom: 2pt;
    line-height: 1.5;
}

/* ── Lists ───────────────────────────────────────────── */
ul, ol {
    margin: 6pt 0 10pt 0;
    padding-left: 18pt;
}

li {
    margin-bottom: 3pt;
}

li strong {
    color: #1e293b;
}

/* ── Horizontal rules ────────────────────────────────── */
hr {
    border: none;
    border-top: 1pt solid #e2e8f0;
    margin: 18pt 0;
}

/* ── Tables ──────────────────────────────────────────── */
table {
    width: 100%;
    border-collapse: collapse;
    margin: 10pt 0 14pt 0;
    font-size: 8.5pt;
    page-break-inside: avoid;
    table-layout: fixed;
}

thead {
    background-color: #f8fafc;
}

th {
    font-weight: 600;
    text-align: left;
    padding: 7pt 10pt;
    border-bottom: 2pt solid #e2e8f0;
    color: #334155;
    font-size: 8pt;
    text-transform: uppercase;
    letter-spacing: 0.4pt;
    word-wrap: break-word;
    overflow-wrap: break-word;
}

td {
    padding: 6pt 10pt;
    border-bottom: 0.5pt solid #f1f5f9;
    color: #475569;
    vertical-align: middle;
    word-wrap: break-word;
    overflow-wrap: break-word;
}

tr:nth-child(even) {
    background-color: #fafbfc;
}

/* Right-align currency/number columns */
td:last-child {
    font-variant-numeric: tabular-nums;
}

/* ── Status badges (emoji replacements) ──────────────── */
.status-yes {
    display: inline-block;
    background-color: #dcfce7;
    color: #166534;
    font-size: 6.5pt;
    font-weight: 600;
    padding: 1.5pt 5pt;
    border-radius: 3pt;
    letter-spacing: 0.3pt;
    vertical-align: middle;
}

.status-no {
    display: inline-block;
    background-color: #fee2e2;
    color: #991b1b;
    font-size: 6.5pt;
    font-weight: 600;
    padding: 1.5pt 5pt;
    border-radius: 3pt;
    letter-spacing: 0.3pt;
    vertical-align: middle;
}

.status-warn {
    display: inline-block;
    background-color: #fef3c7;
    color: #92400e;
    font-size: 6.5pt;
    font-weight: 600;
    padding: 1.5pt 5pt;
    border-radius: 3pt;
    letter-spacing: 0.3pt;
    vertical-align: middle;
}

.status-prog {
    display: inline-block;
    background-color: #dbeafe;
    color: #1e40af;
    font-size: 6.5pt;
    font-weight: 600;
    padding: 1.5pt 5pt;
    border-radius: 3pt;
    letter-spacing: 0.3pt;
    vertical-align: middle;
}

/* ── Code ────────────────────────────────────────────── */
code {
    font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 8pt;
    background-color: #f1f5f9;
    padding: 1.5pt 4pt;
    border-radius: 3pt;
    color: #334155;
}

pre {
    font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 7.5pt;
    line-height: 1.6;
    background-color: #f8fafc;
    border: 0.5pt solid #e2e8f0;
    border-left: 3pt solid #3b82f6;
    padding: 12pt 14pt;
    margin: 8pt 0 12pt 0;
    border-radius: 0 4pt 4pt 0;
    overflow-x: hidden;
    white-space: pre-wrap;
    word-wrap: break-word;
    page-break-inside: avoid;
    color: #334155;
}

pre code {
    background: none;
    padding: 0;
    border-radius: 0;
    font-size: inherit;
}

/* ── Blockquotes ─────────────────────────────────────── */
blockquote {
    margin: 10pt 0 12pt 0;
    padding: 10pt 16pt;
    border-left: 3pt solid #3b82f6;
    background-color: #f8fafc;
    border-radius: 0 4pt 4pt 0;
    color: #475569;
    font-size: 9pt;
    line-height: 1.6;
    page-break-inside: avoid;
}

blockquote p {
    margin: 0 0 6pt 0;
}

blockquote p:last-child {
    margin-bottom: 0;
}

blockquote strong {
    color: #1e293b;
}

blockquote em {
    color: #64748b;
}

/* ── Version history table (first table) ─────────────── */

/* ── Emoji replacements for check/cross ──────────────── */

/* ── Timeline ────────────────────────────────────────── */
.timeline {
    page-break-inside: avoid;
    margin: 12pt 0 16pt 0;
}

.timeline-phase {
    margin-bottom: 14pt;
}

.timeline-phase:last-child {
    margin-bottom: 0;
}

.phase-header {
    display: flex;
    align-items: center;
    margin-bottom: 6pt;
    padding-bottom: 5pt;
    border-bottom: 1pt solid #e2e8f0;
}

.phase-badge {
    display: inline-block;
    font-size: 6.5pt;
    font-weight: 700;
    padding: 2pt 6pt;
    border-radius: 3pt;
    letter-spacing: 0.5pt;
    text-transform: uppercase;
    margin-right: 8pt;
    vertical-align: middle;
}

.phase-1 { background-color: #dbeafe; color: #1e40af; }
.phase-2 { background-color: #ede9fe; color: #5b21b6; }
.phase-3 { background-color: #dcfce7; color: #166534; }

.phase-title {
    font-size: 10pt;
    font-weight: 600;
    color: #1e293b;
    margin-right: auto;
}

.phase-weeks {
    font-size: 8pt;
    color: #94a3b8;
    font-weight: 400;
}

.phase-items {
    padding-left: 4pt;
}

.timeline-item {
    display: flex;
    align-items: baseline;
    padding: 4pt 0;
    border-bottom: 0.5pt solid #f1f5f9;
}

.timeline-item:last-child {
    border-bottom: none;
}

.week-label {
    display: inline-block;
    width: 55pt;
    min-width: 55pt;
    font-size: 8pt;
    font-weight: 500;
    color: #64748b;
    flex-shrink: 0;
}

.week-task {
    font-size: 9pt;
    color: #334155;
}

.week-task code {
    font-size: 7.5pt;
}

/* ── Risk register ───────────────────────────────────── */
.risk-register {
    margin: 8pt 0 14pt 0;
}

.risk-card {
    background: #fafbfc;
    border: 0.5pt solid #e2e8f0;
    border-radius: 4pt;
    margin-bottom: 8pt;
    padding: 9pt 12pt;
    page-break-inside: avoid;
}

.risk-card:last-child {
    margin-bottom: 0;
}

.risk-high-impact {
    border-left: 3pt solid #ef4444;
}

.risk-header {
    display: flex;
    align-items: center;
    margin-bottom: 5pt;
}

.risk-id {
    font-size: 8pt;
    font-weight: 700;
    color: #1e293b;
    background: #e2e8f0;
    padding: 1.5pt 5pt;
    border-radius: 3pt;
    margin-right: 8pt;
}

.risk-badges {
    display: flex;
    gap: 4pt;
}

.risk-badge {
    font-size: 6pt;
    font-weight: 600;
    padding: 1.5pt 5pt;
    border-radius: 2pt;
    letter-spacing: 0.3pt;
    text-transform: uppercase;
}

.likelihood-low { background: #dcfce7; color: #166534; }
.likelihood-med { background: #fef3c7; color: #92400e; }
.impact-med { background: #fef3c7; color: #92400e; }
.impact-high { background: #fee2e2; color: #991b1b; }

.risk-body {
    padding-left: 0;
}

.risk-title {
    font-size: 9pt;
    font-weight: 600;
    color: #1e293b;
    margin-bottom: 3pt;
}

.risk-detail {
    font-size: 8.5pt;
    color: #475569;
    line-height: 1.5;
    margin-bottom: 2pt;
}

.risk-detail strong {
    color: #334155;
    font-weight: 600;
}

.risk-owner {
    font-size: 7.5pt;
    color: #94a3b8;
    font-style: italic;
}

/* ── Page breaks ─────────────────────────────────────── */
/* Let content flow naturally — only break if a heading lands
   in the bottom 20% of a page (WeasyPrint handles this via
   orphans/widows + page-break-after: avoid on headings). */

h2 {
    page-break-before: auto;
    page-break-after: avoid;
}

h3 {
    page-break-after: avoid;
}

/* Keep headings attached to the content that follows */
h3 + p, h3 + table, h3 + blockquote, h3 + div,
h2 + h3, h2 + p, h2 + table, h2 + blockquote, h2 + div {
    page-break-before: avoid;
}

/* Avoid breaking inside key blocks */
table, blockquote, pre, .exec-summary, .timeline-phase {
    page-break-inside: avoid;
}

/* ── Print optimisation ──────────────────────────────── */
img {
    max-width: 100%;
}
"""

# --- Full HTML document ---
html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <style>{CSS}</style>
</head>
<body>
{html_body}
</body>
</html>
"""

# --- Generate PDF ---
print(f"Generating PDF from: {MD_FILE.name}")
HTML(string=html_doc).write_pdf(str(PDF_FILE))
print(f"PDF saved to: {PDF_FILE}")
print(f"Size: {PDF_FILE.stat().st_size / 1024:.0f} KB")
