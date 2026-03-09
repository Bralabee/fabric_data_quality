"""Jinja2 template rendering for alert messages.

Renders validation results into human-readable alert messages
using Jinja2 templates. Supports both built-in templates (via
PackageLoader) and user-supplied templates (via FileSystemLoader).
"""

import logging

from jinja2 import Environment, PackageLoader, select_autoescape

logger = logging.getLogger(__name__)


class AlertFormatter:
    """Renders validation results into alert messages using Jinja2 templates.

    Args:
        template_dir: Optional path to a directory containing custom .j2
            templates. If None, uses the built-in templates shipped with
            the ``dq_framework.alerting`` package.

    Example::

        formatter = AlertFormatter()
        message = formatter.render("summary.txt.j2", validation_results)
    """

    def __init__(self, template_dir: str | None = None):
        if template_dir:
            from jinja2 import FileSystemLoader

            self._env = Environment(
                loader=FileSystemLoader(template_dir),
                autoescape=select_autoescape(["html"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self._env = Environment(
                loader=PackageLoader("dq_framework.alerting", "templates"),
                autoescape=select_autoescape(["html"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )

    def render(self, template_name: str, results: dict) -> str:
        """Render a validation result dict into a formatted message.

        Args:
            template_name: Name of the Jinja2 template file (e.g. ``summary.txt.j2``).
            results: Validation result dictionary with keys matching the
                template variables (suite_name, success, success_rate, etc.).

        Returns:
            Rendered message string.

        Raises:
            jinja2.TemplateNotFound: If the template file does not exist.
        """
        template = self._env.get_template(template_name)
        return template.render(
            suite_name=results.get("suite_name", "unknown"),
            batch_name=results.get("batch_name", "unknown"),
            success=results.get("success", False),
            success_rate=results.get("success_rate", 0),
            evaluated_checks=results.get("evaluated_checks", 0),
            failed_checks=results.get("failed_checks", 0),
            failed_expectations=results.get("failed_expectations", []),
            severity_stats=results.get("severity_stats", {}),
            threshold_failures=results.get("threshold_failures", []),
            timestamp=results.get("timestamp", ""),
        )
