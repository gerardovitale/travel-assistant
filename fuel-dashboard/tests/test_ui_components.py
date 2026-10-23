import re
from pathlib import Path

import config
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined

# Resolve relative to the app code: the Docker test image flattens app/ into /app, so tests/../app/web is wrong there.
_WEB_DIR = Path(config.__file__).resolve().parent / "web"


def _ui():
    env = Environment(loader=FileSystemLoader(str(_WEB_DIR / "templates")), autoescape=True, undefined=StrictUndefined)
    return env.get_template("components/ui.html").module


def test_button_renders_variant_icon_and_attrs():
    html = str(_ui().button("Buscar", variant="primary", icon="search", type="submit", attrs={"data-testid": "x"}))
    assert html.startswith('<button type="submit" class="fp-btn fp-btn--primary"')
    assert 'data-testid="x"' in html
    assert '<span class="material-symbols-outlined" aria-hidden="true">search</span>' in html


def test_button_with_href_renders_link():
    html = str(_ui().button("Ver", href="/trip"))
    assert html.startswith('<a href="/trip" class="fp-btn fp-btn--tonal"')


def test_button_escapes_label():
    html = str(_ui().button("<script>"))
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_icon_button_has_accessible_label():
    html = str(_ui().icon_button("my_location", "Usar mi ubicación"))
    assert 'aria-label="Usar mi ubicación"' in html


def test_field_links_label_and_error_message():
    html = str(_ui().field("Radio (km)", "radius", type="number", error="Valor no válido"))
    assert 'for="fp-radius"' in html and 'id="fp-radius"' in html
    assert 'aria-invalid="true"' in html
    assert 'aria-describedby="fp-radius-msg"' in html
    assert 'id="fp-radius-msg" class="fp-hint fp-hint--error"' in html


def test_field_custom_id_is_not_duplicated():
    html = str(_ui().field("Ubicación", "location", attrs={"id": "loc", "data-testid": "loc-input"}))
    assert html.count('id="loc"') == 1
    assert 'data-testid="loc-input"' in html


def test_select_marks_selected_option():
    html = str(_ui().select("Combustible", "fuel", [("a", "A"), ("b", "B")], selected="b"))
    assert '<option value="b" selected>B</option>' in html
    assert '<option value="a">A</option>' in html


def test_segmented_uses_existing_state_classes():
    html = str(_ui().segmented([("best", "MEJOR"), ("near", "CERCANA")], active="best", label="Modo"))
    assert 'aria-selected="true" class="fp-seg__item seg-active"' in html
    assert 'aria-selected="false" class="fp-seg__item seg-inactive"' in html


def test_banner_role_depends_on_tone():
    assert 'role="alert"' in str(_ui().banner("Error", tone="error"))
    assert 'role="status"' in str(_ui().banner("Info"))


def test_every_component_class_used_by_macros_is_defined():
    css = (_WEB_DIR / "static" / "css" / "components.css").read_text()
    macros = (_WEB_DIR / "templates" / "components" / "ui.html").read_text()
    defined = set(re.findall(r"\.(fp-[a-z0-9_-]+)", css))
    used = set(re.findall(r"\b(fp-[a-z0-9_-]+)", macros)) - {"fp-eyebrow"}
    # fp-btn--{{ variant }} etc. are built dynamically; check the static prefixes resolve.
    used = {u for u in used if not u.endswith("-")}
    assert used <= defined, used - defined


def test_tailwind_colors_all_have_css_tokens():
    base = (_WEB_DIR / "templates" / "base.html").read_text()
    tokens = (_WEB_DIR / "static" / "css" / "tokens.css").read_text()
    referenced = set(re.findall(r"var\(--fp-([a-z-]+)\) / <alpha-value>", base))
    defined = set(re.findall(r"^\s*--fp-([a-z-]+):", tokens, re.M))
    assert referenced, "Tailwind config should map colors to --fp-* tokens"
    assert referenced <= defined, referenced - defined


def test_disabled_link_button_has_no_href():
    html = str(_ui().button("Ver", href="/trip", disabled=True))
    assert "href=" not in html
    assert 'aria-disabled="true"' in html


def test_disabled_hover_rules_do_not_apply():
    css = (_WEB_DIR / "static" / "css" / "components.css").read_text()
    hover_rules = re.findall(r"\.fp-btn--[a-z]+:hover[^{]*", css)
    assert hover_rules
    assert all(':not(:disabled):not([aria-disabled="true"])' in rule for rule in hover_rules)
