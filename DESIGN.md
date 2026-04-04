# Design System Specification: The Precision Engine

This document outlines the visual and structural logic for a high-end, data-centric fuel price platform. As designers,
your goal is to transcend the "utility app" aesthetic. We are building a high-performance instrument—an interface that
feels as authoritative as a Bloomberg terminal but as intuitive as a premium editorial layout.

---

### 1. Overview & Creative North Star

**The Creative North Star: "The Digital Cartographer"** This system is defined by precision, depth, and clarity. We
reject the "boxed-in" look of standard SaaS templates. Instead, we use **Intentional Asymmetry** and **Tonal Layering**
to guide the user’s eye. The layout should feel like a sophisticated map: layers of information resting on top of one
another, separated not by lines, but by light and shadow. We favor "breathing room" (generous white space) to make dense
fuel data feel digestible rather than overwhelming.

---

### 2. Colors & Surface Logic

Our palette is rooted in a professional "Command Center" aesthetic, utilizing deep navies and slate grays to establish
trust.

- **Primary Logic:** Use `primary` (#003d9b) for core brand moments and `surface_tint` (#0c56d0) for interactive states.
- **The "Success/Warning" Spectrum:** Fuel prices are volatile. Use `tertiary` (#004e33) and `tertiary_fixed_dim`
  (#4edea3) for "low price" wins. Use `error` (#ba1a1a) only for critical high-price spikes or system failures.
- **The "No-Line" Rule:** **Strictly prohibit 1px solid borders for sectioning.** Boundaries must be defined through
  background color shifts. To separate a sidebar from a map, place a `surface_container_low` panel against a `surface`
  background.
- **The Glass & Gradient Rule:** For floating map overlays or price tooltips, use **Glassmorphism**. Apply
  `surface_container_lowest` at 80% opacity with a `20px` backdrop-blur. Main CTAs should utilize a subtle linear
  gradient from `primary` to `primary_container` to add "soul" and dimension.

---

### 3. Typography: Editorial Authority

We utilize a dual-font strategy to balance character with extreme legibility.

- **Display & Headlines (Manrope):** Use Manrope for all `display` and `headline` tokens. Its geometric yet modern
  construction provides a "tech-forward" editorial feel. High contrast in scale (e.g., a `display-lg` price next to
  `label-sm` unit text) creates a clear information hierarchy.
- **Data & Body (Inter):** Use Inter for all `title`, `body`, and `label` tokens. Inter is optimized for small-scale
  data density, ensuring that fuel prices and map labels remain legible even at high zoom levels.
- **Hierarchy Tip:** Always pair `title-lg` headers with `body-sm` metadata to create a "Professional Digest" look.

---

### 4. Elevation & Depth: Tonal Layering

Traditional drop shadows are often messy. In this system, depth is a product of light and material stacking.

- **The Layering Principle:** Stack surfaces to create hierarchy.
- _Base:_ `surface` (#f8f9ff)
- _Sectioning:_ `surface_container_low` (#eff4ff)
- _Interactive Cards:_ `surface_container_lowest` (#ffffff)
- **Ambient Shadows:** For "floating" elements (like a fuel station detail card), use a shadow with a 24px blur, 0px
  offset, and 6% opacity. The shadow color must be derived from `on_surface` (#0b1c30) to feel like natural ambient
  light.
- **The "Ghost Border" Fallback:** If a divider is functionally required, use `outline_variant` at **15% opacity**.
  Never use 100% opaque lines; they break the fluid, high-end feel of the interface.

---

### 5. Components & Data Patterns

#### **Buttons & Interactive Elements**

- **Primary:** Gradient fill (`primary` to `primary_container`), `xl` roundedness (0.75rem), white text.
- **Secondary:** No fill. Use `surface_container_high` as the background with `primary` text.
- **Ghost Interaction:** Use `surface_tint` at 8% opacity for hover states on icon buttons.
- **Navigation Active State:** Selected navigation items must keep the same size as inactive items. Indicate state
  through a distinctly darker tonal fill aligned with the palette: primary navigation uses a deep `primary` /
  `surface_tint` gradient with white text, while secondary navigation uses a darker navy-slate fill than its base
  surface, also with white text. Avoid extra pills, badges, or size changes for the selected state.

#### **Data Visualization: Sparklines & Maps**

- **Sparklines:** Use `tertiary_fixed_dim` for upward trends (saving money) and `error` for downward trends. Lines
  should be 2px thick with a subtle "glow" (shadow) of the same color.
- **Choropleth Gradients:** When shading map regions by price, transition from `tertiary_container` (cheapest) to
  `surface_dim` (average) to `error_container` (expensive).

#### **Cards & Lists**

- **Rule:** Forbid divider lines between list items. Use `2.5` (0.5rem) vertical spacing from the Spacing Scale to
  separate fuel station entries.
- **Nesting:** A list of prices should sit inside a `surface_container_low` card, with the individual active price item
  highlighted by a `surface_container_highest` background.

#### **Search & Filters**

- **Input Fields:** Use `surface_container_lowest` for the field background. The active state is defined by a 2px
  `surface_tint` "Ghost Border" (20% opacity) rather than a heavy solid line.

---

### 6. Do’s and Don’ts

**Do:**

- **Do** use `display-lg` for the primary fuel price—make it the hero of the screen.
- **Do** use `xl` (0.75rem) roundedness for large containers and `md` (0.375rem) for smaller buttons to create a
  "nested" visual language.
- **Do** embrace white space. If a layout feels "crowded," increase the spacing token by one level (e.g., move from `6`
  to `8`).

**Don't:**

- **Don’t** use pure black (#000000) for text. Always use `on_surface` (#0b1c30) to maintain the sophisticated
  navy-slate tonal range.
- **Don’t** use "Standard Blue" (#0000FF). Only use the specified `primary` and `surface_tint` tokens.
- **Don’t** use traditional "Card Shadows" on every element. Let the background color shifts (`surface` vs
  `surface_container`) do the heavy lifting of separation.
