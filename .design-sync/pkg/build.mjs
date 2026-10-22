// Builds tokens/tailwind.css for the "Fuel Precision Design System" Claude Design project.
//
// The project is hand-authored (React components styled with CSS vars + a ui_kit). This file
// layers the deployed dashboard's Tailwind vocabulary on top, so designs can use the exact
// classes the Jinja templates use and port back 1:1. Inputs come from the live dashboard:
//   - theme: the inline `window.tailwind.config` in fuel-dashboard/app/web/templates/base.html
//   - utilities: Tailwind v3 (same major as cdn.tailwindcss.com) scanning templates + static JS,
//     plus a safelist (brand palette + common layout) for classes the app doesn't use yet
// Additive by design: preflight is OFF and @tailwindcss/forms uses the `class` strategy, so
// no element defaults change and the existing components render exactly as before.
// Output: dist/tailwind.css -> upload as tokens/tailwind.css (imported last from styles.css).
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const DASH = resolve(HERE, '../../fuel-dashboard/app/web');
const DIST = join(HERE, 'dist');
const NM = join(HERE, 'node_modules');

rmSync(DIST, { recursive: true, force: true });
mkdirSync(DIST, { recursive: true });

// -- 1. Tailwind config, lifted from base.html ------------------------------
const baseHtml = readFileSync(join(DASH, 'templates/base.html'), 'utf8');
const m = baseHtml.match(/window\.tailwind\.config\s*=\s*(\{[\s\S]*?\});\s*<\/script>/);
if (!m) throw new Error('tailwind config block not found in base.html');
const twConfig = new Function(`return (${m[1]});`)();
const colors = Object.keys(twConfig.theme.extend.colors);

const colorAlt = colors.map((c) => c.replace(/-/g, '\\-')).join('|');
const sp = '0|0\\.5|1|1\\.5|2|2\\.5|3|3\\.5|4|5|6|7|8|9|10|11|12|14|16|20|24|28|32';
const responsive = ['sm', 'md', 'lg'];
const safelist = [
  { pattern: new RegExp(`^(bg|text|border|ring|from|to|fill|outline)-(${colorAlt})$`), variants: ['hover', 'focus'] },
  { pattern: new RegExp(`^-?(p|px|py|pt|pb|pl|pr|m|mx|my|mt|mb|ml|mr|gap|gap-x|gap-y|space-x|space-y)-(${sp})$`), variants: responsive },
  { pattern: new RegExp(`^(w|h|min-h|size)-(${sp}|full|screen|auto|fit|1\\/2|1\\/3|2\\/3|1\\/4|3\\/4)$`), variants: responsive },
  { pattern: /^max-w-(xs|sm|md|lg|xl|2xl|3xl|4xl|5xl|6xl|7xl|full|prose)$/, variants: responsive },
  { pattern: /^text-(xs|sm|base|lg|xl|2xl|3xl|4xl|5xl|6xl|left|center|right)$/, variants: responsive },
  { pattern: /^font-(headline|body|label|normal|medium|semibold|bold|extrabold)$/ },
  { pattern: /^(leading|tracking)-(none|tight|snug|normal|relaxed|loose|tighter|wide|wider|widest)$/ },
  { pattern: /^rounded(-(t|b|l|r))?(-(none|sm|md|lg|xl|2xl|3xl|full))?$/ },
  { pattern: /^shadow(-(sm|md|lg|xl|2xl|inner|none))?$/, variants: ['hover'] },
  { pattern: /^(grid-cols|col-span)-(1|2|3|4|5|6|12)$/, variants: responsive },
  {
    pattern: /^(flex|flex-row|flex-col|flex-wrap|flex-1|grid|block|inline-flex|inline-block|hidden|items-(start|center|end|baseline)|justify-(start|center|end|between)|shrink-0|grow)$/,
    variants: responsive,
  },
  { pattern: /^(opacity)-(0|50|60|70|80|90|100)$/, variants: ['hover'] },
  { pattern: /^(transition|transition-colors|transition-all|duration-(150|200|300)|ease-out)$/ },
  { pattern: /^form-(input|select|checkbox|radio|textarea)$/ },
  'uppercase', 'truncate', 'tabular-nums', 'overflow-hidden', 'relative', 'absolute', 'sticky', 'top-0', 'inset-0',
  'border', 'border-0', 'border-2', 'border-t', 'border-b', 'mx-auto', 'antialiased', 'cursor-pointer',
];

const serialize = (list) =>
  `[\n    ${list
    .map((s) =>
      typeof s === 'string'
        ? JSON.stringify(s)
        : `{ pattern: ${s.pattern}${s.variants ? `, variants: ${JSON.stringify(s.variants)}` : ''} }`,
    )
    .join(',\n    ')}\n  ]`;

const configPath = join(DIST, 'tailwind.config.cjs');
const cfg = {
  ...twConfig,
  content: [join(DASH, 'templates/**/*.html'), join(DASH, 'static/js/**/*.js')],
  corePlugins: { preflight: false },
  safelist: '__SAFELIST__',
};
writeFileSync(
  configPath,
  `module.exports = ${JSON.stringify(cfg, null, 2).replace('"__SAFELIST__"', serialize(safelist))};\n` +
    `module.exports.plugins = [\n` +
    `  require(${JSON.stringify(join(NM, '@tailwindcss/forms'))})({ strategy: 'class' }),\n` +
    `  require(${JSON.stringify(join(NM, '@tailwindcss/container-queries'))}),\n];\n`,
);

// -- 2. Compile -------------------------------------------------------------
const inputCss = join(DIST, 'input.css');
writeFileSync(inputCss, '@tailwind base;\n@tailwind components;\n@tailwind utilities;\n');
const twOut = join(DIST, 'compiled.css');
execFileSync(join(NM, '.bin/tailwindcss'), ['-c', configPath, '-i', inputCss, '-o', twOut], {
  stdio: ['ignore', 'ignore', 'inherit'],
  env: { ...process.env, BROWSERSLIST_IGNORE_OLD_DATA: '1' },
});

writeFileSync(
  join(DIST, 'tailwind.css'),
  [
    '/* ── Tailwind utilities from the deployed fuel-dashboard ──────────────────',
    '   Generated by .design-sync/pkg/build.mjs in the travel-assistant repo from',
    '   templates/base.html (tailwind.config) + the Jinja templates and static JS.',
    '   Do not edit by hand: re-run the generator. Additive only (no preflight). */',
    '',
    '/* Without preflight, border/divide utilities need a border style to show up. */',
    ':where([class*="border"], [class*="divide-"] > *) { border-style: solid; border-width: 0; }',
    '',
    readFileSync(twOut, 'utf8'),
  ].join('\n'),
);
for (const f of [twOut, inputCss]) rmSync(f);
console.log(`built dist/tailwind.css: ${colors.length} palette colors`);
