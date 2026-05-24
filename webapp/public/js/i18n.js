// Minimal client-side i18n for the landing page.
//
// Markup contract:
//   <h1 data-i18n="hero.title">English default</h1>
//   <p data-i18n-html="some.key">Rich <em>default</em></p>
//   <a data-i18n-attr-href="header.nav.wiki_href" href="/wiki.html">Wiki</a>
//   <meta name="description" data-i18n-attr-content="meta.description" content="…">
//
// The English text stays inline so /index.html paints correctly with no
// flash. A second locale (currently "zh") fetches /locales/<lang>.json and
// swaps every annotated element / attribute.
//
// Locale picked from (in order):
//   1. ?lang=… query parameter
//   2. URL pathname like foo.zh.html, foo.zh-cn.html
//   3. DEFAULT_LOCALE
//
// The matching .zh.html file is allowed to be a literal copy of the English
// HTML — both files run the same JS, the JS just detects the URL and applies
// translations. Keep the two files byte-identical with `npm run sync-html`.

const DEFAULT_LOCALE = "en";
const SUPPORTED_LOCALES = new Set(["en", "zh"]);

function detectLocale() {
  const params = new URLSearchParams(window.location.search);
  const fromParam = params.get("lang");
  if (fromParam && SUPPORTED_LOCALES.has(fromParam)) return fromParam;

  const match = window.location.pathname.match(/\.([a-z]{2}(?:-[a-z]+)?)\.html$/i);
  if (match) {
    const tag = match[1].toLowerCase().split("-")[0];
    if (SUPPORTED_LOCALES.has(tag)) return tag;
  }
  return DEFAULT_LOCALE;
}

async function loadLocale(locale) {
  const resp = await fetch(`/locales/${locale}.json`, { cache: "no-store" });
  if (!resp.ok) throw new Error(`Failed to load ${locale}.json: ${resp.status}`);
  return resp.json();
}

function applyTranslations(dict) {
  document.querySelectorAll("[data-i18n]").forEach(el => {
    const key = el.getAttribute("data-i18n");
    const value = dict[key];
    if (value === undefined) return;
    el.textContent = value;
  });

  document.querySelectorAll("[data-i18n-html]").forEach(el => {
    const key = el.getAttribute("data-i18n-html");
    const value = dict[key];
    if (value === undefined) return;
    el.innerHTML = value;
  });

  document.querySelectorAll("*").forEach(el => {
    for (const attr of Array.from(el.attributes)) {
      if (!attr.name.startsWith("data-i18n-attr-")) continue;
      const targetAttr = attr.name.slice("data-i18n-attr-".length);
      const value = dict[attr.value];
      if (value === undefined) continue;
      el.setAttribute(targetAttr, value);
    }
  });
}

function setLocaleSwitcherActive(locale) {
  document.querySelectorAll(".lang-switch a").forEach(a => {
    const lang = (a.getAttribute("lang") || "").toLowerCase().split("-")[0];
    const isActive = lang === locale;
    a.classList.toggle("active", isActive);
    if (isActive) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
}

async function initI18n() {
  const locale = detectLocale();
  document.documentElement.lang = locale === "zh" ? "zh-CN" : "en";
  setLocaleSwitcherActive(locale);

  if (locale === DEFAULT_LOCALE) return;

  try {
    const dict = await loadLocale(locale);
    applyTranslations(dict);
    if (dict["meta.title"]) document.title = dict["meta.title"];
  } catch (err) {
    console.error("[i18n] failed to load locale", locale, err);
  }
}

initI18n();
