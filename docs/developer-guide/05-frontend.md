# 5. Frontend

The UI is a single-page application without a build step: Vue 3 (global build, templates are
compiled in the browser), Bootstrap 5 and Chart.js are vendored under `frontend/app/vendor`
(licences included). Plain ES modules, hash router, served by FastAPI from `frontend/app`
(`/` → `index.html`, assets under `/app`, shared Bootstrap/icons under `/static`).

```
frontend/app/
  index.html          shell: sidebar, top bar (live/healthy/alerts), router outlet, toasts, confirm dialog
  css/app.css         design tokens (light/dark), layout, components
  js/main.js          hash router (#/connections/new, #/pipelines/<id> …), global components, polling, WebSocket
  js/api.js           fetch wrapper (timeouts, normalised errors) + typed endpoint groups
  js/store.js         reactive global state (health, alerts, reference data), toasts, confirm(), formatting helpers
  js/ui.js            kpi-card, state-badge, modal-dialog, data-table (filter/sort/paginate/slots), line-chart, json-view, facts-tree
  js/help.js          context help: <help-hint> boxes (dismiss state in localStorage) and the guided tour (TOUR_STEPS + spotlight overlay)
  js/pages/*.js       one component per page
```

## Conventions

* One component per page in `js/pages/`; the template is a string, state in `data()`, server
  calls through `api.js` wrapped in `store.run(fn)` which shows an error toast on failure.
* Global reactive state (`store.js`): health, alerts, connections, mappings, pipelines,
  snapshots, audit stats – refreshed every 5 s while the tab is visible; WebSocket
  (`/api/monitoring/events`) pushes live CDC events and alert toasts.
* Shared components (`ui.js`): `kpi-card`, `state-badge`, `modal-dialog` (teleported, ESC
  closes), `data-table` (filter/sort/pagination, scoped `cell-<key>` slots), `line-chart`
  (Chart.js wrapper, "no data yet" overlay), `json-view`, `facts-tree`.
* Confirmation: `await store.ask(message, {danger, okLabel})`.
* Formatting helpers: `fmt.num/pct/secs/bytes/ts/ago/pos`.
* Theme: `data-bs-theme` on `<html>`, persisted in `localStorage`; CSS tokens in `css/app.css`.
* Engine-specific connection fields are declared once in `pages/connections.js::ENGINES`.
* Context help (`help.js`): put `<help-hint id="unique" title="…">text</help-hint>` at the top of
  a page or dialog; the id is the dismiss key (`localStorage['help.hints']`). Tour steps are
  the `TOUR_STEPS` list (`route`, `target`, `placement`, `title`, `text`); a step finds its
  element through a `data-tour="<target>"` attribute, so templates never contain tour logic.
  `data-tour-fit` on a stretched container (the sidebar `<nav>`) spotlights the union of its
  children instead. The tour auto-starts once on an empty instance (`help.maybeAutoStart`),
  `help.tourDone` in `localStorage` remembers it, `?notour` in the hash suppresses it.

## Legacy pages

The 1.x Jinja2 templates (`frontend/templates`) and scripts (`frontend/static/*.js`) are served
under `/legacy/*` for the visual workflow designer (jsPlumb), the multi-source builder and the
ingestion-pattern tools. They call the same API. Old URLs (`/admin`, `/operations`,
`/monitoring`) redirect into the SPA.

## Browser tests

`tests/ui` – puppeteer-core with a Chromium build shipped through npm (`@sparticuz/chromium`,
no CDN download): `npm run smoke` renders every page and fails on console errors;
`npm run e2e` drives the demo-engine scenario through the real forms. `tests/integration/test_spa_assets.py`
(pytest) checks that the SPA and all page modules are served and parse as ES modules.
