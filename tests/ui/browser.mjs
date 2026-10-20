/* Shared launcher: headless chromium from @sparticuz/chromium (pure npm, works offline behind a registry mirror). */
import chromium from '@sparticuz/chromium';
import puppeteer from 'puppeteer-core';

export const BASE = process.env.CDCSYNC_URL || 'http://localhost:8000';
export const sleep = (ms) => new Promise(r => setTimeout(r, ms));

export async function launch() {
    const browser = await puppeteer.launch({ args: [...chromium.args, '--no-sandbox'], executablePath: process.env.CHROME_PATH || await chromium.executablePath(), headless: true, defaultViewport: { width: 1400, height: 900 } });
    const page = await browser.newPage();
    if (!process.env.TOUR) await page.evaluateOnNewDocument(() => localStorage.setItem('help.tourDone', '1'));  // keep the first-run tour out of the way
    const errors = [];
    page.on('pageerror', e => errors.push('pageerror: ' + e.message));
    page.on('console', async m => { if (m.type() === 'error') errors.push('console.error: ' + m.text()); });
    page.on('requestfailed', r => { if (!r.url().includes('/api/monitoring/events')) errors.push('requestfailed: ' + r.url()); });
    const h = {
        browser, page, errors,
        shot: (n) => page.screenshot({ path: `${process.env.SHOT_DIR || '.'}/${n}.png` }),
        clickText: async (sel, text) => { const els = await page.$$(sel); for (const el of els) { const t = await el.evaluate(e => e.textContent.trim()); if (t.includes(text)) { await el.click(); return true; } } throw new Error(`no ${sel} with text "${text}"`); },
        type: async (sel, text) => { await page.waitForSelector(sel); await page.click(sel, { clickCount: 3 }); await page.type(sel, text); },
        selectByText: async (sel, text) => { await page.waitForSelector(sel); const v = await page.$eval(sel, (s, t) => { const o = [...s.options].find(o => o.textContent.includes(t)); return o ? o.value : null; }, text); if (v == null) throw new Error(`option "${text}" not found in ${sel}`); await page.select(sel, v); },
        text: async (sel = 'main.content') => (await page.$eval(sel, e => e.innerText)).replace(/\s+/g, ' '),
        goto: async (route) => { await page.goto(`${BASE}/#/${route}`, { waitUntil: 'networkidle0', timeout: 30000 }); await sleep(800); },
    };
    return h;
}
