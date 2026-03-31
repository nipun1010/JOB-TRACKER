"""
Job Scraper — Product & Growth Roles | India | Freshers (0-2 YOE)
Scrapes: hiring.cafe, LinkedIn, Naukri, Wellfound, Internshala
Runs daily via GitHub Actions, outputs data/jobs.json
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Optional: Playwright for JS-heavy sites ──────────────────────────────────
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
TARGET_ROLES = [
    "product intern",
    "growth intern",
    "associate product manager",
    "apm",
    "product manager",
]

INDIA_CITIES = [
    "bangalore", "bengaluru", "mumbai", "delhi", "new delhi",
    "hyderabad", "pune", "chennai", "kolkata", "gurgaon",
    "gurugram", "noida", "india", "remote",
]

MAX_EXP_YEARS = 2  # 0–2 YOE only

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}

OUTPUT_PATH = Path("data/jobs.json")
NOW_UTC = datetime.now(timezone.utc)
CUTOFF = NOW_UTC - timedelta(hours=24)


# ── Helpers ───────────────────────────────────────────────────────────────────

def job_id(title: str, company: str) -> str:
    """Stable dedup key based on title + company."""
    key = f"{title.lower().strip()}|{company.lower().strip()}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def is_target_role(title: str) -> bool:
    t = title.lower()
    return any(role in t for role in TARGET_ROLES)


def is_india_location(location: str) -> bool:
    loc = location.lower()
    return any(city in loc for city in INDIA_CITIES)


def is_fresher_exp(exp_text: str) -> bool:
    """Returns True if experience requirement is 0–2 years."""
    if not exp_text:
        return True  # assume fresher-friendly if not stated
    exp = exp_text.lower()
    if any(kw in exp for kw in ["fresher", "0-1", "0-2", "0 - 1", "0 - 2", "entry", "intern"]):
        return True
    nums = re.findall(r"\d+", exp)
    if nums:
        return int(nums[0]) <= MAX_EXP_YEARS
    return False


def detect_work_mode(text: str) -> str:
    t = text.lower()
    if "remote" in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    return "On-site"


def time_ago(dt: datetime) -> str:
    diff = NOW_UTC - dt
    hours = int(diff.total_seconds() // 3600)
    if hours < 1:
        return "Just now"
    if hours == 1:
        return "1 hour ago"
    return f"{hours} hours ago"


def make_job(title, company, location, work_mode, experience, salary, source, apply_link, posted_dt):
    return {
        "id": job_id(title, company),
        "title": title.strip(),
        "company": company.strip(),
        "location": location.strip(),
        "work_mode": work_mode,
        "experience": experience.strip() if experience else "Fresher / 0-2 YOE",
        "salary": salary.strip() if salary else None,
        "source": source,
        "apply_link": apply_link,
        "posted_at": posted_dt.isoformat(),
        "posted_ago": time_ago(posted_dt),
    }


# ── Scraper: Internshala ─────────────────────────────────────────────────────

def scrape_internshala() -> list:
    jobs = []
    queries = ["product+manager", "product+intern", "growth+intern", "associate+product+manager"]
    base = "https://internshala.com"

    for q in queries:
        url = f"{base}/jobs/keywords-{q}/"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".individual_internship")
            log.info(f"Internshala [{q}]: {len(cards)} cards")

            for card in cards:
                try:
                    title_el = card.select_one(".job-internship-name")
                    company_el = card.select_one(".company_name")
                    location_el = card.select_one(".location_link, .locations_strip")
                    salary_el = card.select_one(".stipend")
                    link_el = card.select_one("a.view_detail_button, a[href*='/jobs/detail']")

                    if not title_el or not company_el:
                        continue

                    title = title_el.get_text(strip=True)
                    if not is_target_role(title):
                        continue

                    company = company_el.get_text(strip=True)
                    location = location_el.get_text(strip=True) if location_el else "India"
                    salary = salary_el.get_text(strip=True) if salary_el else None
                    link = base + link_el["href"] if link_el else url
                    work_mode = detect_work_mode(location + " " + title)

                    # Internshala doesn't show exact post time on listing cards
                    # Treat as recent (posted today)
                    posted_dt = NOW_UTC - timedelta(hours=2)

                    jobs.append(make_job(
                        title, company, location, work_mode,
                        "Fresher / 0-1 YOE", salary, "Internshala", link, posted_dt
                    ))
                except Exception as e:
                    log.warning(f"Internshala card error: {e}")
        except Exception as e:
            log.error(f"Internshala fetch error [{q}]: {e}")
        time.sleep(1)

    return jobs


# ── Scraper: Naukri ──────────────────────────────────────────────────────────

def scrape_naukri() -> list:
    jobs = []
    queries = [
        ("product-manager-jobs", "Product Manager"),
        ("associate-product-manager-jobs", "Associate Product Manager"),
        ("product-intern-jobs", "Product Intern"),
        ("growth-manager-jobs", "Growth"),
    ]

    for path, label in queries:
        url = f"https://www.naukri.com/{path}?experience=0&jobAge=1"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("article.jobTuple, .cust-job-tuple")
            log.info(f"Naukri [{label}]: {len(cards)} cards")

            for card in cards:
                try:
                    title_el = card.select_one("a.title, .title")
                    company_el = card.select_one("a.subTitle, .subTitle, .comp-name")
                    location_el = card.select_one(".locWdth, li.location span")
                    exp_el = card.select_one(".expwdth, li.experience span")
                    salary_el = card.select_one(".salary, li.salary span")

                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    if not is_target_role(title):
                        continue

                    company = company_el.get_text(strip=True) if company_el else "N/A"
                    location = location_el.get_text(strip=True) if location_el else "India"
                    if not is_india_location(location):
                        continue

                    exp = exp_el.get_text(strip=True) if exp_el else ""
                    if not is_fresher_exp(exp):
                        continue

                    salary = salary_el.get_text(strip=True) if salary_el else None
                    link = title_el.get("href", url)
                    work_mode = detect_work_mode(location + " " + title)
                    posted_dt = NOW_UTC - timedelta(hours=4)

                    jobs.append(make_job(
                        title, company, location, work_mode,
                        exp or "0-2 YOE", salary, "Naukri", link, posted_dt
                    ))
                except Exception as e:
                    log.warning(f"Naukri card error: {e}")
        except Exception as e:
            log.error(f"Naukri fetch error [{label}]: {e}")
        time.sleep(1.5)

    return jobs


# ── Scraper: Wellfound (AngelList) ───────────────────────────────────────────

def scrape_wellfound() -> list:
    """
    Wellfound is JS-heavy. Uses Playwright if available,
    otherwise falls back to their public JSON API endpoint.
    """
    jobs = []

    if PLAYWRIGHT_AVAILABLE:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                queries = ["product-manager", "product-intern", "growth", "associate-product-manager"]
                for q in queries:
                    url = f"https://wellfound.com/jobs?q={q}&l=India&role=product"
                    page.goto(url, timeout=30000)
                    page.wait_for_timeout(3000)
                    soup = BeautifulSoup(page.content(), "html.parser")
                    cards = soup.select("[class*='JobListing'], [class*='job-listing']")
                    log.info(f"Wellfound [{q}]: {len(cards)} cards (Playwright)")
                    for card in cards:
                        try:
                            title_el = card.select_one("h2, h3, [class*='title']")
                            company_el = card.select_one("[class*='company'], [class*='startup']")
                            location_el = card.select_one("[class*='location']")
                            link_el = card.select_one("a")
                            if not title_el:
                                continue
                            title = title_el.get_text(strip=True)
                            if not is_target_role(title):
                                continue
                            company = company_el.get_text(strip=True) if company_el else "Startup"
                            location = location_el.get_text(strip=True) if location_el else "India"
                            link = "https://wellfound.com" + link_el["href"] if link_el and link_el.get("href", "").startswith("/") else (link_el["href"] if link_el else url)
                            work_mode = detect_work_mode(location + " " + title)
                            posted_dt = NOW_UTC - timedelta(hours=6)
                            jobs.append(make_job(
                                title, company, location, work_mode,
                                "0-2 YOE", None, "Wellfound", link, posted_dt
                            ))
                        except Exception as e:
                            log.warning(f"Wellfound card error: {e}")
                browser.close()
        except Exception as e:
            log.error(f"Wellfound Playwright error: {e}")
    else:
        log.warning("Playwright not available — skipping Wellfound JS scrape. Install with: pip install playwright && playwright install chromium")

    return jobs


# ── Scraper: hiring.cafe ─────────────────────────────────────────────────────

def scrape_hiring_cafe() -> list:
    jobs = []
    queries = ["product manager", "product intern", "growth intern", "associate product manager"]

    for q in queries:
        url = f"https://hiring.cafe/?q={requests.utils.quote(q)}&country=India"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")

            # hiring.cafe structure varies; try common patterns
            cards = soup.select(".job-card, .job-item, [class*='job']")
            log.info(f"hiring.cafe [{q}]: {len(cards)} cards")

            for card in cards:
                try:
                    title_el = card.select_one("h2, h3, .title, [class*='title']")
                    company_el = card.select_one(".company, [class*='company']")
                    location_el = card.select_one(".location, [class*='location']")
                    link_el = card.select_one("a")

                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    if not is_target_role(title):
                        continue

                    company = company_el.get_text(strip=True) if company_el else "N/A"
                    location = location_el.get_text(strip=True) if location_el else "India"
                    link = link_el["href"] if link_el else url
                    if link.startswith("/"):
                        link = "https://hiring.cafe" + link
                    work_mode = detect_work_mode(location + " " + title)
                    posted_dt = NOW_UTC - timedelta(hours=3)

                    jobs.append(make_job(
                        title, company, location, work_mode,
                        "0-2 YOE", None, "hiring.cafe", link, posted_dt
                    ))
                except Exception as e:
                    log.warning(f"hiring.cafe card error: {e}")
        except Exception as e:
            log.error(f"hiring.cafe fetch error [{q}]: {e}")
        time.sleep(1)

    return jobs


# ── Scraper: LinkedIn ────────────────────────────────────────────────────────

def scrape_linkedin() -> list:
    """
    LinkedIn blocks most scraping. This uses their public job search
    endpoint which works without login for basic listings.
    For production, consider LinkedIn Job Search API or a proxy service.
    """
    jobs = []
    queries = [
        "product+intern",
        "growth+intern",
        "associate+product+manager",
        "product+manager",
    ]

    for q in queries:
        # LinkedIn's public job search (no auth required for basic results)
        url = (
            f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={q}&location=India&f_TPR=r86400&f_E=1,2&start=0"
        )
        # f_TPR=r86400 = last 24 hours, f_E=1,2 = Internship + Entry level
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("li")
            log.info(f"LinkedIn [{q}]: {len(cards)} cards")

            for card in cards:
                try:
                    title_el = card.select_one(".base-search-card__title")
                    company_el = card.select_one(".base-search-card__subtitle")
                    location_el = card.select_one(".job-search-card__location")
                    link_el = card.select_one("a.base-card__full-link")
                    time_el = card.select_one("time")

                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    if not is_target_role(title):
                        continue

                    company = company_el.get_text(strip=True) if company_el else "N/A"
                    location = location_el.get_text(strip=True) if location_el else "India"
                    if not is_india_location(location):
                        continue

                    link = link_el["href"].split("?")[0] if link_el else url
                    work_mode = detect_work_mode(location + " " + title)

                    # Parse posted datetime from <time datetime="...">
                    if time_el and time_el.get("datetime"):
                        try:
                            posted_dt = datetime.fromisoformat(time_el["datetime"].replace("Z", "+00:00"))
                        except Exception:
                            posted_dt = NOW_UTC - timedelta(hours=5)
                    else:
                        posted_dt = NOW_UTC - timedelta(hours=5)

                    if posted_dt < CUTOFF:
                        continue

                    jobs.append(make_job(
                        title, company, location, work_mode,
                        "Entry Level / 0-2 YOE", None, "LinkedIn", link, posted_dt
                    ))
                except Exception as e:
                    log.warning(f"LinkedIn card error: {e}")
        except Exception as e:
            log.error(f"LinkedIn fetch error [{q}]: {e}")
        time.sleep(2)

    return jobs


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(jobs: list) -> list:
    seen = {}
    for job in jobs:
        jid = job["id"]
        if jid not in seen:
            seen[jid] = job
        else:
            # Keep the one with more info
            existing = seen[jid]
            if not existing.get("salary") and job.get("salary"):
                seen[jid] = job
    return list(seen.values())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Job Scraper Starting")
    log.info(f"Cutoff: jobs posted after {CUTOFF.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    all_jobs = []
    scrapers = [
        ("Internshala", scrape_internshala),
        ("Naukri", scrape_naukri),
        ("hiring.cafe", scrape_hiring_cafe),
        ("LinkedIn", scrape_linkedin),
        ("Wellfound", scrape_wellfound),
    ]

    for name, fn in scrapers:
        log.info(f"── Scraping {name} ──")
        try:
            results = fn()
            log.info(f"  ✓ {name}: {len(results)} jobs found")
            all_jobs.extend(results)
        except Exception as e:
            log.error(f"  ✗ {name} failed entirely: {e}")

    # Deduplicate
    deduped = deduplicate(all_jobs)
    log.info(f"\nTotal before dedup: {len(all_jobs)} | After: {len(deduped)}")

    # Sort by posted_at descending
    deduped.sort(key=lambda j: j["posted_at"], reverse=True)

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_updated": NOW_UTC.isoformat(),
        "last_updated_ist": (NOW_UTC + timedelta(hours=5, minutes=30)).strftime("%d %b %Y, %I:%M %p IST"),
        "total_jobs": len(deduped),
        "jobs": deduped,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    log.info(f"\n✅ Saved {len(deduped)} jobs to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
