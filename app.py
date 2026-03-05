"""
Coursera Course Scraper — Streamlit UI
Deployed on Streamlit Cloud for easy testing.
"""

from __future__ import annotations

import streamlit as st
import requests
from bs4 import BeautifulSoup
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Constants ───────────────────────────────────────────────────────────────

BASE_URL = "https://www.coursera.org"
API_BASE = "https://www.coursera.org/api"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

API_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


# ─── Search ──────────────────────────────────────────────────────────────────

def scrape_search(query: str, page: int = 1) -> list[dict]:
    """Scrape a single Coursera search results page."""
    params = {"query": query}
    if page > 1:
        params["page"] = str(page)

    url = f"{BASE_URL}/search"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = []

    results_div = soup.find("div", attrs={"aria-label": "Search Results"})
    if not results_div:
        return cards

    for li in results_div.select("ul > li"):
        # Find title link
        title_link = (
            li.select_one("a.cds-CommonCard-titleLink")
            or li.select_one('a[data-click-key*="search_card"]')
            or li.select_one(
                'a[href^="/learn/"], a[href^="/specializations/"], '
                'a[href^="/professional-certificates/"]'
            )
        )
        if not title_link:
            continue

        href = title_link.get("href", "")
        h3 = title_link.find("h3")
        title = (h3.get_text(strip=True) if h3 else title_link.get_text(strip=True))
        if not href or not title:
            continue

        # Rating
        rating_el = li.select_one('[aria-roledescription="rating"] span[aria-hidden="true"]')
        rating = float(rating_el.get_text(strip=True)) if rating_el else None

        # Image
        img = li.select_one("img[src]")
        image_url = img["src"] if img else None

        # Review count
        li_text = li.get_text()
        review_match = re.search(r"([\d,]+)\s*reviews?", li_text, re.I)
        review_count = int(review_match.group(1).replace(",", "")) if review_match else None

        cards.append({
            "platform": "coursera",
            "externalId": href,
            "deepLink": f"{BASE_URL}{href}",
            "title": title,
            "imageUrl": image_url,
            "rating": rating,
            "reviewCount": review_count,
        })

    return cards


def scrape_search_pages(query: str, max_pages: int = 1) -> list[dict]:
    """Scrape multiple search pages."""
    all_cards = []
    for page in range(1, max_pages + 1):
        cards = scrape_search(query, page)
        all_cards.extend(cards)
        if len(cards) < 10:
            break
        if page < max_pages:
            time.sleep(0.8)
    return all_cards


# ─── Detail Scraper ──────────────────────────────────────────────────────────

def scrape_details(external_id: str) -> dict | None:
    """Scrape full details from a Coursera course page."""
    deep_link = external_id if external_id.startswith("http") else f"{BASE_URL}{external_id}"
    pathname = requests.utils.urlparse(deep_link).path

    try:
        resp = requests.get(deep_link, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    body_text = soup.get_text(" ", strip=True)

    # Certification type
    cert_type = "Course"
    if "/specializations/" in pathname:
        cert_type = "Specialization"
    elif "/professional-certificates/" in pathname:
        cert_type = "Professional Certificate"

    # Title
    title_el = soup.select_one("h2.css-1q5srzp")
    og_title = soup.select_one('meta[property="og:title"]')
    title = (
        (title_el.get_text(strip=True) if title_el else "")
        or (og_title["content"] if og_title else "")
    )

    # Rating
    rating_el = soup.select_one('[aria-roledescription="rating"] span[aria-hidden="true"]')
    rating = float(rating_el.get_text(strip=True)) if rating_el else None

    # Review count
    review_match = re.search(r"([\d,]+)\s*reviews?", body_text, re.I)
    review_count = int(review_match.group(1).replace(",", "")) if review_match else None

    # Level
    level = None
    key_info = soup.select_one('[data-e2e="key-information"]')
    key_info_text = key_info.get_text(" ", strip=True) if key_info else body_text
    level_match = re.search(r"(Beginner|Intermediate|Advanced)\s*level", key_info_text, re.I)
    if level_match:
        level = level_match.group(1).capitalize()

    # Duration
    duration_hours = None
    dur_patterns = [
        (r"(\d+)\s*months?\s*at\s*(\d+)\s*hours?\s*a\s*week", "months"),
        (r"(\d+)\s*weeks?\s*at\s*(\d+)\s*hours?\s*a\s*week", "weeks"),
        (r"(\d+)\s*hours?\s*to\s*complete", "hours"),
    ]
    for pat, kind in dur_patterns:
        m = re.search(pat, key_info_text, re.I)
        if m:
            if kind == "months":
                duration_hours = round(int(m.group(1)) * 4.33 * int(m.group(2)))
            elif kind == "weeks":
                duration_hours = int(m.group(1)) * int(m.group(2))
            else:
                duration_hours = int(m.group(1))
            break

    # Skills
    skills = []
    for a in soup.select('a[href*="courses?query="]'):
        text = a.get_text(strip=True)
        if text and text not in skills:
            skills.append(text)

    # Learning outcomes
    outcomes = []
    for li in soup.select('[data-track-component="what_you_will_learn_section"] li'):
        text = li.get_text(strip=True)
        if text and text not in outcomes:
            outcomes.append(text)

    # Enrolled count
    enrolled = None
    enrolled_match = re.search(r"([\d,]+)\s*(?:already\s*)?(?:enrolled|learners?)", body_text, re.I)
    if enrolled_match:
        enrolled = int(enrolled_match.group(1).replace(",", ""))

    # Instructors
    instructors = []
    for a in soup.select('a[data-click-key*="hero_instructor"]'):
        span = a.find("span")
        name = span.get_text(strip=True) if span else a.get_text(strip=True)
        if name and name not in [i["name"] for i in instructors]:
            instructors.append({"name": name, "href": a.get("href", "")})

    # Pricing
    pricing = {}
    price_match = re.search(r"[₹$€£][\d,.]+(?:/(?:month|year))?", body_text)
    if price_match:
        pricing["displayPrice"] = price_match.group(0)
    if "free trial" in body_text.lower():
        pricing["freeTrial"] = True
    if "financial aid" in body_text.lower():
        pricing["financialAid"] = True

    # Partners
    partners = []
    offered_match = re.search(r"Offered by\s+([A-Za-z\s.&]+)", body_text)
    if offered_match:
        partners.append(offered_match.group(1).strip().rstrip("."))

    # Description
    about_div = soup.select_one('div[id="about"]')
    og_desc = soup.select_one('meta[property="og:description"]')
    description = (
        (about_div.get_text(strip=True)[:1000] if about_div else "")
        or (og_desc["content"] if og_desc else "")
    )

    # Image
    og_img = soup.select_one('meta[property="og:image"]')
    image_url = og_img["content"] if og_img else None

    # JSON‑LD structured data
    json_ld_level = None
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            parsed = json.loads(script.string or "")
            items = parsed.get("@graph", [parsed])
            for item in items:
                if item.get("@type") == "Course":
                    json_ld_level = item.get("educationalLevel")
                    break
        except Exception:
            pass

    return {
        "externalId": pathname,
        "deepLink": deep_link,
        "platform": "coursera",
        "certificationType": cert_type,
        "title": title,
        "description": description,
        "imageUrl": image_url,
        "rating": rating,
        "reviewCount": review_count,
        "level": level or json_ld_level,
        "durationHours": duration_hours,
        "skills": skills,
        "learningOutcomes": outcomes,
        "enrolledCount": enrolled,
        "instructors": instructors,
        "pricing": pricing,
        "partners": partners,
    }


# ─── API Enrichment ─────────────────────────────────────────────────────────

def api_fetch(path: str, retries: int = 2):
    """Fetch from Coursera REST API with retries."""
    url = f"{API_BASE}/{path}"
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=API_HEADERS, timeout=12)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else 0
            if 400 <= status < 500 and status != 429:
                return None
            if attempt < retries:
                time.sleep(1 * (attempt + 1))
            else:
                return None
        except Exception:
            if attempt < retries:
                time.sleep(1 * (attempt + 1))
            else:
                return None


def slug_from_path(pathname: str) -> str | None:
    m = re.search(r"/(?:learn|specializations|professional-certificates)/([^/?#]+)", pathname)
    return m.group(1) if m else None


def fetch_course_by_slug(slug: str) -> dict | None:
    path = (
        f"courses.v1?q=slug&slug={slug}"
        f"&includes=instructorIds,partnerIds"
        f"&fields=name,slug,description,photoUrl,workload,courseType,"
        f"primaryLanguages,subtitleLanguages,domainTypes"
    )
    data = api_fetch(path)
    if not data or not data.get("elements"):
        return None
    c = data["elements"][0]
    return {
        "name": c.get("name"),
        "slug": c.get("slug"),
        "description": c.get("description"),
        "photoUrl": c.get("photoUrl"),
        "workload": c.get("workload"),
        "courseType": c.get("courseType"),
        "primaryLanguages": c.get("primaryLanguages", []),
        "subtitleLanguages": c.get("subtitleLanguages", []),
        "domainTypes": c.get("domainTypes", []),
        "instructorIds": c.get("instructorIds", []),
        "partnerIds": c.get("partnerIds", []),
    }


def fetch_instructors(ids: list[str]) -> list[dict]:
    if not ids:
        return []
    path = f"instructors.v1?ids={','.join(ids)}&fields=fullName,title,department,bio,photo"
    data = api_fetch(path)
    if not data or not data.get("elements"):
        return []
    return [
        {"fullName": i.get("fullName"), "title": i.get("title"), "department": i.get("department")}
        for i in data["elements"]
    ]


def fetch_partners(ids: list[str]) -> list[dict]:
    if not ids:
        return []
    path = f"partners.v1?ids={','.join(ids)}&fields=name,shortName,description,logo"
    data = api_fetch(path)
    if not data or not data.get("elements"):
        return []
    return [{"name": p.get("name"), "logo": p.get("logo")} for p in data["elements"]]


def enrich_course(detail: dict) -> dict:
    """Enrich a scraped detail with Coursera API data."""
    slug = slug_from_path(detail.get("externalId", ""))
    if not slug:
        return detail

    course_data = fetch_course_by_slug(slug)
    if not course_data:
        return detail

    instructor_ids = course_data.get("instructorIds", [])
    partner_ids = course_data.get("partnerIds", [])

    api_instructors = fetch_instructors(instructor_ids)
    api_partners = fetch_partners(partner_ids)

    detail["description"] = detail.get("description") or course_data.get("description", "")
    detail["imageUrl"] = detail.get("imageUrl") or course_data.get("photoUrl")

    if api_instructors:
        existing_names = {i["name"].lower() for i in detail.get("instructors", [])}
        for ai in api_instructors:
            if ai["fullName"] and ai["fullName"].lower() not in existing_names:
                detail.setdefault("instructors", []).append({"name": ai["fullName"]})
        # Overwrite with richer data
        detail["apiInstructors"] = api_instructors

    if api_partners:
        detail["partners"] = [p["name"] for p in api_partners]
        detail["partnerLogos"] = {p["name"]: p.get("logo") for p in api_partners}

    detail["workload"] = course_data.get("workload")
    detail["courseType"] = course_data.get("courseType")
    detail["primaryLanguages"] = course_data.get("primaryLanguages", [])

    return detail


# ─── Full Pipeline ───────────────────────────────────────────────────────────

def run_pipeline(query: str, max_pages: int = 1, limit: int = 10, concurrency: int = 3):
    """
    Full scraping pipeline:
      1. Search  →  2. Detail pages  →  3. API enrichment
    Yields progress messages + final results.
    """
    # Step 1: Search
    yield ("status", f"🔍 Searching Coursera for **\"{query}\"** …")
    cards = scrape_search_pages(query, max_pages=max_pages)

    if not cards:
        yield ("status", "⚠️ No courses found. Try a different search term.")
        yield ("done", [])
        return

    # Deduplicate
    seen = set()
    unique = []
    for c in cards:
        if c["externalId"] not in seen:
            seen.add(c["externalId"])
            unique.append(c)

    unique = unique[:limit]
    yield ("status", f"📋 Found **{len(unique)}** courses. Scraping details …")

    # Step 2: Detail pages (parallel)
    details = []
    failed = 0

    def _scrape(card):
        return scrape_details(card["externalId"])

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_scrape, c): c for c in unique}
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                details.append(result)
            else:
                failed += 1
            yield ("progress", f"  Scraped {i}/{len(unique)} detail pages …")

    yield ("status", f"✅ **{len(details)}** details scraped ({failed} failed). Enriching with API …")

    # Step 3: Enrich
    enriched = []
    for i, d in enumerate(details, 1):
        enriched.append(enrich_course(d))
        if i % 3 == 0 or i == len(details):
            yield ("progress", f"  Enriched {i}/{len(details)} courses …")

    yield ("status", f"🎉 Done! **{len(enriched)}** courses ready.")
    yield ("done", enriched)


# ─── Streamlit UI ────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Coursera Course Scraper",
    page_icon="🎓",
    layout="wide",
)

st.title("🎓 Coursera Course Scraper")
st.caption("Search for any topic and get detailed course information from Coursera.")

# ── Sidebar settings ──
with st.sidebar:
    st.header("⚙️ Settings")
    max_pages = st.slider("Search pages", 1, 5, 1, help="Number of search result pages to scrape")
    limit = st.slider("Max courses", 1, 20, 5, help="Maximum number of courses to return")
    concurrency = st.slider("Concurrency", 1, 5, 3, help="Parallel detail page requests")

# ── Main search ──
col1, col2 = st.columns([4, 1])
with col1:
    query = st.text_input(
        "What do you want to learn?",
        placeholder="e.g. Python, Machine Learning, Web Development …",
        label_visibility="collapsed",
    )
with col2:
    search_btn = st.button("🔍 Search", type="primary", use_container_width=True)

if search_btn and query.strip():
    status_container = st.empty()
    progress_container = st.empty()

    results = []

    for msg_type, payload in run_pipeline(query.strip(), max_pages, limit, concurrency):
        if msg_type == "status":
            status_container.info(payload)
        elif msg_type == "progress":
            progress_container.text(payload)
        elif msg_type == "done":
            results = payload

    progress_container.empty()

    if not results:
        st.warning("No courses found. Try a different search term.")
    else:
        status_container.success(f"🎉 Found **{len(results)}** courses for **\"{query}\"**")

        # ── Display results ──
        for i, course in enumerate(results, 1):
            with st.container():
                st.markdown("---")
                cols = st.columns([1, 4])

                # Image column
                with cols[0]:
                    img = course.get("imageUrl")
                    if img:
                        st.image(img, use_container_width=True)
                    else:
                        st.markdown("🖼️ *No image*")

                # Details column
                with cols[1]:
                    title = course.get("title", "Untitled")
                    link = course.get("deepLink", "#")
                    st.markdown(f"### [{title}]({link})")

                    # Badges row
                    badges = []
                    if course.get("rating"):
                        badges.append(f"⭐ **{course['rating']}**")
                    if course.get("reviewCount"):
                        badges.append(f"📝 {course['reviewCount']:,} reviews")
                    if course.get("level"):
                        badges.append(f"📊 {course['level']}")
                    if course.get("certificationType") and course["certificationType"] != "Course":
                        badges.append(f"🏆 {course['certificationType']}")
                    if course.get("enrolledCount"):
                        badges.append(f"👥 {course['enrolledCount']:,} enrolled")
                    if course.get("pricing", {}).get("displayPrice"):
                        badges.append(f"💰 {course['pricing']['displayPrice']}")
                    if course.get("durationHours"):
                        badges.append(f"⏱️ ~{course['durationHours']}h")

                    if badges:
                        st.markdown(" &nbsp;|&nbsp; ".join(badges))

                    # Partners
                    partners = course.get("partners", [])
                    if partners:
                        partner_str = ", ".join(
                            p if isinstance(p, str) else p.get("name", "")
                            for p in partners
                        )
                        if partner_str:
                            st.markdown(f"🏛️ **Offered by:** {partner_str}")

                    # Instructors
                    instructors = course.get("instructors", [])
                    if instructors:
                        names = [i.get("name", "") for i in instructors if i.get("name")]
                        if names:
                            st.markdown(f"👨‍🏫 **Instructors:** {', '.join(names)}")

                    # Skills
                    skills = course.get("skills", [])
                    if skills:
                        skill_tags = " ".join(f"`{s}`" for s in skills[:8])
                        st.markdown(f"🛠️ **Skills:** {skill_tags}")

                    # Languages
                    langs = course.get("primaryLanguages", [])
                    if langs:
                        st.markdown(f"🌐 **Languages:** {', '.join(langs)}")

                    # Description (expandable)
                    desc = course.get("description", "")
                    if desc:
                        with st.expander("📖 Description"):
                            st.write(desc[:800] + ("…" if len(desc) > 800 else ""))

                    # Learning outcomes
                    outcomes = course.get("learningOutcomes", [])
                    if outcomes:
                        with st.expander("🎯 What you'll learn"):
                            for outcome in outcomes[:6]:
                                st.markdown(f"- {outcome}")

        # ── Download JSON ──
        st.markdown("---")
        json_str = json.dumps(results, indent=2, ensure_ascii=False)
        st.download_button(
            label="📥 Download results as JSON",
            data=json_str,
            file_name=f"coursera-{query.replace(' ', '-')}.json",
            mime="application/json",
        )

elif search_btn:
    st.warning("Please enter a search term.")
else:
    # Landing state
    st.markdown(
        """
        <div style="text-align:center; padding: 60px 0; color: #888;">
            <h3>Enter a topic above and click Search</h3>
            <p>Examples: <code>Python</code>, <code>Data Science</code>, <code>UX Design</code>, <code>AWS</code></p>
        </div>
        """,
        unsafe_allow_html=True,
    )
