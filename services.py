import os
import json
import re
import traceback
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from google import genai
from google.genai import types
from serpapi import Client as SerpApiClient

from database import get_db, log_message


def _load_local_env(path=".env"):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_local_env()

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
OPENALEX_API_BASE = "https://api.openalex.org"


def _select_rows(query, params=()):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def _execute_write(query, params=()):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    conn.close()


def _json_dump(value):
    return json.dumps(value, ensure_ascii=False, indent=2)


def _publication_summary(publication_info):
    if not publication_info:
        return ""

    if isinstance(publication_info, dict):
        summary = publication_info.get("summary")
        authors = publication_info.get("authors") or []
        author_names = ", ".join(
            author.get("name", "") for author in authors if isinstance(author, dict) and author.get("name")
        )
        parts = [summary, author_names]
        return " | ".join(part for part in parts if part)

    return str(publication_info)


def _clean_text(text, limit=None):
    if not text:
        return ""

    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned[:limit] if limit else cleaned


def _extract_year_from_text(text):
    if not text:
        return None
    match = re.search(r"(19|20)\d{2}", text)
    return float(match.group(0)) if match else None


def _extract_doi(text):
    if not text:
        return None
    match = re.search(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", text, re.IGNORECASE)
    return match.group(0).rstrip(" .);,]") if match else None


def _openalex_request(path, params=None):
    request_params = {"api_key": OPENALEX_API_KEY}
    if params:
        request_params.update(params)
    response = requests.get(
        f"{OPENALEX_API_BASE}{path}",
        params=request_params,
        headers={"User-Agent": USER_AGENT},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def _rebuild_abstract(abstract_inverted_index):
    if not abstract_inverted_index:
        return ""

    positions = {}
    for token, indexes in abstract_inverted_index.items():
        for position in indexes:
            positions[position] = token
    return _clean_text(" ".join(positions[idx] for idx in sorted(positions.keys())), 4000)


def _match_openalex_work(title, link, pub_info, snippet):
    doi = _extract_doi(link) or _extract_doi(pub_info) or _extract_doi(snippet) or _extract_doi(title)
    select_fields = "id,display_name,publication_year,publication_date,abstract_inverted_index,primary_location,best_oa_location,locations"

    if doi:
        try:
            return _openalex_request(f"/works/https://doi.org/{quote(doi, safe='')}", {"select": select_fields})
        except Exception:
            log_message("DEBUG", f"OpenAlex DOI lookup failed for {doi}", traceback.format_exc())

    try:
        results = _openalex_request(
            "/works",
            {
                "search": title or snippet or "",
                "per-page": 5,
                "select": select_fields,
            },
        ).get("results", [])
    except Exception:
        log_message("DEBUG", f"OpenAlex search failed for title '{title}'", traceback.format_exc())
        return None

    if not results:
        return None

    title_norm = _clean_text((title or "").lower())
    for result in results:
        candidate = _clean_text((result.get("display_name") or "").lower())
        if candidate and title_norm and candidate == title_norm:
            return result
    return results[0]


def _extract_abstract_and_text(html):
    soup = BeautifulSoup(html, "html.parser")

    for element in soup(["script", "style", "noscript", "svg"]):
        element.decompose()

    meta_candidates = [
        ("meta", {"name": "citation_abstract"}),
        ("meta", {"name": "dc.description"}),
        ("meta", {"name": "description"}),
        ("meta", {"property": "og:description"}),
        ("meta", {"name": "twitter:description"}),
    ]
    for tag_name, attrs in meta_candidates:
        found = soup.find(tag_name, attrs=attrs)
        if found and found.get("content"):
            abstract_text = _clean_text(found["content"], 4000)
            if len(abstract_text) > 80:
                break
    else:
        abstract_text = ""

    if not abstract_text:
        selectors = [
            "#abstract",
            ".abstract",
            "section.abstract",
            "[itemprop='description']",
            ".abstractSection",
            ".article__abstract",
            ".c-article-section__content",
        ]
        for selector in selectors:
            found = soup.select_one(selector)
            if found:
                abstract_text = _clean_text(found.get_text(" ", strip=True), 4000)
                if abstract_text:
                    break

    paragraph_texts = [
        _clean_text(paragraph.get_text(" ", strip=True))
        for paragraph in soup.find_all("p")
        if _clean_text(paragraph.get_text(" ", strip=True))
    ]

    if not abstract_text and paragraph_texts:
        abstract_text = _clean_text(" ".join(paragraph_texts[:4]), 4000)

    full_text = _clean_text(" ".join(paragraph_texts), 18000)
    return abstract_text, full_text


def _extract_json_object(text):
    if not text:
        raise ValueError("Gemini returned an empty response")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))


def _to_bool(value, default=True):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1"}:
            return True
        if normalized in {"false", "no", "0"}:
            return False
    return default


def _to_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def sense_scholar(topic_id, limit=20, start=0):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM topics WHERE id = ?", (topic_id,))
        topic = cursor.fetchone()
        if not topic:
            log_message("ERROR", f"Topic {topic_id} not found.")
            return

        params = {
            "engine": "google_scholar",
            "q": topic["query"],
            "hl": "en",
            "lr": "ko",
            "scisbd": "1",
            "num": str(limit),
            "as_sdt": "7",
            "start": str(max(int(start or 0), 0)),
        }

        log_message("INFO", f"SerpApi sensing started for topic '{topic['name']}'.")
        log_message("DEBUG", f"SerpApi request for topic '{topic['name']}'", _json_dump(params))

        client = SerpApiClient(api_key=SERPAPI_KEY)
        results = client.search(params)
        results_payload = results.as_dict() if hasattr(results, "as_dict") else dict(results)
        log_message("DEBUG", f"SerpApi response for topic '{topic['name']}'", _json_dump(results_payload))

        organic_results = results_payload.get("organic_results", [])
        inserted_count = 0
        updated_count = 0

        for item in organic_results:
            result_id = item.get("result_id")
            if not result_id:
                continue

            publication_info = item.get("publication_info") or {}
            publication_summary = _publication_summary(publication_info)
            inferred_year = _extract_year_from_text(publication_summary)

            cursor.execute("SELECT 1 FROM papers WHERE result_id = ?", (result_id,))
            exists = cursor.fetchone() is not None

            payload = (
                topic_id,
                item.get("title"),
                item.get("link"),
                item.get("snippet"),
                json.dumps(publication_info, ensure_ascii=False),
                publication_summary,
                inferred_year,
                result_id,
            )

            if exists:
                cursor.execute(
                    """
                    UPDATE papers
                    SET topic_id = ?,
                        title = COALESCE(?, title),
                        link = COALESCE(?, link),
                        snippet = COALESCE(?, snippet),
                        pub_info = COALESCE(?, pub_info),
                        publication_summary = COALESCE(?, publication_summary),
                        year = COALESCE(year, ?),
                        updated_at = datetime('now', 'localtime')
                    WHERE result_id = ?
                    """,
                    payload,
                )
                updated_count += 1
            else:
                cursor.execute(
                    """
                    INSERT INTO papers (
                        topic_id, title, link, snippet, pub_info,
                        publication_summary, year, result_id, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new')
                    """,
                    payload,
                )
                inserted_count += 1

        cursor.execute(
            "UPDATE topics SET updated_at = datetime('now', 'localtime') WHERE id = ?",
            (topic_id,),
        )
        conn.commit()
        log_message(
            "INFO",
            f"SerpApi sensing finished for '{topic['name']}'. inserted={inserted_count}, updated={updated_count}, total={len(organic_results)}.",
        )
    except Exception as exc:
        conn.rollback()
        log_message("ERROR", f"SerpApi sensing failed for topic {topic_id}: {exc}", traceback.format_exc())
    finally:
        conn.close()


def fetch_abstracts(limit=10):
    try:
        papers = _select_rows(
            """
            SELECT result_id, title, link, snippet, pub_info
            FROM papers
            WHERE COALESCE(abstract, '') = ''
              AND status = 'new'
              AND COALESCE(excluded, 0) = 0
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        if not papers:
            log_message("INFO", "No abstract candidates found.")
            return

        processed_count = 0
        for paper in papers:
            abstract_text = ""
            full_text = ""
            status = "abstract_error"
            publication_year = None
            year_month = None

            try:
                work = _match_openalex_work(
                    paper["title"],
                    paper["link"],
                    paper["pub_info"],
                    paper["snippet"],
                )
                if work:
                    abstract_text = _rebuild_abstract(work.get("abstract_inverted_index"))
                    publication_year = work.get("publication_year")
                    publication_date = work.get("publication_date") or ""
                    year_month = None
                    if publication_year and publication_date:
                        parts = publication_date.split("-")
                        if len(parts) >= 2 and parts[1].isdigit():
                            year_month = f"{int(publication_year):04d}-{int(parts[1]):02d}"
                            publication_year = float(publication_year) + (int(parts[1]) / 12.0)
                    elif publication_year:
                        year_month = f"{int(publication_year):04d}"
                    location = work.get("best_oa_location") or work.get("primary_location") or {}
                    full_text = _clean_text(
                        " ".join(
                            filter(
                                None,
                                [
                                    location.get("landing_page_url"),
                                    location.get("pdf_url"),
                                ],
                            )
                        ),
                        2000,
                    )
                    if abstract_text:
                        status = "abstract_fetched"
                    log_message(
                        "DEBUG",
                        f"OpenAlex match for {paper['result_id']}",
                        _json_dump(
                            {
                                "work_id": work.get("id"),
                                "display_name": work.get("display_name"),
                                "publication_year": work.get("publication_year"),
                                "has_abstract": bool(work.get("abstract_inverted_index")),
                            }
                        ),
                    )
            except Exception as exc:
                log_message(
                    "DEBUG",
                    f"OpenAlex abstract fetch error for {paper['result_id']}",
                    traceback.format_exc() if str(exc) else None,
                )

            _execute_write(
                """
                UPDATE papers
                SET abstract = ?,
                    full_text = ?,
                    year = COALESCE(?, year),
                    year_month = COALESCE(?, year_month),
                    fetch_attempts = COALESCE(fetch_attempts, 0) + 1,
                    status = ?,
                    updated_at = datetime('now', 'localtime')
                WHERE result_id = ?
                """,
                (
                    abstract_text,
                    full_text,
                    float(publication_year) if publication_year else None,
                    year_month,
                    status,
                    paper["result_id"],
                ),
            )
            processed_count += 1

        log_message("INFO", f"Abstract fetch completed for {processed_count} papers.")
    except Exception as exc:
        log_message("ERROR", f"Abstract fetch failed: {exc}", traceback.format_exc())


def process_llm(limit=10):
    try:
        papers = _select_rows(
            """
            SELECT p.result_id, p.title, p.snippet, p.publication_summary, p.abstract, p.full_text, t.name AS topic_name
            FROM papers p
            LEFT JOIN topics t ON p.topic_id = t.id
            WHERE (
                    p.status IN ('abstract_fetched', 'llm_error')
                    OR COALESCE(p.llm_summary, '') = ''
                  )
              AND COALESCE(p.excluded, 0) = 0
              AND (
                    COALESCE(p.abstract, '') != ''
                    OR COALESCE(p.full_text, '') != ''
                  )
            ORDER BY p.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        if not papers:
            log_message("INFO", "No LLM candidates found.")
            return

        client = genai.Client(api_key=GEMINI_API_KEY)
        processed_count = 0

        for paper in papers:
            context = "\n".join(
                [
                    f"Title: {paper['title'] or ''}",
                    f"Publication summary: {paper['publication_summary'] or ''}",
                    f"Snippet: {paper['snippet'] or ''}",
                    f"Abstract: {paper['abstract'] or ''}",
                    f"Full text excerpt: {(paper['full_text'] or '')[:6000]}",
                ]
            )

            prompt = f"""
You are extracting structured data for a research database about non-volatile memory.
Return only one JSON object.

Rules:
- Use concise strings.
- If a value is unknown, use an empty string.
- If the paper is not relevant to the given topic, set "is_relevant_to_topic" to false.
- If you confidently know full-paper details beyond the abstract from prior model knowledge, you may use them. Otherwise leave unknown fields blank.
- "mechanism" should be one of CTF, FG, Re, EC, FE, PC, SOM, or a precise alternative.
- "architecture" should capture array/cell structure such as vertical channel, horizontal channel, vertical gate, NAND, NOR, crossbar.
- "stack" should capture the major gate stack or device stack family such as MAONOS, MIM, MFM, MINFIS.
- "key_film" should capture the main functional thin film(s) if present.
- "tr_structure" should describe the device stack or structure in detail.
- "memory_window" should be a concise display string. Prefer a representative value only.
- "memory_window_voltage" should be a representative numeric voltage value if memory window is reported in voltage units.
- "memory_window_ratio" should be a representative numeric on/off ratio if memory window is reported as current ratio.
- "voltage" should be a concise display string and "voltage_value" should be one representative operating voltage as a pure number.
- "speed" should be a concise display string and "speed_seconds" should be one representative speed converted to seconds.
- "retention" should be a concise display string and "retention_year1" should be the estimated retained percentage after 1 year as a number from 0 to 100 when possible.
- "endurance" should be a concise display string and "endurance_cycles" should be the representative cycle count as a pure number.
- "category" should be DRAM, Flash, Logic, or Other.
- "year" must be a 4-digit year if known.
- "month" must be 1-12 if known, otherwise 6.

Return JSON keys exactly:
is_relevant_to_topic
mechanism
architecture
stack
key_film
tr_structure
year
month
memory_window
memory_window_voltage
memory_window_ratio
voltage
voltage_value
speed
speed_seconds
retention
retention_year1
endurance
endurance_cycles
other_features
uniqueness
category
comparison_notes
llm_summary

Topic:
{paper['topic_name'] or 'NVM'}

Paper content:
{context}
""".strip()

            try:
                log_message("DEBUG", f"Gemini request for {paper['result_id']}", prompt)
                response = client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                    ),
                )
                log_message("DEBUG", f"Gemini response for {paper['result_id']}", response.text)
                payload = _extract_json_object(response.text)
                is_relevant = _to_bool(payload.get("is_relevant_to_topic", True), True)

                year = payload.get("year") or ""
                month = payload.get("month") or 6
                numeric_year = None
                year_month = ""

                if year and str(year).isdigit():
                    numeric_year = float(year) + (float(month) / 12.0)
                    year_month = f"{int(year):04d}-{int(month):02d}"
                else:
                    fallback_year = _extract_year_from_text(paper["publication_summary"])
                    if fallback_year:
                        numeric_year = fallback_year + (float(month) / 12.0)
                        year_month = f"{int(fallback_year):04d}-{int(month):02d}"

                _execute_write(
                    """
                    UPDATE papers
                    SET mechanism = ?,
                        architecture = ?,
                        stack = ?,
                        key_film = ?,
                        tr_structure = ?,
                        year = ?,
                        year_month = ?,
                        memory_window = ?,
                        memory_window_voltage = ?,
                        memory_window_ratio = ?,
                        voltage = ?,
                        voltage_value = ?,
                        speed = ?,
                        speed_seconds = ?,
                        retention = ?,
                        retention_year1 = ?,
                        endurance = ?,
                        endurance_cycles = ?,
                        other_features = ?,
                        uniqueness = ?,
                        category = ?,
                        comparison_notes = ?,
                        llm_summary = ?,
                        excluded = ?,
                        llm_attempts = COALESCE(llm_attempts, 0) + 1,
                        status = 'llm_processed',
                        updated_at = datetime('now', 'localtime')
                    WHERE result_id = ?
                    """,
                    (
                        str(payload.get("mechanism", "")),
                        str(payload.get("architecture", "")),
                        str(payload.get("stack", "")),
                        str(payload.get("key_film", "")),
                        str(payload.get("tr_structure", "")),
                        numeric_year,
                        year_month,
                        str(payload.get("memory_window", "")),
                        _to_float(payload.get("memory_window_voltage")),
                        _to_float(payload.get("memory_window_ratio")),
                        str(payload.get("voltage", "")),
                        _to_float(payload.get("voltage_value")),
                        str(payload.get("speed", "")),
                        _to_float(payload.get("speed_seconds")),
                        str(payload.get("retention", "")),
                        _to_float(payload.get("retention_year1")),
                        str(payload.get("endurance", "")),
                        _to_float(payload.get("endurance_cycles")),
                        str(payload.get("other_features", "")),
                        str(payload.get("uniqueness", "")),
                        str(payload.get("category", "")),
                        str(payload.get("comparison_notes", "")),
                        str(payload.get("llm_summary", "")),
                        0 if is_relevant else 1,
                        paper["result_id"],
                    ),
                )
                processed_count += 1
            except Exception as exc:
                log_message("ERROR", f"LLM processing failed for {paper['result_id']}: {exc}", traceback.format_exc())
                _execute_write(
                    """
                    UPDATE papers
                    SET llm_attempts = COALESCE(llm_attempts, 0) + 1,
                        status = 'llm_error',
                        updated_at = datetime('now', 'localtime')
                    WHERE result_id = ?
                    """,
                    (paper["result_id"],),
                )

        log_message("INFO", f"LLM processing completed for {processed_count} papers.")
    except Exception as exc:
        log_message("ERROR", f"LLM batch processing failed: {exc}", traceback.format_exc())
