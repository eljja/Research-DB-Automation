import os
import json
import re
import subprocess
import tempfile
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
GEMINI_FALLBACK_MODELS = [
    m.strip()
    for m in os.getenv(
        "GEMINI_FALLBACK_MODELS",
        "gemini-2.5-flash-preview-04-17,gemini-2.5-flash,gemini-2.5-flash-lite",
    ).split(",")
    if m.strip()
]
GEMINI_MODEL_CHAIN = [GEMINI_MODEL] + [m for m in GEMINI_FALLBACK_MODELS if m != GEMINI_MODEL]
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")
DOI_RESOLVER_BASES = [
    base.strip().rstrip("/")
    for base in os.getenv("DOI_RESOLVER_BASES", "https://doi.org").split(",")
    if base.strip()
]
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


def _request_url(url, stream=False):
    return requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=25,
        allow_redirects=True,
        stream=stream,
    )


def _doi_candidate_urls(doi):
    if not doi:
        return []
    return [f"{base}/{quote(doi, safe='')}" for base in DOI_RESOLVER_BASES]


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
    select_fields = (
        "id,ids,display_name,publication_year,publication_date,"
        "abstract_inverted_index,primary_location,best_oa_location,locations"
    )

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


def _pdftotext_available():
    try:
        result = subprocess.run(["pdftotext", "-v"], capture_output=True, timeout=5, check=False)
        return result.returncode == 0 or b"pdftotext" in (result.stderr + result.stdout).lower()
    except (FileNotFoundError, OSError):
        return False


_PDFTOTEXT_AVAILABLE = _pdftotext_available()


def _extract_text_from_pdf_bytes(pdf_bytes):
    if not pdf_bytes or not _PDFTOTEXT_AVAILABLE:
        return ""

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as pdf_file:
        pdf_file.write(pdf_bytes)
        pdf_path = pdf_file.name

    txt_path = f"{pdf_path}.txt"
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", "-nopgbrk", pdf_path, txt_path],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if result.returncode != 0 or not os.path.exists(txt_path):
            return ""
        with open(txt_path, "r", encoding="utf-8", errors="ignore") as txt_file:
            return _clean_text(txt_file.read(), 18000)
    except Exception:
        return ""
    finally:
        for path in [pdf_path, txt_path]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass


def _extract_pdf_links_from_html(html, base_url=""):
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for meta_name in ["citation_pdf_url", "pdf_url"]:
        found = soup.find("meta", attrs={"name": meta_name})
        if found and found.get("content"):
            candidates.append(found["content"].strip())

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        label = _clean_text(anchor.get_text(" ", strip=True)).lower()
        if ".pdf" in href.lower() or "pdf" in label:
            candidates.append(requests.compat.urljoin(base_url, href))

    seen = set()
    ordered = []
    for item in candidates:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _fulltext_candidate_urls(paper, work=None):
    urls = []
    doi = _extract_doi(paper.get("link")) or _extract_doi(paper.get("pub_info")) or _extract_doi(paper.get("title"))

    if paper.get("link"):
        urls.append(paper["link"])
    urls.extend(_doi_candidate_urls(doi))

    if work:
        work_doi = ((work.get("ids") or {}).get("doi") or "").strip()
        if work_doi:
            normalized_work_doi = _extract_doi(work_doi) or work_doi.removeprefix("https://doi.org/").removeprefix("http://doi.org/")
            urls.extend(_doi_candidate_urls(normalized_work_doi))
        for location_key in ["best_oa_location", "primary_location"]:
            location = work.get(location_key) or {}
            for field in ["pdf_url", "landing_page_url"]:
                value = location.get(field)
                if value:
                    urls.append(value)
        for location in work.get("locations") or []:
            for field in ["pdf_url", "landing_page_url"]:
                value = location.get(field)
                if value:
                    urls.append(value)

    seen = set()
    ordered = []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def _fetch_full_text_from_candidates(candidate_urls):
    for candidate_url in candidate_urls:
        try:
            response = _request_url(candidate_url, stream=True)
            content_type = (response.headers.get("Content-Type") or "").lower()
            final_url = response.url
            body = response.content
            response.close()

            if response.status_code >= 400 or not body:
                continue

            if "pdf" in content_type or final_url.lower().endswith(".pdf"):
                pdf_text = _extract_text_from_pdf_bytes(body)
                if len(pdf_text) > 1000:
                    return {"full_text": pdf_text, "abstract": "", "source_url": final_url}
                continue

            html = body.decode(response.encoding or "utf-8", errors="ignore")
            abstract_text, full_text = _extract_abstract_and_text(html)
            if len(full_text) > 1500:
                return {"full_text": full_text, "abstract": abstract_text, "source_url": final_url}

            for pdf_url in _extract_pdf_links_from_html(html, final_url):
                try:
                    pdf_response = _request_url(pdf_url, stream=True)
                    pdf_bytes = pdf_response.content
                    pdf_final_url = pdf_response.url
                    pdf_response.close()
                    pdf_text = _extract_text_from_pdf_bytes(pdf_bytes)
                    if len(pdf_text) > 1000:
                        return {"full_text": pdf_text, "abstract": abstract_text, "source_url": pdf_final_url}
                except Exception:
                    continue
        except Exception:
            continue

    return {"full_text": "", "abstract": "", "source_url": ""}


def _generate_with_fallback(client, prompt):
    last_exc = None
    for model in GEMINI_MODEL_CHAIN:
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json",
                ),
            )
            if model != GEMINI_MODEL_CHAIN[0]:
                log_message("INFO", f"Gemini fallback succeeded with model '{model}'.")
            return response
        except Exception as exc:
            log_message("INFO", f"Gemini model '{model}' failed: {exc}. Trying next fallback.")
            last_exc = exc
    raise last_exc


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


def _upsert_paper(cursor, topic_id, item):
    result_id = item.get("result_id")
    if not result_id:
        return None

    publication_info = item.get("publication_info") or {}
    publication_summary = _publication_summary(publication_info)
    inferred_year = _extract_year_from_text(publication_summary)

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

    cursor.execute("SELECT 1 FROM papers WHERE result_id = ?", (result_id,))
    if cursor.fetchone():
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
        return "updated"
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
        return "inserted"


def sense_scholar(topic_id, limit=20, start=0):
    batch_start = max(int(start or 0), 0)

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
            "num": "20",
            "as_sdt": "7",
            "start": str(batch_start),
        }
        log_message("DEBUG", f"SerpApi request for '{topic['name']}' start={batch_start}", _json_dump(params))

        client = SerpApiClient(api_key=SERPAPI_KEY)
        results = client.search(params)
        results_payload = results.as_dict() if hasattr(results, "as_dict") else dict(results)
        log_message("DEBUG", f"SerpApi response for '{topic['name']}'", _json_dump(results_payload))

        organic_results = results_payload.get("organic_results", [])

        inserted = 0
        updated = 0
        for item in organic_results:
            outcome = _upsert_paper(cursor, topic_id, item)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated":
                updated += 1

        if organic_results:
            conn.commit()

        cursor.execute(
            "UPDATE topics SET updated_at = datetime('now', 'localtime') WHERE id = ?",
            (topic_id,),
        )
        conn.commit()
        log_message(
            "INFO",
            f"SerpApi done — topic='{topic['name']}', start={batch_start}, got={len(organic_results)}, inserted={inserted}, updated={updated}.",
        )
    except Exception as exc:
        conn.rollback()
        log_message("ERROR", f"SerpApi sensing failed for topic {topic_id}: {exc}", traceback.format_exc())
    finally:
        conn.close()


def fetch_abstracts(limit=10, topic_id=None):
    try:
        params = []
        where_clauses = [
            "COALESCE(abstract, '') = ''",
            "status = 'new'",
            "COALESCE(excluded, 0) = 0",
        ]
        if topic_id is not None:
            where_clauses.append("topic_id = ?")
            params.append(topic_id)
        params.append(limit)

        papers = _select_rows(
            f"""
            SELECT result_id, title, link, snippet, pub_info
            FROM papers
            WHERE {' AND '.join(where_clauses)}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        if not papers:
            log_message("INFO", "No abstract candidates found.")
            return

        processed_count = 0
        for paper in papers:
            abstract_text = ""
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
                    year = COALESCE(?, year),
                    year_month = COALESCE(?, year_month),
                    fetch_attempts = COALESCE(fetch_attempts, 0) + 1,
                    status = ?,
                    updated_at = datetime('now', 'localtime')
                WHERE result_id = ?
                """,
                (
                    abstract_text,
                    float(publication_year) if publication_year else None,
                    year_month,
                    status,
                    paper["result_id"],
                ),
            )
            processed_count += 1

        scope = f" for topic_id={topic_id}" if topic_id is not None else ""
        log_message("INFO", f"Abstract fetch completed for {processed_count} papers{scope}.")
    except Exception as exc:
        log_message("ERROR", f"Abstract fetch failed: {exc}", traceback.format_exc())


def fetch_full_papers(limit=10, topic_id=None):
    try:
        params = []
        where_clauses = [
            "COALESCE(full_text, '') = ''",
            "COALESCE(excluded, 0) = 0",
            "COALESCE(link, '') != ''",
        ]
        if topic_id is not None:
            where_clauses.append("topic_id = ?")
            params.append(topic_id)
        params.append(limit)

        papers = _select_rows(
            f"""
            SELECT result_id, topic_id, title, link, snippet, pub_info, abstract
            FROM papers
            WHERE {' AND '.join(where_clauses)}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params),
        )
        if not papers:
            log_message("INFO", "No full paper candidates found.")
            return

        processed_count = 0
        success_count = 0
        for paper in papers:
            try:
                work = _match_openalex_work(
                    paper["title"],
                    paper["link"],
                    paper["pub_info"],
                    paper["snippet"],
                )
                candidates = _fulltext_candidate_urls(paper, work)
                result = _fetch_full_text_from_candidates(candidates)
                full_text = result["full_text"]
                merged_abstract = paper["abstract"] or result["abstract"] or ""

                if full_text:
                    _execute_write(
                        """
                        UPDATE papers
                        SET abstract = CASE WHEN COALESCE(abstract, '') = '' THEN ? ELSE abstract END,
                            full_text = ?,
                            status = CASE
                                WHEN status IN ('new', 'abstract_error') THEN 'abstract_fetched'
                                ELSE status
                            END,
                            updated_at = datetime('now', 'localtime')
                        WHERE result_id = ?
                        """,
                        (merged_abstract, full_text, paper["result_id"]),
                    )
                    success_count += 1
                    log_message(
                        "INFO",
                        f"Full paper fetched for result_id={paper['result_id']}.",
                        result.get("source_url") or None,
                    )
                else:
                    log_message("INFO", f"Full paper fetch found no OA source for result_id={paper['result_id']}.")
            except Exception as exc:
                log_message(
                    "ERROR",
                    f"Full paper fetch failed for {paper['result_id']}: {exc}",
                    traceback.format_exc(),
                )
            processed_count += 1

        scope = f" for topic_id={topic_id}" if topic_id is not None else ""
        log_message("INFO", f"Full paper fetch completed: success={success_count}, processed={processed_count}{scope}.")
    except Exception as exc:
        log_message("ERROR", f"Full paper fetch batch failed: {exc}", traceback.format_exc())


def process_llm(limit=10, topic_id=None):
    try:
        params = []
        where_clauses = [
            """(
                    p.status IN ('abstract_fetched', 'llm_error')
                    OR COALESCE(p.llm_summary, '') = ''
                    OR (p.status = 'llm_processed' AND COALESCE(p.key_material, '') = '')
                  )""",
            "COALESCE(p.excluded, 0) = 0",
            """(
                    COALESCE(p.abstract, '') != ''
                    OR COALESCE(p.full_text, '') != ''
                  )""",
        ]
        if topic_id is not None:
            where_clauses.append("p.topic_id = ?")
            params.append(topic_id)
        params.append(limit)

        papers = _select_rows(
            f"""
            SELECT p.result_id, p.title, p.snippet, p.publication_summary, p.abstract, p.full_text, t.name AS topic_name
            FROM papers p
            LEFT JOIN topics t ON p.topic_id = t.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY p.created_at DESC
            LIMIT ?
            """,
            tuple(params),
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
- "key_material" should be exactly ONE most important functional thin film or material inferred from the title or abstract. Use the common chemical formula or short name (e.g., HfO2, Al2O3, SiNx, IGZO, GST). If unknown, use an empty string.
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
key_material
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
                response = _generate_with_fallback(client, prompt)
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
                        key_material = ?,
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
                        str(payload.get("key_material", "")),
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

        scope = f" for topic_id={topic_id}" if topic_id is not None else ""
        log_message("INFO", f"LLM processing completed for {processed_count} papers{scope}.")
    except Exception as exc:
        log_message("ERROR", f"LLM batch processing failed: {exc}", traceback.format_exc())
