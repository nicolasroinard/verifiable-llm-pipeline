"""extractor.py — ODT validation and text extraction for the ODT V7.5 pipeline.

Responsible for:
- Validating an ODT file (ZIP integrity, mandatory entries)
- Parsing content.xml into ordered paragraph/heading blocks
- Grouping paragraphs into deterministic fixed-size segments
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

TEXT_NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
    "xlink": "http://www.w3.org/1999/xlink",
}


def validate_input(odt_path: Path) -> dict:
    """Check that *odt_path* is a readable, valid ODT (ZIP) with mandatory entries.

    Returns a dict with keys: readable, zip_valid, mandatory_entries_present, errors.
    """
    result = {
        "readable": False,
        "zip_valid": False,
        "mandatory_entries_present": False,
        "errors": [],
    }
    if not odt_path.exists():
        result["errors"].append("ODT file does not exist.")
        return result
    try:
        with zipfile.ZipFile(odt_path, "r") as zf:
            bad = zf.testzip()
            result["readable"] = True
            result["zip_valid"] = bad is None
            names = set(zf.namelist())
            required = {"content.xml", "styles.xml", "meta.xml", "META-INF/manifest.xml"}
            result["mandatory_entries_present"] = required.issubset(names)
            if bad:
                result["errors"].append(f"Corrupted zip entry: {bad}")
            missing = sorted(required - names)
            if missing:
                result["errors"].append(f"Missing required entries: {missing}")
    except Exception as exc:
        result["errors"].append(str(exc))
    return result


def extract_text_blocks(content_xml: bytes) -> list:
    """Parse *content_xml* and return an ordered list of non-empty text blocks.

    Each block is a dict with: kind, sequence_in_kind, absolute_sequence,
    text, xml_tag, style_name, xml_path.
    Both <text:p> (paragraph) and <text:h> (heading) elements are included.
    """
    root = ET.fromstring(content_xml)
    office_text = root.find(".//office:body/office:text", TEXT_NS)
    paragraphs = []
    if office_text is None:
        return paragraphs
    para_tags = {
        f"{{{TEXT_NS['text']}}}p",
        f"{{{TEXT_NS['text']}}}h",
    }

    def flatten_text(elem):
        return "".join(elem.itertext()).strip()

    counters: dict = {}
    absolute_sequence = 0
    for elem in office_text.iter():
        if elem.tag not in para_tags:
            continue
        text = flatten_text(elem)
        if not text:
            continue
        absolute_sequence += 1
        tag_key = "heading" if elem.tag.endswith("}h") else "paragraph"
        counters[tag_key] = counters.get(tag_key, 0) + 1
        xml_tag = elem.tag.split("}", 1)[-1]
        paragraphs.append(
            {
                "kind": tag_key,
                "sequence_in_kind": counters[tag_key],
                "absolute_sequence": absolute_sequence,
                "text": text,
                "xml_tag": xml_tag,
                "style_name": elem.attrib.get(f"{{{TEXT_NS['text']}}}style-name"),
                "xml_path": (
                    "content.xml:/office:document-content/office:body/office:text/"
                    f"{xml_tag}[{absolute_sequence}]"
                ),
            }
        )
    return paragraphs


def deterministic_segments(paragraphs: list, max_chars: int = 1200) -> list:
    """Group *paragraphs* into segments of at most *max_chars* characters.

    Splitting is greedy and deterministic: a new segment is opened only when
    adding the next paragraph would exceed the limit. Returns a list of
    (segment_index, paragraph_list, segment_text) tuples.
    """
    segments = []
    current: list = []
    current_len = 0
    seg_idx = 1
    for paragraph in paragraphs:
        txt = paragraph["text"]
        if current and current_len + len(txt) + 1 > max_chars:
            segment_text = "\n".join(x["text"] for x in current)
            segments.append((seg_idx, current, segment_text))
            seg_idx += 1
            current = []
            current_len = 0
        current.append(paragraph)
        current_len += len(txt) + 1
    if current:
        segment_text = "\n".join(x["text"] for x in current)
        segments.append((seg_idx, current, segment_text))
    return segments
