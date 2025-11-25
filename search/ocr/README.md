# OCR Module

## Purpose

Refines PDF extractions using Mistral OCR when PyPDF2 produces poor results. This is a post-processing step that improves text quality for chunking and embedding.

## Structure

```
ocr/
├── inspect.py     # Identify PDFs needing OCR
├── process.py     # Run Mistral OCR on flagged PDFs
├── mistral.py     # Mistral API wrapper
└── outputs/       # Temporary PDF storage
```

## Workflow

### 1. Inspect PDF Extractions

After loading search results to database:

```bash
python -m search.ocr.inspect
```

This evaluates each PDF extraction and flags those needing OCR based on:
- Text length < 100 characters
- High ratio of garbled/non-printable characters (>30%)

Updates `extraction_quality` and `needs_ocr` fields in database.

### 2. Process with OCR

```bash
python -m search.ocr.process --limit 10
```

Options:
- `--limit` - process only N PDFs (optional)

This:
- Downloads flagged PDFs
- Uploads to Mistral OCR service
- Extracts markdown-formatted text
- Updates database with refined text
- Marks extraction_method as 'pdf_ocr'

## Extraction Quality Levels

**good** - Clean text, sufficient length, ready for chunking
**poor** - Short or garbled text, needs OCR
**failed** - Extraction failed completely

## Database Fields

Added to `sources.search_results`:
- `extraction_method` - 'html', 'pdf_basic', 'pdf_ocr'
- `extraction_quality` - 'good', 'poor', 'failed'
- `needs_ocr` - boolean flag

## OCR Output Format

Mistral OCR returns markdown with:
- Page numbers as `## Page N`
- Original document headings preserved
- Structured text layout

Example:
```markdown
## Page 1

# Main Title

Body text here...

## Subsection

More content...

---

## Page 2

Continued...
```

## Notes

- Only processes PDFs marked as `needs_ocr = TRUE`
- Replaces poor PyPDF2 text with OCR text in database
- Temporary PDFs stored in `outputs/temp/` then deleted
- 2 second pause between documents for API rate limiting
- Cost: ~$0.001-0.01 per page depending on complexity

## Configuration

Set in `.env`:
```
MISTRAL_API_KEY=your_key_here
```