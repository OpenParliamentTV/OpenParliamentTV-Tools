"""PDF text-extraction backends for the PDF→TEI pipeline.

Only PyMuPDF is shipped; it gives reading-order blocks fast enough for the whole
German PDF tier. (A docling backend was used in the prototype but dropped — the
validated path is PyMuPDF.)
"""
