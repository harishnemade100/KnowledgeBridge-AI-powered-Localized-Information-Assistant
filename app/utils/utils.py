# utils.py
from bs4 import BeautifulSoup

def extract_faqs(content: str):
    faqs = []
    if not content:
        return faqs
    soup = BeautifulSoup(content, "html.parser")
    # schema.org FAQPage detection
    for faq in soup.select('[itemtype*="FAQ"]'):
        for q in faq.select('[itemprop="question"]'):
            question = q.get_text(strip=True)
            ans_el = q.find_next(attrs={"itemprop":"answer"})
            answer = ans_el.get_text(strip=True) if ans_el else ""
            faqs.append({"q": question, "a": answer})
    # fallback dt/dd pairs
    if not faqs:
        dts = soup.find_all("dt")
        dds = soup.find_all("dd")
        for dt, dd in zip(dts, dds):
            faqs.append({"q": dt.get_text(strip=True), "a": dd.get_text(strip=True)})
    return faqs
